"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Outlier detection over streaming IQR / z-score fences.

The previous implementation fitted a pyod KNN (sklearn NearestNeighbors) per
column and once more over a one-hot encoded copy of the whole table. Both need
a dense in-memory matrix, so the pack could only ever run on a head() slice of
a large source, and the multivariate fit ran on the full row count with no
sampling at all. Fences derived from order statistics are a two-pass streaming
computation whose memory is independent of the row count, which is why the
detector changed. See the README: `normality_score` is not comparable with the
values produced before 3.0.0.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.aggregation import streaming_outliers
from qalita_core.pack import Pack

logger = logging.getLogger(__name__)

# Columns per streaming_outliers call. Every column contributes two expressions
# to the counting aggregate, so a very wide table would otherwise build an
# unbounded expression list; batching trades extra passes for a bounded plan.
MAX_COLUMNS_PER_PASS = 500

# Example rows are evidence, not an export. The cap is enforced here as well as
# in analytics.failures so a pack_conf typo cannot turn a 100 GiB source into a
# 100 GiB metrics.json.
DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1_000

# Row index used to label example rows. Prefixed because a source column named
# "index" is common and must not be shadowed.
ROW_INDEX = "__qalita_row_index"

DEFAULT_IQR_MULTIPLIER = 1.5
DEFAULT_ZSCORE_THRESHOLD = 3.0


def determine_recommendation_level(proportion_outliers: float) -> str:
    """Severity of a recommendation, from the share of rows involved."""
    if proportion_outliers > 0.5:
        return "high"
    if proportion_outliers > 0.3:
        return "warning"
    return "info"


def detect(
    lf: "pl.LazyFrame",
    columns: Sequence[str],
    *,
    method: str = "iqr",
    iqr_multiplier: float = DEFAULT_IQR_MULTIPLIER,
    zscore_threshold: float = DEFAULT_ZSCORE_THRESHOLD,
    exact: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Global fences and out-of-fence counts for every numeric column.

    Delegates the two passes to ``qalita_core.aggregation.streaming_outliers``
    so this pack and the platform agree on what an outlier is. The columns are
    batched because the counting pass builds two expressions per column: on a
    very wide table one call would produce an unbounded expression list.
    """
    threshold = iqr_multiplier if method == "iqr" else zscore_threshold
    results: Dict[str, Dict[str, Any]] = {}
    for batch in analytics.batched(columns, MAX_COLUMNS_PER_PASS):
        results.update(
            streaming_outliers(
                lf,
                batch,
                method=method,
                threshold=threshold,
                exact=exact,
            )
        )
    return results


def fences(
    results: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Tuple[float, float]]:
    """The columns that got a usable fence, as ``{column: (lower, upper)}``.

    All-null and constant columns have none: no value can fall outside a fence
    of zero width, so they are excluded from the row-level predicate rather
    than contributing a term that is always false.
    """
    return {
        column: (float(stats["lower"]), float(stats["upper"]))
        for column, stats in results.items()
        if stats.get("lower") is not None and stats.get("upper") is not None
    }


def outlier_predicate(
    bounds: Mapping[str, Tuple[float, float]],
) -> Optional["pl.Expr"]:
    """True for a row holding at least one out-of-fence value.

    Nulls stay null so ``analytics.failures`` can treat them as passing; a
    missing value is not an outlier.
    """
    predicate: Optional[pl.Expr] = None
    for column, (lower, upper) in bounds.items():
        value = pl.col(column).cast(pl.Float64)
        term = (value < lower) | (value > upper)
        predicate = term if predicate is None else (predicate | term)
    return predicate


def build_outliers_table(
    rows: "pl.DataFrame",
    bounds: Mapping[str, Tuple[float, float]],
    id_columns: Sequence[str],
) -> Dict[str, Any]:
    """Turn bounded example rows into the `outliers_table` metric payload.

    One entry per offending VALUE, so a row breaching three fences shows up
    three times — that is what makes the table readable as "which column, which
    value".
    """
    labels = ["index", *id_columns, "OutlierAttribute", "value"]
    data: List[List[Dict[str, Any]]] = []
    for row in rows.iter_rows(named=True):
        for column, (lower, upper) in bounds.items():
            value = row.get(column)
            if value is None:
                continue
            numeric = float(value)
            if lower <= numeric <= upper:
                continue
            data.append(
                [
                    {"value": row.get(ROW_INDEX)},
                    *[{"value": row.get(name)} for name in id_columns],
                    {"value": column},
                    {"value": value},
                ]
            )
    return {"columnLabels": labels, "data": data}


def _column_metric(
    key: str, value: Any, column: str, dataset_label: str
) -> Dict[str, Any]:
    return {
        "key": key,
        "value": value,
        "scope": {
            "perimeter": "column",
            "value": column,
            "parent_scope": {
                "perimeter": "dataset",
                "value": dataset_label,
            },
        },
    }


def _dataset_metric(
    key: str, value: Any, dataset_label: str
) -> Dict[str, Any]:
    return {
        "key": key,
        "value": value,
        "scope": {"perimeter": "dataset", "value": dataset_label},
    }


def analyze_dataset(
    lf: "pl.LazyFrame",
    dataset_label: str,
    *,
    schema: Mapping[str, Any],
    id_columns: Sequence[str] = (),
    method: str = "iqr",
    iqr_multiplier: float = DEFAULT_IQR_MULTIPLIER,
    zscore_threshold: float = DEFAULT_ZSCORE_THRESHOLD,
    exact: bool = False,
    normality_threshold: float = 0.9,
    example_rows: int = DEFAULT_EXAMPLE_ROWS,
    total_rows: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Metrics and recommendations for one logical object.

    Three streaming passes at most, whatever the column count: the fences, the
    counts, and the bounded example rows. Nothing here materializes an
    unbounded row set.
    """
    metrics: List[Dict[str, Any]] = []
    recommendations: List[Dict[str, Any]] = []

    ids = [name for name in id_columns if name in schema]
    candidates = [
        name
        for name in analytics.numeric_columns(schema)
        if name not in set(id_columns)
    ]
    rows = analytics.row_count(lf) if total_rows is None else int(total_rows)

    results = detect(
        lf,
        candidates,
        method=method,
        iqr_multiplier=iqr_multiplier,
        zscore_threshold=zscore_threshold,
        exact=exact,
    )
    bounds = fences(results)
    # One approximate column makes every dataset-level number approximate.
    dataset_method = (
        "histogram"
        if any(
            stats.get("bounds_method") == "histogram"
            for stats in results.values()
        )
        else "exact"
    )

    logger.info(
        "[%s] %s fences (%s) over %d of %d numeric columns, %d rows",
        dataset_label,
        method,
        dataset_method,
        len(bounds),
        len(candidates),
        rows,
    )

    metrics.append(_dataset_metric("outlier_method", method, dataset_label))
    metrics.append(_dataset_metric("n", rows, dataset_label))

    total_outliers = 0
    for column in candidates:
        stats = results.get(column, {})
        outliers = int(stats.get("outlier_count", 0))
        non_null = int(stats.get("non_null", 0))
        bounds_method = str(stats.get("bounds_method", "exact"))
        total_outliers += outliers
        normality = float(stats.get("normality_score", 1.0))
        metrics.append(
            _column_metric("outliers", outliers, column, dataset_label)
        )
        metrics.append(
            _column_metric(
                "outliers_method", bounds_method, column, dataset_label
            )
        )
        metrics.append(
            _column_metric(
                "normality_score", round(normality, 2), column, dataset_label
            )
        )
        metrics.append(
            _column_metric(
                "normality_score_method",
                bounds_method,
                column,
                dataset_label,
            )
        )
        fence = bounds.get(column)
        if fence is not None:
            metrics.append(
                _column_metric(
                    "outlier_lower_bound",
                    round(fence[0], 6),
                    column,
                    dataset_label,
                )
            )
            metrics.append(
                _column_metric(
                    "outlier_upper_bound",
                    round(fence[1], 6),
                    column,
                    dataset_label,
                )
            )
        if outliers > 0:
            recommendations.append(
                {
                    "content": f"Column '{column}' has {outliers} outliers.",
                    "type": "Outliers",
                    "scope": {
                        "perimeter": "column",
                        "value": column,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_label,
                        },
                    },
                    "level": determine_recommendation_level(
                        outliers / max(non_null, 1)
                    ),
                }
            )
        if round(normality, 2) < normality_threshold:
            recommendations.append(
                {
                    "content": (
                        f"Column '{column}' has a normality score of "
                        f"{round(normality, 2) * 100}%."
                    ),
                    "type": "Outliers",
                    "scope": {
                        "perimeter": "column",
                        "value": column,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_label,
                        },
                    },
                    "level": determine_recommendation_level(1 - normality),
                }
            )

    # Row-level view. It replaces the multivariate KNN, which needed a dense
    # matrix of the whole table: a row counts as an outlier when any of its
    # values breaches its own fence. Exact and streaming, and the same pass
    # yields the bounded evidence rows.
    predicate = outlier_predicate(bounds)
    limit = max(0, min(int(example_rows), MAX_EXAMPLE_ROWS))
    outlier_rows = 0
    table = {
        "columnLabels": ["index", *ids, "OutlierAttribute", "value"],
        "data": [],
    }
    if predicate is not None:
        outlier_rows, examples = analytics.failures(
            lf.with_row_index(ROW_INDEX),
            predicate,
            limit=limit,
            columns=[ROW_INDEX, *ids, *bounds.keys()],
        )
        table = build_outliers_table(examples, bounds, ids)

    dataset_normality = 1.0 - (outlier_rows / rows) if rows else 1.0
    dataset_normality = round(dataset_normality, 2)

    metrics.append(_dataset_metric("outliers", outlier_rows, dataset_label))
    metrics.append(
        _dataset_metric("outlier_rows", outlier_rows, dataset_label)
    )
    metrics.append(
        _dataset_metric("outliers_method", dataset_method, dataset_label)
    )
    metrics.append(
        _dataset_metric(
            "normality_score_dataset", dataset_normality, dataset_label
        )
    )
    metrics.append(
        _dataset_metric(
            "normality_score_dataset_method", dataset_method, dataset_label
        )
    )
    metrics.append(
        _dataset_metric("score", str(dataset_normality), dataset_label)
    )
    metrics.append(
        _dataset_metric("total_outliers_count", total_outliers, dataset_label)
    )
    metrics.append(_dataset_metric("outliers_table", table, dataset_label))

    if dataset_normality < normality_threshold:
        recommendations.append(
            {
                "content": (
                    f"The dataset '{dataset_label}' has a normality score of "
                    f"{dataset_normality * 100}%."
                ),
                "type": "Outliers",
                "scope": {"perimeter": "dataset", "value": dataset_label},
                "level": determine_recommendation_level(1 - dataset_normality),
            }
        )

    recommendations.append(
        {
            "content": (
                f"The dataset '{dataset_label}' has a total of "
                f"{total_outliers} outliers over {len(bounds)} checked "
                f"columns. Up to {limit} example rows are attached to the "
                f"'outliers_table' metric."
            ),
            "type": "Outliers",
            "scope": {"perimeter": "dataset", "value": dataset_label},
            "level": determine_recommendation_level(
                outlier_rows / max(rows, 1)
            ),
        }
    )

    return metrics, recommendations


def main() -> None:
    with Pack() as pack:
        job = pack.pack_config.get("job", {})

        if pack.source_config.get("type") == "database":
            table_or_query = pack.source_config.get("config", {}).get(
                "table_or_query"
            )
            if not table_or_query:
                raise ValueError(
                    "For a 'database' type source, you must specify "
                    "'table_or_query' in the config."
                )
            pack.load_data("source", table_or_query=table_or_query)
        else:
            pack.load_data("source")

        method = str(job.get("method", "iqr")).lower()
        settings = {
            "id_columns": job.get("id_columns", []) or [],
            "method": method,
            "iqr_multiplier": float(
                job.get("iqr_multiplier", DEFAULT_IQR_MULTIPLIER)
            ),
            "zscore_threshold": float(
                job.get("zscore_threshold", DEFAULT_ZSCORE_THRESHOLD)
            ),
            "exact": bool(job.get("exact", False)),
            "normality_threshold": float(job.get("normality_threshold", 0.9)),
            "example_rows": int(job.get("example_rows", DEFAULT_EXAMPLE_ROWS)),
        }

        # One dataset per logical object. Chunked sources need no special case:
        # Pack.scan() hands the streaming engine every part of an object as a
        # single frame, so cross-chunk state lives in the engine rather than in
        # a pack-side accumulator that used to re-derive it from file names.
        tables = pack.tables("source")
        for table in tables:
            # A single-object source keeps the source name as its scope, which
            # is what previous runs recorded; renaming it to the internal
            # object key would orphan every historical metric on the platform.
            label = (
                pack.source_config.get("name") or table
                if len(tables) == 1
                else table
            )
            lf = pack.scan("source", table)
            metrics, recommendations = analyze_dataset(
                lf,
                label,
                schema=pack.schema("source", table),
                **settings,
            )
            pack.metrics.data.extend(metrics)
            pack.recommendations.data.extend(recommendations)

        pack.recommendations.save()
        pack.metrics.save()


if __name__ == "__main__":
    main()
