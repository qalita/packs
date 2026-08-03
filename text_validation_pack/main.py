"""
Text Validation Pack

Validates text data for length constraints, word counts, and whitespace issues.
Covers ks:
- text_min_length, text_max_length, text_mean_length
- text_length_below_min_length, text_length_above_max_length
- text_length_in_range_percent
- min_word_count, max_word_count
- empty_text_found, whitespace_text_found, null_placeholder_text_found
- text_surrounded_by_whitespace_found

Every statistic of every text column is evaluated in a SINGLE streaming pass:
the rules are compiled into one expression mapping and handed to
``analytics.agg``. The previous implementation materialized each parquet part
with ``pd.read_parquet`` and kept them all alive, then walked column by column.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack

# Common null placeholder patterns
NULL_PLACEHOLDERS = [
    "null",
    "NULL",
    "Null",
    "none",
    "NONE",
    "None",
    "n/a",
    "N/A",
    "NA",
    "na",
    "nan",
    "NaN",
    "NAN",
    "-",
    "--",
    "---",
    ".",
    "..",
    "undefined",
    "UNDEFINED",
    "missing",
    "MISSING",
    "unknown",
    "UNKNOWN",
    "#N/A",
    "#NA",
    "#NULL!",
    "(blank)",
    "(empty)",
    "<null>",
    "<NULL>",
]

LOWER_NULL_PLACEHOLDERS = sorted({p.lower() for p in NULL_PLACEHOLDERS})

# analytics.failures() caps example rows at this value however generously the
# pack is configured, so a misconfigured job cannot export a dataset.
MAX_EXAMPLE_ROWS = 1000
DEFAULT_EXAMPLE_ROWS = 10

# Upper bound on how many columns get an example query. Each one is a bounded
# `filter(...).head(n)` that the streaming engine short-circuits, but a very
# wide table with violations everywhere would otherwise queue hundreds of them.
DEFAULT_MAX_EXAMPLE_COLUMNS = 20


def dataset_labels(pack: Pack, trigger: str = "source") -> Dict[str, str]:
    """Dataset scope name to publish for each logical object.

    A chunked single-object source stays ONE dataset. The previous code labelled
    each parquet part ``<source>_<n>`` and published one dataset per chunk, so a
    table split into three chunks was reported as three datasets.
    """
    tables = pack.tables(trigger)
    if len(tables) == 1:
        return {tables[0]: pack.source_config.get("name") or tables[0]}
    return {name: name for name in tables}


def text_columns(schema: Dict[str, Any]) -> List[str]:
    """Columns holding text, from the parquet footers.

    Replaces ``select_dtypes(include=["object", "string"])``, which needed the
    frame in memory before it could tell what was in it.
    """
    return [
        name
        for name, dtype in schema.items()
        if dtype in (pl.String, pl.Categorical, pl.Enum)
    ]


def column_expressions(
    column: str, key: str, min_length: Any, max_length: Any
) -> Dict[str, "pl.Expr"]:
    """Every text statistic of one column, as aggregate expressions.

    Nulls are excluded by construction: Polars aggregations skip them, which is
    what ``series.dropna()`` did in the pandas version.
    """
    # Categorical/Enum columns reject the `str` namespace outright; the cast is
    # a no-op on a String column.
    text = pl.col(column).cast(pl.String)
    length = text.str.len_chars()
    stripped = text.str.strip_chars()

    exprs: Dict[str, pl.Expr] = {
        f"{key}|non_null": text.count(),
        f"{key}|min_length": length.min(),
        f"{key}|max_length": length.max(),
        f"{key}|mean_length": length.mean(),
        # `\S+` counts whitespace-delimited tokens, which is exactly what
        # pandas' `.str.split().str.len()` produced.
        f"{key}|min_words": text.str.count_matches(r"\S+").min(),
        f"{key}|max_words": text.str.count_matches(r"\S+").max(),
        f"{key}|empty": (text == "").sum(),
        f"{key}|blank": (stripped == "").sum(),
        f"{key}|placeholder": (
            text.str.to_lowercase().is_in(LOWER_NULL_PLACEHOLDERS).sum()
        ),
        f"{key}|surrounded": (text != stripped).sum(),
    }
    if min_length is not None:
        exprs[f"{key}|below_min"] = (length < min_length).sum()
    if max_length is not None:
        exprs[f"{key}|above_max"] = (length > max_length).sum()
    return exprs


def violation_predicate(
    column: str, min_length: Any, max_length: Any
) -> "pl.Expr":
    """True for a row this pack would report on, for one column."""
    text = pl.col(column).cast(pl.String)
    stripped = text.str.strip_chars()
    predicate = (
        (text == "")
        | (stripped == "")
        | text.str.to_lowercase().is_in(LOWER_NULL_PLACEHOLDERS)
        | (text != stripped)
    )
    length = text.str.len_chars()
    if min_length is not None:
        predicate = predicate | (length < min_length)
    if max_length is not None:
        predicate = predicate | (length > max_length)
    return predicate


def _as_int(value: Any) -> int:
    return int(value) if value is not None else 0


def column_result(
    stats: Dict[str, Any], key: str, min_length: Any, max_length: Any
) -> Dict[str, Any]:
    """Turn the raw aggregate row into the metric values for one column."""
    non_null = _as_int(stats.get(f"{key}|non_null"))
    if non_null == 0:
        return {
            "non_null": 0,
            "min_length": 0,
            "max_length": 0,
            "mean_length": 0,
            "below_min_length": 0,
            "above_max_length": 0,
            "in_range_percent": 1.0,
            "empty_text_count": 0,
            "whitespace_only_count": 0,
            "null_placeholder_count": 0,
            "surrounded_by_whitespace_count": 0,
            "min_word_count": 0,
            "max_word_count": 0,
        }

    below_min = _as_int(stats.get(f"{key}|below_min"))
    above_max = _as_int(stats.get(f"{key}|above_max"))
    empty = _as_int(stats.get(f"{key}|empty"))
    mean_length = stats.get(f"{key}|mean_length") or 0.0

    return {
        "non_null": non_null,
        "min_length": _as_int(stats.get(f"{key}|min_length")),
        "max_length": _as_int(stats.get(f"{key}|max_length")),
        "mean_length": round(float(mean_length), 2),
        "below_min_length": below_min,
        "above_max_length": above_max,
        "in_range_percent": round(
            (non_null - below_min - above_max) / non_null, 4
        ),
        "empty_text_count": empty,
        # A blank value is also an empty value for the `blank` expression, so
        # the two counts would double-report the same rows.
        "whitespace_only_count": _as_int(stats.get(f"{key}|blank")) - empty,
        "null_placeholder_count": _as_int(stats.get(f"{key}|placeholder")),
        "surrounded_by_whitespace_count": _as_int(
            stats.get(f"{key}|surrounded")
        ),
        "min_word_count": _as_int(stats.get(f"{key}|min_words")),
        "max_word_count": _as_int(stats.get(f"{key}|max_words")),
    }


def example_limit(pack_config: Dict[str, Any]) -> int:
    job = (pack_config or {}).get("job", {}) or {}
    raw = job.get("examples_limit", DEFAULT_EXAMPLE_ROWS)
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        limit = DEFAULT_EXAMPLE_ROWS
    return max(0, min(limit, MAX_EXAMPLE_ROWS))


def max_example_columns(pack_config: Dict[str, Any]) -> int:
    job = (pack_config or {}).get("job", {}) or {}
    try:
        return max(
            0, int(job.get("max_example_columns", DEFAULT_MAX_EXAMPLE_COLUMNS))
        )
    except (TypeError, ValueError):
        return DEFAULT_MAX_EXAMPLE_COLUMNS


def metrics_for_column(
    dataset_label: str,
    column: str,
    result: Dict[str, Any],
    min_length: Any,
    max_length: Any,
) -> List[Dict[str, Any]]:
    col_scope = {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
    }
    metrics: List[Dict[str, Any]] = [
        {
            "key": "text_min_length",
            "value": result["min_length"],
            "scope": col_scope.copy(),
        },
        {
            "key": "text_max_length",
            "value": result["max_length"],
            "scope": col_scope.copy(),
        },
        {
            "key": "text_mean_length",
            "value": str(result["mean_length"]),
            "scope": col_scope.copy(),
        },
        {
            "key": "min_word_count",
            "value": result["min_word_count"],
            "scope": col_scope.copy(),
        },
        {
            "key": "max_word_count",
            "value": result["max_word_count"],
            "scope": col_scope.copy(),
        },
    ]

    if min_length is not None:
        metrics.append(
            {
                "key": "text_length_below_min_length",
                "value": result["below_min_length"],
                "scope": col_scope.copy(),
            }
        )
    if max_length is not None:
        metrics.append(
            {
                "key": "text_length_above_max_length",
                "value": result["above_max_length"],
                "scope": col_scope.copy(),
            }
        )
    if min_length is not None or max_length is not None:
        metrics.append(
            {
                "key": "text_length_in_range_percent",
                "value": str(result["in_range_percent"]),
                "scope": col_scope.copy(),
            }
        )

    metrics.extend(
        [
            {
                "key": "empty_text_found",
                "value": result["empty_text_count"],
                "scope": col_scope.copy(),
            },
            {
                "key": "whitespace_text_found",
                "value": result["whitespace_only_count"],
                "scope": col_scope.copy(),
            },
            {
                "key": "null_placeholder_text_found",
                "value": result["null_placeholder_count"],
                "scope": col_scope.copy(),
            },
            {
                "key": "text_surrounded_by_whitespace_found",
                "value": result["surrounded_by_whitespace_count"],
                "scope": col_scope.copy(),
            },
        ]
    )
    return metrics


def recommendations_for_column(
    dataset_label: str,
    column: str,
    result: Dict[str, Any],
    min_length: Any,
    max_length: Any,
) -> List[Dict[str, Any]]:
    col_scope = {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
    }
    checks: List[Tuple[int, str, str, str]] = [
        (
            result["empty_text_count"],
            f"Column '{column}' has {result['empty_text_count']} empty text "
            f"values.",
            "Empty Text Found",
            "info",
        ),
        (
            result["whitespace_only_count"],
            f"Column '{column}' has {result['whitespace_only_count']} "
            f"whitespace-only values.",
            "Whitespace Only Text",
            "warning",
        ),
        (
            result["null_placeholder_count"],
            f"Column '{column}' has {result['null_placeholder_count']} null "
            f"placeholder values (N/A, None, etc.).",
            "Null Placeholder Found",
            "warning",
        ),
        (
            result["surrounded_by_whitespace_count"],
            f"Column '{column}' has "
            f"{result['surrounded_by_whitespace_count']} values with "
            f"leading/trailing whitespace.",
            "Text Surrounded By Whitespace",
            "info",
        ),
        (
            result["below_min_length"],
            f"Column '{column}' has {result['below_min_length']} values "
            f"shorter than minimum length {min_length}.",
            "Text Too Short",
            "warning",
        ),
        (
            result["above_max_length"],
            f"Column '{column}' has {result['above_max_length']} values "
            f"longer than maximum length {max_length}.",
            "Text Too Long",
            "warning",
        ),
    ]
    return [
        {
            "content": content,
            "type": rec_type,
            "scope": col_scope.copy(),
            "level": level,
        }
        for count, content, rec_type, level in checks
        if count > 0
    ]


def main() -> None:
    with Pack() as pack:
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

        job = pack.pack_config.get("job", {}) or {}
        validation_rules = job.get("rules", []) or []
        analyze_all_text = job.get("analyze_all_text_columns", True)
        col_rules = {r.get("column"): r for r in validation_rules}
        limit = example_limit(pack.pack_config)
        example_columns_budget = max_example_columns(pack.pack_config)

        total_checks = 0
        total_valid_percent = 0.0
        total_issues = 0

        for table, dataset_label in dataset_labels(pack, "source").items():
            print(f"Validating text columns for {dataset_label}")

            lf = pack.scan("source", table)
            schema = pack.schema("source", table)
            columns = text_columns(schema)
            if not analyze_all_text:
                columns = [c for c in columns if c in col_rules]
            if not columns:
                continue

            keys = {column: str(i) for i, column in enumerate(columns)}
            exprs: Dict[str, pl.Expr] = {}
            for column in columns:
                rule = col_rules.get(column, {})
                exprs.update(
                    column_expressions(
                        column,
                        keys[column],
                        rule.get("min_length"),
                        rule.get("max_length"),
                    )
                )

            # ONE pass for every rule of every column of this dataset.
            stats = analytics.agg(lf, exprs)

            examples_left = example_columns_budget
            for column in columns:
                rule = col_rules.get(column, {})
                min_length = rule.get("min_length")
                max_length = rule.get("max_length")
                result = column_result(
                    stats, keys[column], min_length, max_length
                )
                if result["non_null"] == 0:
                    print(f"Column '{column}' is empty. Skipping.")
                    continue

                pack.metrics.data.extend(
                    metrics_for_column(
                        dataset_label, column, result, min_length, max_length
                    )
                )
                pack.recommendations.data.extend(
                    recommendations_for_column(
                        dataset_label, column, result, min_length, max_length
                    )
                )

                issues = (
                    result["empty_text_count"]
                    + result["whitespace_only_count"]
                    + result["null_placeholder_count"]
                )
                total_checks += 1
                total_valid_percent += 1 - (issues / result["non_null"])
                total_issues += issues

                violations = (
                    issues
                    + result["surrounded_by_whitespace_count"]
                    + result["below_min_length"]
                    + result["above_max_length"]
                )
                if violations and limit and examples_left:
                    examples_left -= 1
                    _, rows = analytics.failures(
                        lf,
                        violation_predicate(column, min_length, max_length),
                        limit=limit,
                        columns=[column],
                    )
                    pack.metrics.data.append(
                        {
                            "key": "violation_examples",
                            "value": rows.to_dicts(),
                            "scope": {
                                "perimeter": "column",
                                "value": column,
                                "parent_scope": {
                                    "perimeter": "dataset",
                                    "value": dataset_label,
                                },
                            },
                        }
                    )

                print(
                    f"  [{column}] len: {result['min_length']}-"
                    f"{result['max_length']} (avg {result['mean_length']}), "
                    f"empty: {result['empty_text_count']}, whitespace: "
                    f"{result['whitespace_only_count']}, placeholders: "
                    f"{result['null_placeholder_count']}"
                )

        score = total_valid_percent / total_checks if total_checks else 1.0

        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {
                    "perimeter": "dataset",
                    "value": pack.source_config["name"],
                },
            }
        )
        pack.metrics.data.append(
            {
                "key": "total_text_issues",
                "value": total_issues,
                "scope": {
                    "perimeter": "dataset",
                    "value": pack.source_config["name"],
                },
            }
        )

        pack.metrics.save()
        pack.recommendations.save()


if __name__ == "__main__":
    main()
