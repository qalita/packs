"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Distribution drift between a reference and a current dataset.

This pack used to run scipy's exact two-sample KS test over two dense numpy
arrays built with ``.dropna().values``. That test needs both samples fully
sorted in memory and has no streaming or sketch form, so it could only ever
describe what fitted in RAM. Worse, the previous code read ``paths[0]`` on each
side, so on a chunked source it silently compared the first 100k-row chunk of
the reference against the first chunk of the current dataset and reported the
answer as if it described both datasets.

Drift is now measured from binned CDFs:

1. bin edges are derived from the REFERENCE side alone, from its quantiles, so
   the bins are equi-frequent on the reference and the comparison is not driven
   by whichever side happens to have the wider range;
2. one streaming pass per side counts rows per bin, batched so a wide table
   still costs a bounded number of passes.

Both histograms are O(bins) in memory whatever the row count. PSI and a binned
KS distance are computed from them, and ``p_value`` is still emitted — now
derived from the binned KS distance through the asymptotic Kolmogorov
distribution rather than from the exact test.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack

logger = logging.getLogger("data_drift_pack")

# Bins for the reference histogram. Ten is the convention PSI is read against
# (< 0.1 stable, 0.1-0.2 moderate, > 0.2 significant); changing it changes the
# scale those thresholds live on.
DEFAULT_BINS = 10

DEFAULT_ALPHA = 0.05
DEFAULT_PSI_THRESHOLD = 0.2

# Bounded failing-row examples, per the pack contract.
DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000
DEFAULT_EXAMPLE_COLUMNS = 5

# A zero bucket on either side would send PSI to infinity; the usual fix is to
# floor both shares. The floor is far below the 0.1 PSI decision threshold.
PSI_EPSILON = 1e-6

# Columns per aggregation call. Each column contributes (bins + 1) expressions,
# so a 500-column table would otherwise build a 5500-expression projection.
COLUMN_BATCH = 40


def _job(pack: Pack) -> Dict[str, Any]:
    return pack.pack_config.get("job", {}) or {}


def _example_limit(job: Dict[str, Any]) -> int:
    raw = job.get("examples", DEFAULT_EXAMPLE_ROWS)
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        limit = DEFAULT_EXAMPLE_ROWS
    return max(0, min(limit, MAX_EXAMPLE_ROWS))


def _examples_value(frame: "pl.DataFrame") -> str:
    """Serialize bounded example rows.

    The platform stores a metric value as a string, so the rows travel as JSON
    rather than as a list that would be rejected on ingestion.
    """
    return json.dumps(frame.to_dicts(), ensure_ascii=False, default=str)


def _pair_tables(pack: Pack) -> List[Tuple[str, str, str]]:
    """Match reference objects to current objects.

    Returns ``(label, reference_table, current_table)``. Objects present on both
    sides are matched by name; otherwise the two lists are paired in load order,
    which is the only thing available when the reference and the current dataset
    live in differently named tables.
    """
    reference = pack.tables("source")
    current = pack.tables("target")

    shared = [name for name in reference if name in current]
    if shared:
        return [(name, name, name) for name in shared]

    if len(reference) != len(current):
        logger.warning(
            "reference holds %d object(s) and current holds %d; pairing the "
            "first %d in load order",
            len(reference),
            len(current),
            min(len(reference), len(current)),
        )
    # Pairing two lists of LOGICAL OBJECTS, not names against parquet parts:
    # every part of each object is already behind pack.scan(table=...).
    return [(ref, ref, cur) for ref, cur in zip(reference, current)]


def _comparable_columns(
    reference_schema: Dict[str, Any], current_schema: Dict[str, Any]
) -> List[str]:
    """Numeric columns present, and numeric, on both sides."""
    numeric_reference = set(analytics.numeric_columns(reference_schema))
    numeric_current = set(analytics.numeric_columns(current_schema))
    return [
        name
        for name in reference_schema
        if name in numeric_reference and name in numeric_current
    ]


def _bin_edges(
    lf: "pl.LazyFrame",
    columns: Sequence[str],
    bins: int,
    exact: bool,
) -> Dict[str, List[float]]:
    """Bin edges taken from the reference side only.

    Two streaming passes for the bounds and the quantiles of every column at
    once. Both sides must be counted against the SAME edges, which is why they
    are computed once here and reused rather than recomputed per side.
    """
    if not columns:
        return {}

    bounds = analytics.agg(
        lf,
        {
            **{f"min|{c}": pl.col(c).min() for c in columns},
            **{f"max|{c}": pl.col(c).max() for c in columns},
        },
    )

    probabilities = [i / bins for i in range(1, bins)]
    inner_quantiles = (
        analytics.quantiles(lf, list(columns), probabilities, exact=exact)
        if probabilities
        else {}
    )

    edges: Dict[str, List[float]] = {}
    for column in columns:
        low, high = bounds.get(f"min|{column}"), bounds.get(f"max|{column}")
        if low is None or high is None:
            continue
        low, high = float(low), float(high)
        if not (math.isfinite(low) and math.isfinite(high)) or high <= low:
            # Constant or empty on the reference: no distribution to compare.
            continue

        inner = sorted(
            {
                float(value)
                for value in inner_quantiles.get(column, {}).values()
                if value is not None
                and math.isfinite(float(value))
                and low < float(value) < high
            }
        )
        if len(inner) < 2:
            # A heavily skewed column collapses its quantiles onto one value;
            # equal-width edges still describe where the mass moved.
            width = (high - low) / bins
            inner = [low + width * k for k in range(1, bins)]
        edges[column] = [low] + inner + [high]

    return edges


def _bin_counts(
    lf: "pl.LazyFrame", edges: Dict[str, List[float]]
) -> Dict[str, List[int]]:
    """Rows per bin for every column, batched into whole-dataset passes.

    Values outside the reference range are folded into the extreme bins on
    purpose: both histograms have to describe the same support, otherwise the
    two CDFs are not comparable.
    """
    counts: Dict[str, List[int]] = {}

    for batch in analytics.batched(edges.keys(), COLUMN_BATCH):
        exprs: Dict[str, pl.Expr] = {}
        for index, column in enumerate(batch):
            boundaries = edges[column]
            value = pl.col(column).cast(pl.Float64)
            last = len(boundaries) - 2
            for position in range(len(boundaries) - 1):
                low, high = boundaries[position], boundaries[position + 1]
                if position == 0:
                    condition = value < high
                elif position == last:
                    condition = value >= low
                else:
                    condition = (value >= low) & (value < high)
                exprs[f"{index}|{position}"] = condition.sum()

        result = analytics.agg(lf, exprs)
        for index, column in enumerate(batch):
            bin_count = len(edges[column]) - 1
            counts[column] = [
                int(result.get(f"{index}|{position}") or 0)
                for position in range(bin_count)
            ]

    return counts


def _shares(counts: Sequence[int]) -> List[float]:
    total = sum(counts)
    if total <= 0:
        return [0.0] * len(counts)
    return [count / total for count in counts]


def _psi(reference: Sequence[float], current: Sequence[float]) -> float:
    """Population Stability Index between two binned distributions."""
    total = 0.0
    for expected, observed in zip(reference, current):
        expected = max(expected, PSI_EPSILON)
        observed = max(observed, PSI_EPSILON)
        total += (observed - expected) * math.log(observed / expected)
    return total


def _binned_ks(reference: Sequence[float], current: Sequence[float]) -> float:
    """Largest CDF gap at a bin edge.

    This is a lower bound on the exact KS statistic — the supremum is only
    sampled at the edges — and it converges to it as the bin count grows.
    """
    cumulative_reference = 0.0
    cumulative_current = 0.0
    largest = 0.0
    for expected, observed in zip(reference, current):
        cumulative_reference += expected
        cumulative_current += observed
        largest = max(largest, abs(cumulative_current - cumulative_reference))
    return largest


def _kolmogorov_q(lam: float) -> float:
    """Q(lambda) = 2 * sum (-1)^(j-1) exp(-2 j^2 lambda^2)."""
    if lam <= 0.0:
        return 1.0
    total = 0.0
    for j in range(1, 101):
        term = ((-1) ** (j - 1)) * math.exp(-2.0 * j * j * lam * lam)
        total += term
        if abs(term) < 1e-12:
            break
    return min(1.0, max(0.0, 2.0 * total))


def _ks_p_value(statistic: float, n_reference: int, n_current: int) -> float:
    """Asymptotic two-sample KS p-value, without scipy.

    Fed the binned statistic, which under-estimates the true KS distance, so the
    p-value it returns is an upper bound: this errs towards declaring stability,
    never towards a false drift alarm.
    """
    if n_reference <= 0 or n_current <= 0:
        return 1.0
    effective = math.sqrt(
        (n_reference * n_current) / float(n_reference + n_current)
    )
    if effective <= 0.0:
        return 1.0
    return _kolmogorov_q(
        (effective + 0.12 + 0.11 / effective) * float(statistic)
    )


def _worst_bin(
    reference: Sequence[float], current: Sequence[float]
) -> Optional[int]:
    """Index of the bin whose share moved the most."""
    if not reference:
        return None
    return max(
        range(len(reference)),
        key=lambda index: abs(current[index] - reference[index]),
    )


def _bin_predicate(
    column: str, edges: Sequence[float], position: int
) -> "pl.Expr":
    value = pl.col(column).cast(pl.Float64)
    low, high = edges[position], edges[position + 1]
    if position == 0:
        return value < high
    if position == len(edges) - 2:
        return value >= low
    return (value >= low) & (value < high)


def _metric(key: str, value: Any, scope: Dict[str, Any]) -> Dict[str, Any]:
    return {"key": key, "value": value, "scope": scope}


def run(pack: Pack) -> None:
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

    if pack.target_config.get("type") == "database":
        table_or_query = pack.target_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type target, you must specify "
                "'table_or_query' in the config."
            )
        pack.load_data("target", table_or_query=table_or_query)
    else:
        pack.load_data("target")

    job = _job(pack)
    drift_test = str(job.get("drift_test") or "ks").lower()
    if drift_test not in ("ks", "psi"):
        raise ValueError(
            f"unknown drift_test {drift_test!r}; expected 'ks' or 'psi'"
        )
    bins = max(2, int(job.get("bins") or DEFAULT_BINS))
    alpha = float(job.get("alpha") or DEFAULT_ALPHA)
    psi_threshold = float(job.get("psi_threshold") or DEFAULT_PSI_THRESHOLD)
    exact = bool(job.get("exact", False))
    example_limit = _example_limit(job)
    example_columns = int(
        job.get("example_columns", DEFAULT_EXAMPLE_COLUMNS) or 0
    )

    dataset_name = pack.source_config["name"]
    pairs = _pair_tables(pack)
    single_pair = len(pairs) == 1

    compared = 0
    drifted = 0

    for label, reference_table, current_table in pairs:
        dataset_label = dataset_name if single_pair else label
        dataset_scope = {"perimeter": "dataset", "value": dataset_label}

        reference_lf = pack.scan("source", reference_table)
        current_lf = pack.scan("target", current_table)
        columns = _comparable_columns(
            pack.schema("source", reference_table),
            pack.schema("target", current_table),
        )
        if not columns:
            logger.warning(
                "no numeric column shared by %s and %s",
                reference_table,
                current_table,
            )
            continue

        edges = _bin_edges(reference_lf, columns, bins, exact)
        if not edges:
            continue

        reference_counts = _bin_counts(reference_lf, edges)
        current_counts = _bin_counts(current_lf, edges)

        ranked: List[Tuple[float, str, int]] = []
        for column in edges:
            reference_bins = reference_counts.get(column) or []
            current_bins = current_counts.get(column) or []
            n_reference = sum(reference_bins)
            n_current = sum(current_bins)
            if n_reference == 0 or n_current == 0:
                continue

            reference_share = _shares(reference_bins)
            current_share = _shares(current_bins)
            psi = _psi(reference_share, current_share)
            statistic = _binned_ks(reference_share, current_share)
            p_value = _ks_p_value(statistic, n_reference, n_current)

            has_drift = (
                p_value < alpha if drift_test == "ks" else psi >= psi_threshold
            )
            compared += 1
            drifted += int(has_drift)

            column_scope = {
                "perimeter": "column",
                "value": column,
                "parent_scope": dataset_scope,
            }
            pack.metrics.data.extend(
                [
                    # Kept for dashboards built against the scipy version. The
                    # value now comes from the binned statistic, so it is an
                    # upper bound on the exact p-value rather than the exact one.
                    _metric(
                        "p_value",
                        str(round(float(p_value), 6)),
                        column_scope,
                    ),
                    _metric(
                        "p_value_method",
                        "binned_ks_asymptotic",
                        column_scope,
                    ),
                    _metric(
                        "ks_statistic",
                        str(round(float(statistic), 6)),
                        column_scope,
                    ),
                    _metric("ks_statistic_method", "binned_cdf", column_scope),
                    _metric("psi", str(round(float(psi), 6)), column_scope),
                    _metric("psi_method", "binned_histogram", column_scope),
                    _metric("drift_detected", int(has_drift), column_scope),
                ]
            )

            if has_drift:
                pack.recommendations.data.append(
                    {
                        "content": (
                            f"Column '{column}' drifted "
                            f"(PSI {psi:.4f}, KS {statistic:.4f}, "
                            f"p-value {p_value:.6f})."
                        ),
                        "type": "Data Drift",
                        "scope": dict(column_scope),
                        "level": "high" if psi >= 0.25 else "warning",
                    }
                )
                worst = _worst_bin(reference_share, current_share)
                ranked.append((psi, column, 0 if worst is None else worst))

        # Only the most-drifted columns get example rows: every example costs a
        # bounded but real pass over the current dataset.
        if example_limit and example_columns:
            ranked.sort(reverse=True)
            for _, column, position in ranked[:example_columns]:
                boundaries = edges[column]
                count, rows = analytics.failures(
                    current_lf,
                    _bin_predicate(column, boundaries, position),
                    limit=example_limit,
                    columns=[column],
                )
                if count == 0:
                    continue
                pack.metrics.data.append(
                    _metric(
                        "drift_example_rows",
                        _examples_value(rows),
                        {
                            "perimeter": "column",
                            "value": column,
                            "parent_scope": dataset_scope,
                        },
                    )
                )

    score = 1.0 if compared == 0 else 1.0 - (drifted / compared)
    root_scope = {"perimeter": "dataset", "value": dataset_name}
    pack.metrics.data.extend(
        [
            _metric("score", str(round(score, 2)), root_scope),
            _metric("columns_compared", compared, root_scope),
            _metric("drifted_columns", drifted, root_scope),
            _metric("drift_test", drift_test, root_scope),
        ]
    )

    pack.metrics.save()
    pack.recommendations.save()


if __name__ == "__main__":
    with Pack() as _pack:
        run(_pack)
