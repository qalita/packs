"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Regex-based PII detection.

The previous implementation accumulated ``rows_with_pii``, a Python ``set`` of
pandas row indices, across every (column, pattern) pair — and then only ever
called ``len()`` on it. That is one Python object per matching row, roughly
4-6 GB at 1e8 rows, held to produce a single ratio. Row identity is never
needed here, so it is no longer tracked at all: a row-level
``any_horizontal`` expression counts the rows carrying PII in the same
streaming pass that counts every per-(column, pattern) hit.

THIS PACK MUST NEVER EMIT EXAMPLE ROWS. Every other validation pack ships
bounded failing rows via ``analytics.failures()``; here a failing row is by
definition a row that contains personal data, so exporting it would move the
PII out of the customer's perimeter and into the platform. Counts only. Do not
add ``analytics.failures()`` to this file.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack

logger = logging.getLogger("pii_scanner_pack")

# Columns per aggregation call. Each column contributes one expression per
# pattern, so a wide table with many patterns would otherwise build a very large
# projection in a single plan.
COLUMN_BATCH = 40


def _job(pack: Pack) -> Dict[str, Any]:
    return pack.pack_config.get("job", {}) or {}


def _supported_patterns(
    patterns: List[Dict[str, str]],
) -> List[Tuple[str, str]]:
    """Keep the patterns Polars' regex engine can actually run.

    Polars uses the Rust ``regex`` crate, which has no lookaround and no
    backreferences. A pattern using them would blow up mid-scan, after the
    engine had already read part of the dataset; rejecting it up front against
    an empty frame costs nothing and names the offender.
    """
    probe = pl.DataFrame({"__probe": pl.Series("__probe", [], dtype=pl.Utf8)})
    supported: List[Tuple[str, str]] = []
    for pattern in patterns:
        key, regex = pattern.get("key"), pattern.get("regex")
        if not key or not regex:
            continue
        try:
            probe.select(pl.col("__probe").str.contains(regex))
        except Exception as error:  # noqa: BLE001 - reported, not swallowed
            logger.warning(
                "pattern '%s' is not supported by the Polars regex engine "
                "and is skipped: %s",
                key,
                error,
            )
            continue
        supported.append((key, regex))
    return supported


def _hit_expr(column: str, regex: str) -> "pl.Expr":
    """True when a row's value matches a PII pattern.

    Cast to Utf8 first so a phone number stored as an integer is still seen —
    the pandas version did the same with ``.astype(str)``.
    """
    return pl.col(column).cast(pl.Utf8).str.contains(regex).fill_null(False)


def _scan_dataset(
    lf: "pl.LazyFrame",
    columns: List[str],
    patterns: List[Tuple[str, str]],
) -> Tuple[int, int, Dict[Tuple[str, str], int]]:
    """Row total, rows carrying PII, and per-(column, pattern) hit counts.

    Everything comes out of one streaming pass per column batch. The row count
    of rows carrying PII is an ``any_horizontal`` reduction evaluated per row
    and summed by the engine, so no row identity ever crosses into Python.
    """
    if not columns or not patterns:
        return analytics.row_count(lf), 0, {}

    total = 0
    hits: Dict[Tuple[str, str], int] = {}
    any_pii_per_batch: List[int] = []

    for batch in analytics.batched(columns, COLUMN_BATCH):
        exprs: Dict[str, pl.Expr] = {"__rows": pl.len()}
        matchers: List[pl.Expr] = []
        for column in batch:
            for key, regex in patterns:
                matcher = _hit_expr(column, regex)
                matchers.append(matcher)
                exprs[f"hit|{column}|{key}"] = matcher.sum()
        exprs["__any"] = pl.any_horizontal(matchers).sum()

        result = analytics.agg(lf, exprs)
        total = int(result.get("__rows") or 0)
        any_pii_per_batch.append(int(result.get("__any") or 0))
        for column in batch:
            for key, _ in patterns:
                count = int(result.get(f"hit|{column}|{key}") or 0)
                if count:
                    hits[(column, key)] = count

    # With several batches the per-batch "any" counts overlap, so they cannot be
    # summed. One extra pass over the batch-level flags is the only exact
    # answer; a single batch (the common case) never pays for it.
    if len(any_pii_per_batch) == 1:
        rows_with_pii = any_pii_per_batch[0]
    else:
        matchers = [
            _hit_expr(column, regex)
            for column in columns
            for _, regex in patterns
        ]
        rows_with_pii = int(
            analytics.agg(lf, {"__any": pl.any_horizontal(matchers).sum()})[
                "__any"
            ]
            or 0
        )

    return total, rows_with_pii, hits


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

    patterns = _supported_patterns(_job(pack).get("pii_patterns", []) or [])
    if not patterns:
        logger.warning("no usable PII pattern configured")

    dataset_name = pack.source_config["name"]
    tables = pack.tables("source")
    single_table = len(tables) == 1

    total_rows = 0
    total_rows_with_pii = 0
    pii_columns: set = set()

    for table in tables:
        dataset_label = dataset_name if single_table else table
        dataset_scope = {"perimeter": "dataset", "value": dataset_label}

        lf = pack.scan("source", table)
        columns = list(pack.schema("source", table))
        rows, rows_with_pii, hits = _scan_dataset(lf, columns, patterns)

        total_rows += rows
        total_rows_with_pii += rows_with_pii

        per_column: Dict[str, int] = {}
        for (column, key), count in hits.items():
            per_column[column] = per_column.get(column, 0) + count
            pii_columns.add((dataset_label, column))
            pack.metrics.data.append(
                _metric(
                    f"pii_hits_{key}",
                    count,
                    {
                        "perimeter": "column",
                        "value": column,
                        "parent_scope": dataset_scope,
                    },
                )
            )

        for column, count in per_column.items():
            pack.metrics.data.append(
                _metric(
                    "pii_hits",
                    count,
                    {
                        "perimeter": "column",
                        "value": column,
                        "parent_scope": dataset_scope,
                    },
                )
            )
            pack.recommendations.data.append(
                {
                    "content": (
                        f"Column '{column}' matches {count} PII value(s) in "
                        f"'{dataset_label}'. Review whether it should be "
                        f"masked, tokenised or dropped."
                    ),
                    "type": "PII Detected",
                    "scope": {
                        "perimeter": "column",
                        "value": column,
                        "parent_scope": dict(dataset_scope),
                    },
                    "level": "high",
                }
            )

        pack.metrics.data.extend(
            [
                _metric("rows", rows, dataset_scope),
                _metric("pii_rows", rows_with_pii, dataset_scope),
            ]
        )

    root_scope = {"perimeter": "dataset", "value": dataset_name}
    ratio = 0.0 if total_rows == 0 else total_rows_with_pii / total_rows
    pack.metrics.data.extend(
        [
            _metric("pii_columns", str(len(pii_columns)), root_scope),
            _metric("pii_records_ratio", str(round(ratio, 4)), root_scope),
        ]
    )

    pack.metrics.save()
    pack.recommendations.save()


if __name__ == "__main__":
    with Pack() as _pack:
        run(_pack)
