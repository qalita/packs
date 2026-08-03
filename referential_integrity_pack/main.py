"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Foreign key integrity between configured parent and child objects.

Two things were wrong here and both produced wrong numbers rather than slow
ones. First, ``pack_conf.json`` declares ``parent.table`` and ``child.table``
and the code never read them: it took ``pack.df_source`` or ``pack.df_target``
wholesale, so every relation was checked against the same blob of parquet parts
whatever the relation said. Second, the pandas fallback undid the streaming
anti-join by reading ``parquet[0]`` — the first 100k-row chunk — into memory,
so a fallback triggered by any transient error turned a correct answer into a
plausible-looking wrong one.

Now each declared table is loaded once, scanned by name, and every relation is
resolved to the right pair of objects. The anti-join runs in the streaming
engine and is not retried in memory: on a dataset that failed to stream, the
in-memory engine is precisely the thing that cannot succeed.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack
from qalita_core.utils import slugify

logger = logging.getLogger("referential_integrity_pack")

DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000


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


def _as_list(value: Any) -> List[str]:
    return list(value) if isinstance(value, list) else [value]


def _relation_tables(
    relations: Sequence[Dict[str, Any]], side: str
) -> List[str]:
    """Distinct table names a trigger has to provide, in declaration order."""
    tables: List[str] = []
    for relation in relations:
        for role in ("parent", "child"):
            end = relation.get(role) or {}
            if end.get("source") != side:
                continue
            table = end.get("table")
            if table and table not in tables:
                tables.append(table)
    return tables


def _load_tables(pack: Pack, trigger: str, tables: Iterable[str]) -> None:
    """Load several logical objects for one trigger and keep them all.

    ``load_data`` REPLACES the trigger's object map on every call. A pack that
    needs more than one table therefore has to accumulate the mapping itself,
    otherwise only the last table stays scannable and only its parquet parts get
    cleaned up at the end of the run.
    """
    objects: Dict[str, List[str]] = {}
    paths: List[str] = []
    seen: set = set()

    requested = list(tables) or [None]

    for table in requested:
        loaded = pack.load_data(trigger, table_or_query=table) or []
        recorded = (
            pack.objects_source if trigger == "source" else pack.objects_target
        )
        for name, parts in recorded.items():
            known = objects.setdefault(name, [])
            known.extend(part for part in parts if part not in known)
        for path in loaded:
            if path not in seen:
                seen.add(path)
                paths.append(path)

    if trigger == "source":
        pack.objects_source = objects
        pack.paths_source = paths
        pack.df_source = paths
    else:
        pack.objects_target = objects
        pack.paths_target = paths
        pack.df_target = paths


def _resolve_object(objects: Dict[str, List[str]], table: str) -> str:
    """Map a configured table name onto the object key that holds its parts.

    Object keys are built as ``<source_type>_<slugified object>``, so a relation
    that names ``dim_customers`` has to be matched against
    ``postgresql_public_dim_customers``.
    """
    if table in objects:
        return table

    slug = slugify(str(table))
    candidates = [
        name
        for name in objects
        if name == slug or name.endswith(f"_{slug}") or slug in name
    ]
    if len(candidates) == 1:
        return candidates[0]
    if len(objects) == 1:
        # A file or folder source names its object after the file, not after the
        # table the relation declares; with a single object there is no ambiguity.
        return next(iter(objects))
    raise KeyError(
        f"cannot resolve table {table!r} among {', '.join(sorted(objects))}"
    )


def _fk_counts(
    parent_lf: "pl.LazyFrame",
    child_lf: "pl.LazyFrame",
    parent_key: Sequence[str],
    child_key: Sequence[str],
    example_limit: int,
) -> Tuple[int, int, "pl.DataFrame"]:
    """Orphan count, child count and bounded orphan examples.

    A left join against the distinct parent keys marks each child row, so the
    total and the violated count come out of ONE streamed aggregation instead of
    one pass for the total and another for the orphans.
    """
    parent_key = list(parent_key)
    child_key = list(child_key)

    parent = parent_lf.select(parent_key).drop_nulls().unique()
    child = child_lf.select(child_key)
    if parent_key != child_key:
        child = child.rename(dict(zip(child_key, parent_key)))

    marker = "__qalita_parent_present"
    marked = child.join(
        parent.with_columns(pl.lit(True).alias(marker)),
        on=parent_key,
        how="left",
    )
    orphan = pl.col(marker).is_null()

    counts = analytics.agg(
        marked, {"total": pl.len(), "orphans": orphan.sum()}
    )
    total = int(counts.get("total") or 0)
    orphans = int(counts.get("orphans") or 0)

    if orphans == 0 or example_limit <= 0:
        examples = marked.head(0).select(parent_key)
    else:
        examples = marked.filter(orphan).select(parent_key).head(example_limit)

    # Bounded by an explicit head() inside the lazy plan, so the cap holds
    # however many rows are orphaned. Collected directly rather than through
    # analytics.failures() only to avoid re-running the join for a count the
    # aggregation above already produced. No in-memory fallback on purpose.
    return orphans, total, examples.collect(engine="streaming")


def _metric(key: str, value: Any, scope: Dict[str, Any]) -> Dict[str, Any]:
    return {"key": key, "value": value, "scope": scope}


def run(pack: Pack) -> None:
    relations = _job(pack).get("relations", []) or []
    example_limit = _example_limit(_job(pack))

    source_tables = _relation_tables(relations, "source")
    target_tables = _relation_tables(relations, "target")

    if pack.source_config.get("type") == "database" and not source_tables:
        table_or_query = pack.source_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type source, you must specify "
                "'table_or_query' in the config, or declare "
                "job.relations[].parent.table in pack_conf.json."
            )
        source_tables = _as_list(table_or_query)
    _load_tables(pack, "source", source_tables)

    if target_tables:
        _load_tables(pack, "target", target_tables)

    dataset_name = pack.source_config["name"]
    missing_total = 0
    checked_total = 0

    for relation in relations:
        parent = relation["parent"]
        child = relation["child"]
        parent_key = _as_list(parent["key"])
        child_key = _as_list(child["key"])

        parent_trigger = parent.get("source", "source")
        child_trigger = child.get("source", "source")
        parent_objects = (
            pack.objects_source
            if parent_trigger == "source"
            else pack.objects_target
        )
        child_objects = (
            pack.objects_source
            if child_trigger == "source"
            else pack.objects_target
        )

        parent_table = _resolve_object(parent_objects, parent["table"])
        child_table = _resolve_object(child_objects, child["table"])

        missing_count, child_count, orphan_rows = _fk_counts(
            pack.scan(parent_trigger, parent_table),
            pack.scan(child_trigger, child_table),
            parent_key,
            child_key,
            example_limit,
        )
        logger.info(
            "%s -> %s: %d orphan foreign key(s) out of %d row(s)",
            child["table"],
            parent["table"],
            missing_count,
            child_count,
        )

        missing_total += missing_count
        checked_total += child_count

        child_scope = {"perimeter": "dataset", "value": child["table"]}
        ratio = (missing_count / child_count) if child_count else 0.0
        pack.metrics.data.extend(
            [
                _metric("missing_foreign_keys", missing_count, child_scope),
                _metric("checked_foreign_keys", child_count, child_scope),
                _metric(
                    "missing_foreign_keys_ratio",
                    str(round(ratio, 6)),
                    child_scope,
                ),
            ]
        )
        if missing_count and orphan_rows.height:
            pack.metrics.data.append(
                _metric(
                    "missing_foreign_keys_examples",
                    _examples_value(orphan_rows),
                    child_scope,
                )
            )
        if missing_count:
            pack.recommendations.data.append(
                {
                    "content": (
                        f"{missing_count} row(s) of '{child['table']}' "
                        f"reference a missing '{parent['table']}' key "
                        f"({ratio * 100:.2f}%)."
                    ),
                    "type": "Referential Integrity Violation",
                    "scope": dict(child_scope),
                    "level": "high" if ratio > 0.05 else "warning",
                }
            )

    score = (
        1.0
        if checked_total == 0
        else max(0.0, 1 - (missing_total / checked_total))
    )
    root_scope = {"perimeter": "dataset", "value": dataset_name}
    pack.metrics.data.extend(
        [
            _metric("score", str(round(score, 2)), root_scope),
            _metric("missing_foreign_keys_total", missing_total, root_scope),
            _metric("checked_foreign_keys_total", checked_total, root_scope),
        ]
    )

    pack.metrics.save()
    pack.recommendations.save()


if __name__ == "__main__":
    with Pack() as _pack:
        run(_pack)
