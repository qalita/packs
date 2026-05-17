from qalita_core.pack import Pack
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Try to import Polars for memory-efficient anti-join
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


def _find_missing_fks_polars(parent_paths, child_paths, parent_key, child_key):
    """
    Find missing foreign keys using Polars anti-join (memory efficient).

    This is much more efficient than loading entire columns into Python lists.
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars required for efficient FK check")

    # Scan parent and child parquet files lazily
    parent_lf = pl.scan_parquet(parent_paths).select(parent_key).drop_nulls()
    child_lf = pl.scan_parquet(child_paths).select(child_key)

    # Get total child count
    child_count = child_lf.select(pl.len()).collect(engine="streaming").item()

    # Rename child columns to match parent for join (if keys have different names)
    if parent_key != child_key:
        rename_map = {c: p for c, p in zip(child_key, parent_key)}
        child_lf = child_lf.rename(rename_map)

    # Anti-join: find child rows with no matching parent
    orphans_lf = child_lf.join(parent_lf.unique(), on=parent_key, how="anti")

    # Count orphans using streaming
    try:
        orphan_count = (
            orphans_lf.select(pl.len()).collect(engine="streaming").item()
        )
    except Exception:
        orphan_count = orphans_lf.select(pl.len()).collect().item()

    return orphan_count, child_count


def _find_missing_fks_pandas(parent_df, child_df, parent_key, child_key):
    """
    Fallback pandas-based FK check (less memory efficient).

    Optimized to avoid .values.tolist() by using merge instead.
    """
    # Use merge with indicator instead of set operations
    child_subset = child_df[child_key].copy()
    parent_subset = parent_df[parent_key].drop_duplicates()

    # Rename columns for merge if different
    if parent_key != child_key:
        rename_map = {c: p for c, p in zip(child_key, parent_key)}
        child_subset = child_subset.rename(columns=rename_map)

    # Left join to find orphans
    merged = child_subset.merge(
        parent_subset, on=parent_key, how="left", indicator=True
    )

    orphan_count = (merged["_merge"] == "left_only").sum()
    child_count = len(child_subset)

    return int(orphan_count), int(child_count)


with Pack() as pack:
    if pack.source_config.get("type") == "database":
        table_or_query = pack.source_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type source, you must specify 'table_or_query' in the config."
            )
        pack.load_data("source", table_or_query=table_or_query)
    else:
        pack.load_data("source")

    relations = pack.pack_config.get("job", {}).get("relations", [])
    missing_total = 0
    checked_total = 0

    for rel in relations:
        parent_key = rel["parent"]["key"]
        child_key = rel["child"]["key"]
        if not isinstance(parent_key, list):
            parent_key = [parent_key]
        if not isinstance(child_key, list):
            child_key = [child_key]

        parent_raw = (
            pack.df_source
            if rel["parent"]["source"] == "source"
            else pack.df_target
        )
        child_raw = (
            pack.df_source
            if rel["child"]["source"] == "source"
            else pack.df_target
        )

        # Normalize to list of paths
        parent_paths = (
            parent_raw if isinstance(parent_raw, list) else [parent_raw]
        )
        child_paths = child_raw if isinstance(child_raw, list) else [child_raw]

        # Filter to parquet paths
        parent_parquet = [
            p
            for p in parent_paths
            if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
        ]
        child_parquet = [
            p
            for p in child_paths
            if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
        ]

        # Try Polars anti-join first (most memory efficient)
        if POLARS_AVAILABLE and parent_parquet and child_parquet:
            try:
                missing_count, child_count = _find_missing_fks_polars(
                    parent_parquet, child_parquet, parent_key, child_key
                )
                logger.info(
                    f"Polars anti-join: {missing_count} missing FKs out of {child_count} rows"
                )
            except Exception as e:
                logger.warning(
                    f"Polars FK check failed: {e}, falling back to pandas"
                )
                # Fallback to pandas
                parent_df = pd.read_parquet(
                    parent_parquet[0], engine="pyarrow"
                )
                child_df = pd.read_parquet(child_parquet[0], engine="pyarrow")
                missing_count, child_count = _find_missing_fks_pandas(
                    parent_df, child_df, parent_key, child_key
                )
        else:
            # Pandas fallback
            def _load_parquet_if_path(obj):
                try:
                    if isinstance(obj, str) and obj.lower().endswith(
                        (".parquet", ".pq")
                    ):
                        return pd.read_parquet(obj, engine="pyarrow")
                except Exception:
                    pass
                return obj

            parent_df = _load_parquet_if_path(parent_paths[0])
            child_df = _load_parquet_if_path(child_paths[0])
            missing_count, child_count = _find_missing_fks_pandas(
                parent_df, child_df, parent_key, child_key
            )

        missing_total += missing_count
        checked_total += child_count

        pack.metrics.data.append(
            {
                "key": "missing_foreign_keys",
                "value": missing_count,
                "scope": {
                    "perimeter": "dataset",
                    "value": rel["child"]["table"],
                },
            }
        )

    score = (
        1.0
        if checked_total == 0
        else max(0.0, 1 - (missing_total / checked_total))
    )
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

    pack.metrics.save()
