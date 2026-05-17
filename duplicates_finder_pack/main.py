from qalita_core.pack import Pack
import pandas as pd
import logging
from qalita_core.utils import determine_recommendation_level
from datetime import datetime
import os
from qalita_core.aggregation import (
    detect_chunked_from_items,
    DuplicateAggregator,
    normalize_and_dedupe_recommendations,
)

logger = logging.getLogger(__name__)

# Big data configuration
MAX_DUPLICATES_TO_EXPORT = 10_000  # Limit export to avoid memory issues

# Try to import Polars for efficient duplicate detection
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


def _count_duplicates_polars(paths, uniqueness_columns):
    """
    Count duplicates using Polars group_by().count() streaming.

    This is memory-efficient for large datasets as it uses streaming aggregation.

    Returns:
        tuple: (total_rows, total_duplicates)
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars required for efficient duplicate detection")

    lf = pl.scan_parquet(paths)

    # Get total rows
    try:
        total_rows = lf.select(pl.len()).collect(engine="streaming").item()
    except Exception:
        total_rows = lf.select(pl.len()).collect().item()

    # Count unique combinations using group_by
    # Duplicates = total_rows - unique_combinations
    unique_count_lf = (
        lf.select(uniqueness_columns)
        .group_by(uniqueness_columns)
        .agg(pl.len().alias("_count"))
    )

    try:
        unique_df = unique_count_lf.collect(engine="streaming")
    except Exception:
        unique_df = unique_count_lf.collect()

    # Count duplicates: rows where count > 1 contribute (count - 1) duplicates
    duplicates = (unique_df["_count"] - 1).clip(0, None).sum()

    return int(total_rows), int(duplicates)


def _get_duplicate_rows_polars(paths, uniqueness_columns, limit=None):
    """
    Get actual duplicate rows using Polars (for export).

    Returns duplicate rows as a pandas DataFrame.
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars required")

    lf = pl.scan_parquet(paths)

    # Find rows that appear more than once
    counts_lf = lf.group_by(uniqueness_columns).agg(
        pl.len().alias("_dup_count")
    )
    dup_keys_lf = counts_lf.filter(pl.col("_dup_count") > 1).select(
        uniqueness_columns
    )

    # Join back to get full duplicate rows
    dup_rows_lf = lf.join(dup_keys_lf, on=uniqueness_columns, how="inner")

    if limit:
        dup_rows_lf = dup_rows_lf.head(limit)

    try:
        return dup_rows_lf.collect(engine="streaming").to_pandas()
    except Exception:
        return dup_rows_lf.collect().to_pandas()


# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
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

    ############################ Support single or multiple datasets
    raw_df_source = pack.df_source
    configured = pack.source_config.get("config", {}).get("table_or_query")

    def _load_parquet_if_path(obj):
        try:
            if isinstance(obj, str) and obj.lower().endswith(
                (".parquet", ".pq")
            ):
                return pd.read_parquet(obj, engine="pyarrow")
        except Exception:
            pass
        return obj

    if isinstance(raw_df_source, list):
        loaded = [_load_parquet_if_path(x) for x in raw_df_source]
        if isinstance(configured, (list, tuple)) and len(configured) == len(
            loaded
        ):
            items = list(zip(list(configured), loaded))
            names_for_detect = [str(n) for n in configured]
        else:
            base = pack.source_config["name"]
            items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
            names_for_detect = [name for name, _ in items]
    else:
        items = [
            (pack.source_config["name"], _load_parquet_if_path(raw_df_source))
        ]
        names_for_detect = None

    raw_items_list = (
        raw_df_source if isinstance(raw_df_source, list) else [raw_df_source]
    )
    treat_chunks_as_one, auto_named, common_base_detected = (
        detect_chunked_from_items(
            raw_items_list, names_for_detect, pack.source_config["name"]
        )
    )

    # Process: either per dataset or aggregate across chunks into one scope
    # Get parquet paths for Polars processing
    parquet_paths = [
        p
        for p in raw_items_list
        if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
    ]
    use_polars_direct = POLARS_AVAILABLE and len(parquet_paths) > 0

    if treat_chunks_as_one:
        if (
            "job" in pack.pack_config
            and "compute_uniqueness_columns" in pack.pack_config["job"]
            and len(pack.pack_config["job"]["compute_uniqueness_columns"]) > 0
        ):
            uniqueness_columns = pack.pack_config["job"][
                "compute_uniqueness_columns"
            ]
        else:
            uniqueness_columns = list(items[0][1].columns)

        # Try Polars streaming first (most memory efficient)
        if use_polars_direct:
            try:
                total_rows, total_duplicates = _count_duplicates_polars(
                    parquet_paths, uniqueness_columns
                )
                duplication_rate = (
                    total_duplicates / total_rows if total_rows > 0 else 0
                )
                score = max(0.0, min(1.0, 1.0 - duplication_rate))

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
                        "key": "duplicates",
                        "value": int(total_duplicates),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )
                #  distinct_count and distinct_percent metrics
                distinct_count = total_rows - total_duplicates
                distinct_percent = (
                    distinct_count / total_rows if total_rows > 0 else 0
                )
                pack.metrics.data.append(
                    {
                        "key": "distinct_count",
                        "value": int(distinct_count),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "distinct_percent",
                        "value": str(round(distinct_percent, 4)),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )

                if score < 0.9:
                    pack.recommendations.data.append(
                        {
                            "content": f"dataset '{pack.source_config['name']}' has a duplication rate of {duplication_rate*100:.1f}% on the scope {list(uniqueness_columns)}.",
                            "type": "Duplicates",
                            "scope": {
                                "perimeter": "dataset",
                                "value": pack.source_config["name"],
                            },
                            "level": determine_recommendation_level(
                                duplication_rate
                            ),
                        }
                    )

                logger.info(
                    f"Polars duplicate detection: {total_duplicates} duplicates out of {total_rows} rows"
                )
            except Exception as e:
                logger.warning(
                    f"Polars duplicate detection failed: {e}, falling back to pandas aggregator"
                )
                use_polars_direct = False

        if not use_polars_direct:
            # Fallback to DuplicateAggregator (pandas-based)
            dup_agg = DuplicateAggregator(uniqueness_columns)
            for dataset_label, df_curr in items:
                dup_agg.add_df(df_curr)
            metrics, recommendations = dup_agg.finalize_metrics(
                pack.source_config["name"]
            )
            pack.metrics.data.extend(metrics)
            try:
                score_item = next(
                    m for m in metrics if m.get("key") == "score"
                )
                score_val = float(score_item.get("value", 1.0))
                duplication_rate = 1.0 - score_val
                if score_val < 0.9:
                    pack.recommendations.data.append(
                        {
                            "content": f"dataset '{pack.source_config['name']}' has a duplication rate of {duplication_rate*100}% on the scope {list(uniqueness_columns)}.",
                            "type": "Duplicates",
                            "scope": {
                                "perimeter": "dataset",
                                "value": pack.source_config["name"],
                            },
                            "level": determine_recommendation_level(
                                duplication_rate
                            ),
                        }
                    )
            except StopIteration:
                pass
    else:
        for dataset_label, df_curr in items:
            if (
                "job" in pack.pack_config
                and "compute_uniqueness_columns" in pack.pack_config["job"]
                and len(pack.pack_config["job"]["compute_uniqueness_columns"])
                > 0
            ):
                uniqueness_columns = pack.pack_config["job"][
                    "compute_uniqueness_columns"
                ]
            else:
                uniqueness_columns = list(df_curr.columns)

            print("Columns used for checking duplicates:", uniqueness_columns)
            df_subset = df_curr[uniqueness_columns].copy()
            duplicates = df_subset.duplicated()
            total_rows = len(df_curr)
            total_duplicates = duplicates.sum()

            print("[", dataset_label, "] total rows " + str(total_rows))
            print(
                "[",
                dataset_label,
                "] total duplicates " + str(total_duplicates),
            )

            duplication_score = round(
                total_duplicates / total_rows if total_rows > 0 else 0, 2
            )
            score = 1 - duplication_score

            pack.metrics.data.append(
                {
                    "key": "score",
                    "value": str(round(score, 2)),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            pack.metrics.data.append(
                {
                    "key": "duplicates",
                    "value": int(total_duplicates),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            #  distinct_count and distinct_percent metrics
            distinct_count = total_rows - total_duplicates
            distinct_percent = (
                distinct_count / total_rows if total_rows > 0 else 0
            )
            pack.metrics.data.append(
                {
                    "key": "distinct_count",
                    "value": int(distinct_count),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            pack.metrics.data.append(
                {
                    "key": "distinct_percent",
                    "value": str(round(distinct_percent, 4)),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            if (
                "job" in pack.pack_config
                and "compute_uniqueness_columns" in pack.pack_config["job"]
            ):
                pack.metrics.data.append(
                    {
                        "key": "duplicates",
                        "value": int(total_duplicates),
                        "scope": {
                            "perimeter": "dataset",
                            "value": ", ".join(uniqueness_columns),
                        },
                    }
                )

            if score < 0.9:
                recommendation = {
                    "content": f"dataset '{dataset_label}' has a duplication rate of {duplication_score*100}% on the scope {list(uniqueness_columns)}.",
                    "type": "Duplicates",
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                    "level": determine_recommendation_level(duplication_score),
                }
                pack.recommendations.data.append(recommendation)

    pack.metrics.save()
    pack.recommendations.save()

    ######################## Export:
    # Step 1: Retrieve 'id_columns' from pack_config
    id_columns = pack.pack_config.get("job", {}).get("id_columns", [])

    # Step 2: Identify duplicated rows (for the first dataset only for export simplicity)
    export_uniqueness = pack.pack_config.get("job", {}).get(
        "compute_uniqueness_columns"
    ) or (list(items[0][1].columns) if items else [])

    # Try Polars for efficient duplicate extraction
    if use_polars_direct and parquet_paths:
        try:
            duplicated_rows = _get_duplicate_rows_polars(
                parquet_paths,
                list(export_uniqueness),
                limit=MAX_DUPLICATES_TO_EXPORT,
            )
            if len(duplicated_rows) >= MAX_DUPLICATES_TO_EXPORT:
                print(
                    f"Limiting duplicate export to {MAX_DUPLICATES_TO_EXPORT:,} rows"
                )
        except Exception as e:
            logger.warning(
                f"Polars duplicate extraction failed: {e}, falling back to pandas"
            )
            # Fallback to pandas
            if isinstance(pack.df_source, list):
                export_df = items[0][1]
            else:
                export_df = _load_parquet_if_path(pack.df_source)
            export_duplicates = export_df[list(export_uniqueness)].duplicated()
            duplicated_rows = export_df[export_duplicates].head(
                MAX_DUPLICATES_TO_EXPORT
            )
    else:
        if isinstance(pack.df_source, list):
            export_df = items[0][1]
        else:
            export_df = _load_parquet_if_path(pack.df_source)
        export_duplicates = export_df[list(export_uniqueness)].duplicated()
        duplicated_rows = export_df[export_duplicates].head(
            MAX_DUPLICATES_TO_EXPORT
        )

    # Check if there are any duplicates
    if duplicated_rows.empty:
        print("No duplicates found. No report will be generated.")
    else:
        # Step 3: Set index or create 'index' column for the Excel export
        if id_columns:
            # Ensure all id_columns are in the DataFrame columns
            valid_id_columns = [
                col for col in id_columns if col in duplicated_rows.columns
            ]
            if not valid_id_columns:
                print(
                    "None of the specified 'id_columns' are in the DataFrame. Using default index."
                )
                duplicated_rows = duplicated_rows.reset_index(drop=True)
            else:
                duplicated_rows = duplicated_rows.set_index(valid_id_columns)
        else:
            # If 'id_columns' is not provided or is empty, create an 'index' column with the original DataFrame's index
            duplicated_rows = duplicated_rows.reset_index()

        # Continue with the export process
        if pack.source_config["type"] == "file":
            source_file_dir = os.path.dirname(
                pack.source_config["config"]["path"]
            )
            current_date = datetime.now().strftime("%Y%m%d")
            report_file_path = os.path.join(
                source_file_dir,
                f'{current_date}_duplicates_finder_report_{pack.source_config["name"]}.xlsx',
            )

            # Export duplicated rows to an Excel file
            duplicated_rows.to_excel(
                report_file_path, index=False
            )  # Set index=False as 'original_index' is now a column
            print(f"Duplicated rows have been exported to {report_file_path}")
