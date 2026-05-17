import warnings

# Silence noisy pkg_resources deprecation warning triggered by fs namespace package
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r"pkg_resources is deprecated as an API",
)
# Silence Spark/Ray unsupported Python >=3.12 warning emitted by optional dependencies
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r"Python 3\.12 and above currently is not supported by Spark and Ray\. Please note that some functionality will not work and currently is not supported\.",
)
import re
import os
import logging
import pandas as pd
import datacompy
from datetime import datetime
from qalita_core.pack import Pack

logger = logging.getLogger(__name__)

# Big data configuration
MAX_ROWS_FOR_FULL_COMPARE = 1_000_000  # Sample if more than 1M rows
SAMPLE_SIZE_FOR_LARGE_DATASETS = 500_000  # Sample size for large datasets
MAX_MISMATCHES_TO_EXPORT = (
    10_000  # Limit mismatch export to avoid memory issues
)

# Try to import Polars for efficient operations
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


def _get_row_count_efficient(paths):
    """Get row count efficiently without loading all data."""
    if not paths:
        return 0

    if isinstance(paths, str):
        paths = [paths]

    parquet_paths = [
        p
        for p in paths
        if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
    ]
    if not parquet_paths:
        return 0

    if POLARS_AVAILABLE:
        try:
            lf = pl.scan_parquet(parquet_paths)
            return lf.select(pl.len()).collect(engine="streaming").item()
        except Exception:
            pass

    try:
        import pyarrow.parquet as pq

        total = 0
        for path in parquet_paths:
            pf = pq.ParquetFile(path)
            total += pf.metadata.num_rows
        return total
    except Exception:
        return 0


def _load_parquet_with_sampling(paths, max_rows=None):
    """
    Load parquet files with optional sampling for big data.

    Returns:
        tuple: (DataFrame, is_sampled, original_row_count)
    """
    if not paths:
        return pd.DataFrame(), False, 0

    if isinstance(paths, str):
        paths = [paths]

    parquet_paths = [
        p
        for p in paths
        if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
    ]
    if not parquet_paths:
        return pd.DataFrame(), False, 0

    total_rows = _get_row_count_efficient(parquet_paths)
    effective_max_rows = max_rows or MAX_ROWS_FOR_FULL_COMPARE

    if total_rows > effective_max_rows:
        sample_size = min(SAMPLE_SIZE_FOR_LARGE_DATASETS, effective_max_rows)
        logger.info(
            f"Large dataset ({total_rows:,} rows). Sampling {sample_size:,} rows."
        )
        print(
            f"Large dataset ({total_rows:,} rows). Sampling {sample_size:,} rows for comparison."
        )

        if POLARS_AVAILABLE:
            try:
                lf = pl.scan_parquet(parquet_paths)
                df = lf.head(sample_size).collect().to_pandas()
                return df, True, total_rows
            except Exception as e:
                logger.warning(f"Polars sampling failed: {e}")

        try:
            df = pd.read_parquet(parquet_paths[0], engine="pyarrow")
            if len(df) > sample_size:
                df = df.head(sample_size)
            return df, True, total_rows
        except Exception:
            pass

    # Load full dataset
    try:
        if POLARS_AVAILABLE:
            lf = pl.scan_parquet(parquet_paths)
            df = lf.collect().to_pandas()
        else:
            if len(parquet_paths) == 1:
                df = pd.read_parquet(parquet_paths[0], engine="pyarrow")
            else:
                dfs = [
                    pd.read_parquet(p, engine="pyarrow") for p in parquet_paths
                ]
                df = pd.concat(dfs, ignore_index=True)
        return df, False, len(df)
    except Exception as e:
        logger.error(f"Failed to load parquet: {e}")
        return pd.DataFrame(), False, 0


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

    if pack.target_config.get("type") == "database":
        table_or_query = pack.target_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "Pour une cible de type 'database', il faut spécifier 'table_or_query' dans la config."
            )
        pack.load_data("target", table_or_query=table_or_query)
    else:
        pack.load_data("target")

    # Checking if the columns exist in the DataFrames
    compare_col_list = pack.pack_config["job"].get("compare_col_list", [])
    id_columns = pack.pack_config["job"].get("id_columns", [])
    abs_tol = pack.pack_config["job"].get("abs_tol", 0.0001)
    rel_tol = pack.pack_config["job"].get("rel_tol", 0)

    def _load_parquet_if_path(obj):
        """Load parquet with automatic sampling for large datasets."""
        try:
            if isinstance(obj, str) and obj.lower().endswith(
                (".parquet", ".pq")
            ):
                df, is_sampled, orig_rows = _load_parquet_with_sampling([obj])
                if is_sampled:
                    print(f"  Sampled from {orig_rows:,} rows")
                return df
            elif isinstance(obj, list):
                df, is_sampled, orig_rows = _load_parquet_with_sampling(obj)
                if is_sampled:
                    print(f"  Sampled from {orig_rows:,} rows")
                return df
        except Exception:
            pass
        return obj

    raw_source = pack.df_source
    raw_target = pack.df_target

    # Normalize to list of (label, df)
    def to_items(raw_df, conf, default_name):
        if isinstance(raw_df, list):
            loaded = [_load_parquet_if_path(x) for x in raw_df]
            names = conf.get("config", {}).get("table_or_query")
            if isinstance(names, (list, tuple)) and len(names) == len(loaded):
                return list(zip(list(names), loaded))
            else:
                return [
                    (f"{default_name}_{i+1}", df)
                    for i, df in enumerate(loaded)
                ]
        else:
            return [(default_name, _load_parquet_if_path(raw_df))]

    source_items = to_items(
        raw_source, pack.source_config, pack.source_config["name"]
    )
    target_items = to_items(
        raw_target, pack.target_config, pack.target_config["name"]
    )

    # Compare pairwise by index if both are lists, otherwise single pair
    pairings = []
    if len(source_items) == 1 and len(target_items) == 1:
        pairings = [
            (source_items[0], target_items[0])
        ]  # ((s_label, s_df), (t_label, t_df))
    elif len(source_items) == len(target_items):
        pairings = list(zip(source_items, target_items))
    else:
        # Fallback: compare first of each and warn
        print(
            "Source/Target tables count mismatch; comparing first dataset of each."
        )
        pairings = [(source_items[0], target_items[0])]

    for (s_label, s_df), (t_label, t_df) in pairings:
        s_cols = set(s_df.columns)
        t_cols = set(t_df.columns)
        use_cols = compare_col_list or list(s_cols.intersection(t_cols))
        missing_in_source = [
            col for col in use_cols if col not in s_df.columns
        ]
        missing_in_target = [
            col for col in use_cols if col not in t_df.columns
        ]
        if missing_in_source:
            raise ValueError(
                f"Columns missing in source {s_label}: {missing_in_source}"
            )
        if missing_in_target:
            raise ValueError(
                f"Columns missing in target {t_label}: {missing_in_target}"
            )

        combined_columns_list = list(dict.fromkeys(use_cols + id_columns))
        if len(id_columns) == 0:
            id_columns = use_cols

        # Take explicit copies to avoid pandas SettingWithCopyWarning from downstream mutations
        df_source_subset = s_df.loc[:, combined_columns_list].copy()
        df_target_subset = t_df.loc[:, combined_columns_list].copy()

        ############################ Comparison using datacompy
        compare = datacompy.Compare(
            df_source_subset,
            df_target_subset,
            join_columns=id_columns,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
            df1_name=s_label,
            df2_name=t_label,
        )

        comparison_report = compare.report(sample_count=10, column_count=10)

        # Exporting comparison metrics :

        pack.metrics.data.append(
            {
                "key": "dataframe_summary_number_columns_" + s_label,
                "value": compare.df1.shape[1],
                "scope": {"perimeter": "dataset", "value": s_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "dataframe_summary_number_columns_" + t_label,
                "value": compare.df2.shape[1],
                "scope": {"perimeter": "dataset", "value": t_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "dataframe_summary_number_rows_" + s_label,
                "value": compare.df1.shape[0],
                "scope": {"perimeter": "dataset", "value": s_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "dataframe_summary_number_rows_" + t_label,
                "value": compare.df2.shape[0],
                "scope": {"perimeter": "dataset", "value": t_label},
            }
        )

        # Optionally, save the report to an HTML file
        with open(f"comparison_report_{s_label}_vs_{t_label}.txt", "w") as f:
            f.write(comparison_report)

        ############################ Extracting metrics from the report
        # Dictionary to hold the extracted data
        extracted_data = {}

        # Define patterns for the parts you want to extract
        patterns = {
            "column_summary": r"Column Summary\s+-+\s+([\s\S]+?)\n\n",
            "row_summary": r"Row Summary\s+-+\s+([\s\S]+?)\n\n",
            "column_comparison": r"Column Comparison\s+-+\s+([\s\S]+?)\n\n",
        }

        # Extract the data using the patterns
        for key, pattern in patterns.items():
            match = re.search(pattern, comparison_report, re.DOTALL)
            if match:
                section_content = match.group(1)
                extracted_data[key] = dict(
                    re.findall(r"([^\n:]+):\s*(\d+)", section_content)
                )

        # Convert extracted data to metrics
        for section, data in extracted_data.items():
            for key, value in data.items():
                pack.metrics.data.append(
                    {
                        "key": f"{section}_{key.lower().replace(' ', '_')}",
                        "value": value,
                        "scope": {"perimeter": "dataset", "value": s_label},
                    }
                )

        ############################ Computing the matching score
        metrics_values = {
            "Number of rows in common": 0,
            "Number of rows in Target but not in Source": 0,
            "Number of rows in Source but not in Target": 0,
        }

        source_name = s_label.lower().replace(" ", "_")
        target_name = t_label.lower().replace(" ", "_")

        base_keys = {
            "row_summary_number_of_rows_in_common": "Number of rows in common",
            f"row_summary_number_of_rows_in_{source_name}_but_not_in_{target_name}": "Number of rows in Target but not in Source",
            f"row_summary_number_of_rows_in_{target_name}_but_not_in_{source_name}": "Number of rows in Source but not in Target",
        }

        for metric in pack.metrics.data:
            for base_key, value_key in base_keys.items():
                if (
                    base_key in metric["key"]
                    and metric.get("scope", {}).get("value") == s_label
                ):
                    metrics_values[value_key] = int(metric["value"])
                    print(f"Found {value_key} with value {metric['value']}")
                    break

        num_rows_in_common = metrics_values["Number of rows in common"]
        num_rows_in_target_not_in_source = metrics_values[
            "Number of rows in Target but not in Source"
        ]
        num_rows_in_source_not_in_target = metrics_values[
            "Number of rows in Source but not in Target"
        ]

        df_all_mismatch = compare.all_mismatch(ignore_matching_cols=True)

        total_target_rows = len(t_df)
        print(
            f"[{s_label} vs {t_label}] Total rows in target: {total_target_rows}"
        )
        if total_target_rows == 0:
            print(
                "Cannot compute the score as the total number of rows in target is zero."
            )
        else:
            num_mismatches = len(df_all_mismatch)
            if num_mismatches == 0:
                score = 1.0
            else:
                score = max(0, 1 - (num_mismatches / total_target_rows))
            print(f"Matching score: {score}")
            pack.metrics.data.append(
                {
                    "key": "score",
                    "value": str(round(score, 2)),
                    "scope": {"perimeter": "dataset", "value": s_label},
                }
            )

        if total_target_rows == 0:
            precision = 0
        else:
            precision = num_rows_in_common / total_target_rows

        total_source_rows = len(s_df)
        if total_source_rows == 0:
            recall = 0
        else:
            recall = num_rows_in_common / total_source_rows

        print(f"Precision: {precision}")
        print(f"Recall: {recall}")

        if precision + recall == 0:
            f1_score = 0
        else:
            f1_score = 2 * (precision * recall) / (precision + recall)

        print(f"F1 Score: {f1_score}")

        pack.metrics.data.extend(
            [
                {
                    "key": "precision",
                    "value": str(round(precision, 2)),
                    "scope": {"perimeter": "dataset", "value": s_label},
                },
                {
                    "key": "recall",
                    "value": str(round(recall, 2)),
                    "scope": {"perimeter": "dataset", "value": s_label},
                },
                {
                    "key": "f1_score",
                    "value": str(round(f1_score, 2)),
                    "scope": {"perimeter": "dataset", "value": s_label},
                },
            ]
        )

        columnLabels = df_all_mismatch.columns.tolist()
        suffix_mapping = {"_df1": "_source", "_df2": "_target"}
        new_columnLabels = [
            (
                col
                if not any(
                    col.endswith(suffix) for suffix in suffix_mapping.keys()
                )
                else next(
                    col.replace(suffix, replacement)
                    for suffix, replacement in suffix_mapping.items()
                    if col.endswith(suffix)
                )
            )
            for col in columnLabels
        ]
        df_all_mismatch.columns = new_columnLabels

        # Limit mismatches to avoid memory issues with large datasets
        mismatch_count = len(df_all_mismatch)
        if mismatch_count > MAX_MISMATCHES_TO_EXPORT:
            print(
                f"Limiting mismatch export from {mismatch_count:,} to {MAX_MISMATCHES_TO_EXPORT:,} rows"
            )
            df_mismatch_export = df_all_mismatch.head(MAX_MISMATCHES_TO_EXPORT)
        else:
            df_mismatch_export = df_all_mismatch

        # Use vectorized to_dict instead of iterrows for better performance
        data_formatted = df_mismatch_export.to_dict(orient="records")
        data_formatted = [
            [{"value": row.get(col)} for col in df_mismatch_export.columns]
            for row in data_formatted
        ]
        format_structure = {
            "columnLabels": new_columnLabels,
            "data": data_formatted,
        }

        # Add truncation note if applicable
        if mismatch_count > MAX_MISMATCHES_TO_EXPORT:
            format_structure["truncated"] = True
            format_structure["total_mismatches"] = mismatch_count

        pack.metrics.data.extend(
            [
                {
                    "key": "recommendation_levels_mismatches",
                    "value": {"info": "0", "warning": "0.5", "high": "0.8"},
                    "scope": {"perimeter": "dataset", "value": s_label},
                },
                {
                    "key": "mismatches_table",
                    "value": format_structure,
                    "scope": {"perimeter": "dataset", "value": s_label},
                },
            ]
        )

        ######################## Export per pairing
        if not df_all_mismatch.empty and pack.source_config["type"] == "file":
            source_file_dir = os.path.dirname(
                pack.source_config["config"]["path"]
            )
            current_date = datetime.now().strftime("%Y%m%d")
            report_file_path = os.path.join(
                source_file_dir,
                f"{current_date}_data_compare_report_{s_label}_vs_{t_label}.xlsx",
            )
            df_all_mismatch.to_excel(report_file_path, index=False)
            print(f"mismatches rows have been exported to {report_file_path}")

    # Save metrics once after processing all pairings
    pack.metrics.save()
