import json
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
import re
import os
from qalita_core.pack import Pack
from qalita_core.aggregation import (
    detect_chunked_from_items,
    TimelinessAggregator,
    normalize_and_dedupe_recommendations,
)

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

    ############################ Infer Data Types and Count Date Columns

    date_columns_count = 0  # Initialize the counter for date columns

    def is_date(string):
        # Define patterns for date formats
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",  # yyyy-mm-dd
            r"^\d{4}/\d{2}/\d{2}$",  # yyyy/mm/dd
            r"^\d{2}-\d{2}-\d{4}$",  # mm-dd-yyyy
            r"^\d{2}/\d{2}/\d{4}$",  # dd/mm/yyyy
            r"^\d{2}-\d{2}-\d{4}$",  # dd-mm-yyyy
            r"^\d{2}/\d{2}/\d{4}$",  # mm/dd/yyyy
            r"^\d{4}\.\d{2}\.\d{2}$",  # yyyy.mm.dd
            r"^\d{2}\.\d{2}\.\d{4}$",  # dd.mm.yyyy
            r"^\d{2}\.\d{2}\.\d{4}$",  # mm.dd.yyyy
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",  # yyyy-mm-dd HH:MM:SS
        ]

        # Convert the input to a string before checking
        string = str(string)

        # Check if the string is a year
        if re.match(r"^\d{4}$", string):
            # Check if it's a valid year (e.g., between 1900 and the current year)
            year = int(string)
            if 1900 <= year <= datetime.now().year:
                return "year_only"
            else:
                return False

        # Check if the string matches any of the date patterns
        if any(re.match(pattern, string) for pattern in date_patterns):
            try:
                # If the format is correct, try parsing it
                parse(string)
                return True
            except ValueError:
                return False
        return False

    def calculate_timeliness_score(days_since):
        # Score is 1.0 if days_since is 0,
        # and decreases linearly to 0.0 if days_since is 365 or more
        return max(0.0, 1 - (days_since / 365))

    raw_df_source = pack.df_source
    configured = pack.pack_config.get("job", {}).get("compute_score_columns")

    # Safely convert a Series to datetime, coercing invalid/out-of-bounds values
    def safe_to_datetime(series):
        try:
            # Pandas >=2.0 supports format="mixed" for heterogeneous inputs
            return pd.to_datetime(series, errors="coerce", format="mixed")
        except Exception:
            try:
                # Fallback: be lenient and coerce problematic entries
                return pd.to_datetime(series, errors="coerce", dayfirst=True)
            except Exception:
                return pd.to_datetime(series, errors="coerce")

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
        names = pack.source_config.get("config", {}).get("table_or_query")
        loaded = [_load_parquet_if_path(x) for x in raw_df_source]
        if isinstance(names, (list, tuple)) and len(names) == len(loaded):
            dataset_items = list(zip(list(names), loaded))
            names_for_detect = [str(n) for n in names]
        else:
            base = pack.source_config["name"]
            dataset_items = [
                (f"{base}_{i+1}", df) for i, df in enumerate(loaded)
            ]
            names_for_detect = [name for name, _ in dataset_items]
    else:
        dataset_items = [
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

    tim_agg = TimelinessAggregator()
    for dataset_label, df_curr in dataset_items:
        for column in df_curr.columns:
            unique_values = df_curr[column].dropna().unique()
            sample_size = min(10, len(unique_values))
            sample_values = unique_values[:sample_size]

            date_type = set(is_date(value) for value in sample_values)

            if "year_only" in date_type:
                year_data = df_curr[column].astype(int)
                earliest_year = int(year_data.min())
                latest_year = int(year_data.max())
                tim_agg.add_year_obs(column, earliest_year, latest_year)
            elif True in date_type:
                converted = safe_to_datetime(df_curr[column])
                if not converted.notna().any():
                    continue
                df_curr[column] = converted
                date_columns_count += 1
                earliest_date = converted.min()
                latest_date = converted.max()
                if pd.isnull(earliest_date) or pd.isnull(latest_date):
                    continue
                tim_agg.add_date_obs(column, earliest_date, latest_date)

    ############################ (moved) Dataset score will be computed after column timeliness_score

    # Data to be written to JSON
    # Ajoute un compteur global approximatif (toutes tables confondues)
    pack.metrics.data.append(
        {
            "key": "date_columns_count",
            "value": str(date_columns_count),
            "scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        }
    )

    ############################  Data Staleness (based on file metadata)
    # Compute data_staleness_days based on file modification time
    if pack.source_config.get("type") == "file":
        try:
            file_path = pack.source_config.get("config", {}).get("path")
            if file_path and os.path.exists(file_path):
                mtime = os.path.getmtime(file_path)
                staleness_days = (
                    datetime.now() - datetime.fromtimestamp(mtime)
                ).days
                pack.metrics.data.append(
                    {
                        "key": "data_staleness_days",
                        "value": str(staleness_days),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )
                print(
                    f"Data staleness: {staleness_days} days since last file modification"
                )
        except Exception as e:
            print(f"Could not compute data staleness: {e}")
    elif pack.source_config.get("type") == "folder":
        # For folder sources, get the most recent file modification time
        try:
            folder_path = pack.source_config.get("config", {}).get("path")
            if folder_path and os.path.exists(folder_path):
                latest_mtime = 0
                for root, dirs, files in os.walk(folder_path):
                    for f in files:
                        fpath = os.path.join(root, f)
                        mtime = os.path.getmtime(fpath)
                        if mtime > latest_mtime:
                            latest_mtime = mtime
                if latest_mtime > 0:
                    staleness_days = (
                        datetime.now() - datetime.fromtimestamp(latest_mtime)
                    ).days
                    pack.metrics.data.append(
                        {
                            "key": "data_staleness_days",
                            "value": str(staleness_days),
                            "scope": {
                                "perimeter": "dataset",
                                "value": pack.source_config["name"],
                            },
                        }
                    )
                    print(
                        f"Data staleness: {staleness_days} days since last file modification in folder"
                    )
        except Exception as e:
            print(f"Could not compute data staleness for folder: {e}")

    ############################ Compute timeliness_score for Each Column

    if date_columns_count > 0 and treat_chunks_as_one:
        compute_score_columns = pack.pack_config.get("job", {}).get(
            "compute_score_columns"
        )
        metrics, recommendations = tim_agg.finalize_metrics(
            dataset_scope_name=pack.source_config["name"],
            compute_score_columns=compute_score_columns,
            calc_timeliness_score=calculate_timeliness_score,
        )
        pack.metrics.data.extend(metrics)
        pack.recommendations.data.extend(recommendations)
    elif date_columns_count > 0:
        # (legacy per-dataset behavior)
        # Iterate through metrics_data and calculate timeliness_score for each column
        for item in pack.metrics.data:
            if item["key"] == "days_since_latest_date":
                column_name = item["scope"]["value"]
                dataset_label = (
                    item.get("scope", {}).get("parent_scope", {}).get("value")
                    or pack.source_config["name"]
                )
                days_since_latest = int(item["value"])
                timeliness_score = calculate_timeliness_score(
                    days_since_latest
                )
                pack.metrics.data.append(
                    {
                        "key": "timeliness_score",
                        "value": str(round(timeliness_score, 2)),
                        "scope": {
                            "perimeter": "column",
                            "value": column_name,
                            "parent_scope": {
                                "perimeter": "dataset",
                                "value": dataset_label,
                            },
                        },
                    }
                )

    ############################ Compute Dataset Score Based on Average timeliness_score

    if date_columns_count > 0 and not treat_chunks_as_one:
        compute_score_columns = pack.pack_config.get("job", {}).get(
            "compute_score_columns"
        )
        if not compute_score_columns:
            compute_score_columns = None
        ts_metrics = [
            m
            for m in pack.metrics.data
            if m.get("key") == "timeliness_score"
            and m.get("scope", {}).get("perimeter") == "column"
        ]
        dataset_to_scores = {}
        for m in ts_metrics:
            scope = m.get("scope", {})
            dataset_label = (
                scope.get("parent_scope", {}).get("value")
                or pack.source_config["name"]
            )
            column_name = scope.get("value")
            if (
                compute_score_columns is None
                or column_name in compute_score_columns
            ):
                try:
                    score_value = float(m.get("value", 0))
                except Exception:
                    continue
                dataset_to_scores.setdefault(dataset_label, []).append(
                    score_value
                )
        for dataset_label, scores in dataset_to_scores.items():
            if not scores:
                continue
            average_score = sum(scores) / len(scores)
            pack.metrics.data.append(
                {
                    "key": "score",
                    "value": str(round(average_score, 2)),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )

    pack.metrics.save()
    pack.recommendations.save()
