import json
import hashlib
import pandas as pd
from ydata_profiling import ProfileReport
from qalita_core.pack import Pack
from io import StringIO

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
with Pack() as pack:
    if pack.source_config.get("type") == "database":
        table_or_query = pack.source_config.get("config", {}).get("table_or_query")
        if not table_or_query:
            raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
        pack.load_data("source", table_or_query=table_or_query)
    else:
        pack.load_data("source")

    ########################### Profiling and Aggregating Results
    raw_df_source = pack.df_source
    configured = pack.source_config.get("config", {}).get("table_or_query")

    def _load_parquet_if_path(obj):
        try:
            if isinstance(obj, str) and obj.lower().endswith((".parquet", ".pq")):
                return pd.read_parquet(obj, engine="pyarrow")
        except Exception:
            pass
        return obj

    if isinstance(raw_df_source, list):
        loaded = [_load_parquet_if_path(x) for x in raw_df_source]
        if isinstance(configured, (list, tuple)) and len(configured) == len(loaded):
            items = list(zip(list(configured), loaded))
        else:
            base = pack.source_config["name"]
            items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
    else:
        items = [(pack.source_config["name"], _load_parquet_if_path(raw_df_source))]

    for dataset_name, df_curr in items:
        print(f"Generating profile for {dataset_name}")
        profile = ProfileReport(df_curr, minimal=True, title=f"Profiling Report for {dataset_name}")
        html_file_name = f"{dataset_name}_report.html"
        profile.to_file(html_file_name)
        json_file_name = f"{dataset_name}_report.json"
        profile.to_file(json_file_name)
        try:
            with open(html_file_name, "r", encoding="utf-8") as f:
                html_content = f.read()
                tables = pd.read_html(StringIO(html_content))
        except ValueError as e:
            print(f"No tables found in the HTML report: {e}")
            tables = [pd.DataFrame()]

        print(f"Load {dataset_name}_report.json")
        with open(f"{dataset_name}_report.json", "r", encoding="utf-8") as file:
            report = json.load(file)

        for variable_name in report["variables"].keys():
            pack.schemas.data.append(
                {
                    "key": "column",
                    "value": variable_name,
                    "scope": {
                        "perimeter": "column",
                        "value": variable_name,
                        "parent_scope": {"perimeter": "dataset", "value": dataset_name},
                    },
                }
            )

        ############################  Schema Drift Detection Metrics
        # column_count (: column_count check)
        column_count = len(df_curr.columns)
        pack.metrics.data.append({
            "key": "column_count",
            "value": column_count,
            "scope": {"perimeter": "dataset", "value": dataset_name},
        })
        
        # column_list_hash (: column_list_changed detection)
        # Hash of sorted column names to detect schema changes
        column_list_str = ",".join(sorted(df_curr.columns))
        column_list_hash = hashlib.md5(column_list_str.encode(), usedforsecurity=False).hexdigest()
        pack.metrics.data.append({
            "key": "column_list_hash",
            "value": column_list_hash,
            "scope": {"perimeter": "dataset", "value": dataset_name},
        })
        
        # column_order_hash (: column_list_or_order_changed detection)
        # Hash of column names in order to detect order changes
        column_order_str = ",".join(df_curr.columns)
        column_order_hash = hashlib.md5(column_order_str.encode(), usedforsecurity=False).hexdigest()
        pack.metrics.data.append({
            "key": "column_order_hash",
            "value": column_order_hash,
            "scope": {"perimeter": "dataset", "value": dataset_name},
        })
        
        # column_type per column (: column_type_changed detection)
        for col in df_curr.columns:
            col_dtype = str(df_curr[col].dtype)
            pack.metrics.data.append({
                "key": "column_type",
                "value": col_dtype,
                "scope": {
                    "perimeter": "column",
                    "value": col,
                    "parent_scope": {"perimeter": "dataset", "value": dataset_name},
                },
            })
        
        # column_types_hash (: column_types_changed detection)
        # Hash of all column types to detect type changes
        types_str = ",".join([f"{col}:{df_curr[col].dtype}" for col in df_curr.columns])
        column_types_hash = hashlib.md5(types_str.encode(), usedforsecurity=False).hexdigest()
        pack.metrics.data.append({
            "key": "column_types_hash",
            "value": column_types_hash,
            "scope": {"perimeter": "dataset", "value": dataset_name},
        })
        
        print(f"Schema metrics: {column_count} columns, list_hash={column_list_hash[:8]}..., types_hash={column_types_hash[:8]}...")

        if pack.source_config["type"] == "database":
            pack.schemas.data.append(
                {
                    "key": "dataset",
                    "value": dataset_name,
                    "scope": {
                        "perimeter": "dataset",
                        "value": dataset_name,
                        "parent_scope": {"perimeter": "database", "value": pack.source_config["name"]},
                    },
                }
            )
        else:
            pack.schemas.data.append(
                {
                    "key": "dataset",
                    "value": dataset_name,
                    "scope": {"perimeter": "dataset", "value": dataset_name},
                }
            )

    ############################ Writing Results to Files

    if pack.source_config["type"] == "database":
        pack.schemas.data.append(
            {
                "key": "database",
                "value": pack.source_config["name"],
                "scope": {"perimeter": "database", "value": pack.source_config["name"]},
            }
        )

    pack.schemas.save()
    pack.metrics.save()
