from qalita_core.pack import Pack
import pandas as pd
import re

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

    def _load_parquet_if_path(obj):
        try:
            if isinstance(obj, str) and obj.lower().endswith(
                (".parquet", ".pq")
            ):
                return pd.read_parquet(obj, engine="pyarrow")
        except Exception:
            pass
        return obj

    df = pack.df_source
    if isinstance(df, list):
        loaded = [_load_parquet_if_path(x) for x in df]
        datasets = [
            (name, data)
            for name, data in zip(
                pack.source_config.get("config", {}).get("table_or_query", []),
                loaded,
            )
        ]
    else:
        datasets = [(pack.source_config["name"], _load_parquet_if_path(df))]

    patterns = pack.pack_config.get("job", {}).get("pii_patterns", [])
    compiled = [(p["key"], re.compile(p["regex"])) for p in patterns]

    total_pii_columns = 0
    total_rows_with_pii = 0
    total_rows = 0

    for dataset_label, df_curr in datasets:
        rows_with_pii = set()
        per_column_hits = {}
        for column in df_curr.columns:
            column_hits = 0
            for key, regex in compiled:
                matches = (
                    df_curr[column]
                    .astype(str)
                    .str.contains(regex)
                    .fillna(False)
                )
                hit_count = int(matches.sum())
                if hit_count > 0:
                    rows_with_pii.update(df_curr.index[matches])
                    column_hits += hit_count
                    pack.metrics.data.append(
                        {
                            "key": "pii_hits",
                            "value": hit_count,
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
            if column_hits > 0:
                total_pii_columns += 1
        total_rows_with_pii += len(rows_with_pii)
        total_rows += len(df_curr)

    pack.metrics.data.append(
        {
            "key": "pii_columns",
            "value": str(total_pii_columns),
            "scope": {"perimeter": "dataset", "value": datasets[0][0]},
        }
    )

    ratio = 0 if total_rows == 0 else total_rows_with_pii / total_rows
    pack.metrics.data.append(
        {
            "key": "pii_records_ratio",
            "value": str(round(ratio, 4)),
            "scope": {"perimeter": "dataset", "value": datasets[0][0]},
        }
    )

    pack.metrics.save()
