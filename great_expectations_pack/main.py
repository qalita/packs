from qalita_core.pack import Pack
import pandas as pd
from great_expectations.dataset import PandasDataset


class QALITADataset(PandasDataset):
    pass


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
        dataset_items = [
            (name, data)
            for name, data in zip(
                pack.source_config.get("config", {}).get("table_or_query", []),
                loaded,
            )
        ]
    else:
        dataset_items = [
            (pack.source_config["name"], _load_parquet_if_path(df))
        ]

    suite_name = pack.pack_config.get("job", {}).get(
        "suite_name", "qalita_default_suite"
    )
    expectations = pack.pack_config.get("job", {}).get("expectations", [])

    total = 0
    passed = 0

    for dataset_label, df_curr in dataset_items:
        gx_ds = QALITADataset(df_curr)
        for exp in expectations:
            exp_type = exp.get("expectation_type")
            kwargs = exp.get("kwargs", {})
            if not hasattr(gx_ds, exp_type):
                continue
            result = getattr(gx_ds, exp_type)(**kwargs)
            success = bool(result.get("success", False))
            total += 1
            passed += 1 if success else 0
            pack.metrics.data.append(
                {
                    "key": "expectation_result",
                    "value": {"expectation": exp_type, "success": success},
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )

    score = 1.0 if total == 0 else passed / total
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(score, 2)),
            "scope": {"perimeter": "dataset", "value": dataset_items[0][0]},
        }
    )

    pack.metrics.save()
