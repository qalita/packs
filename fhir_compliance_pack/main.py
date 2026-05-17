from qalita_core.pack import Pack
import pandas as pd
import re
from datetime import datetime


def is_boolean_like(value):
    return str(value).lower() in {"true", "false", "1", "0", "yes", "no"}


def is_iso_date(value):
    try:
        datetime.fromisoformat(str(value))
        return True
    except Exception:
        return False


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

    cfg = pack.pack_config.get("job", {})
    field_mappings = cfg.get("field_mappings", {})
    required_fields = cfg.get("required_fields", [])
    enums = cfg.get("enums", {})
    patterns = {k: re.compile(v) for k, v in cfg.get("patterns", {}).items()}
    date_fields = set(cfg.get("date_fields", []))
    boolean_fields = set(cfg.get("boolean_fields", []))

    total_records = 0
    valid_records = 0
    completeness_sum = 0

    for dataset_label, df_curr in datasets:
        # Map to standardized FHIR field names
        mapped = {}
        for fhir_field, col in field_mappings.items():
            if col in df_curr.columns:
                mapped[fhir_field] = df_curr[col]
            else:
                mapped[fhir_field] = None

        row_count = len(df_curr)
        total_records += row_count

        for idx in range(row_count):
            complete_fields = 0
            field_count = len(field_mappings)
            record_valid = True
            for fhir_field, series in mapped.items():
                value = None if series is None else series.iloc[idx]
                # Required check
                if fhir_field in required_fields and (
                    value is None or str(value).strip() == ""
                ):
                    record_valid = False
                # Enum check
                if (
                    fhir_field in enums
                    and value is not None
                    and str(value).strip() != ""
                ):
                    if str(value) not in enums[fhir_field]:
                        record_valid = False
                # Pattern check
                if (
                    fhir_field in patterns
                    and value is not None
                    and str(value).strip() != ""
                ):
                    if not patterns[fhir_field].match(str(value)):
                        record_valid = False
                # Type hints
                if (
                    fhir_field in date_fields
                    and value is not None
                    and str(value).strip() != ""
                ):
                    if not is_iso_date(value):
                        record_valid = False
                if (
                    fhir_field in boolean_fields
                    and value is not None
                    and str(value).strip() != ""
                ):
                    if not is_boolean_like(value):
                        record_valid = False

                if value is not None and str(value).strip() != "":
                    complete_fields += 1

            completeness_ratio = (
                0 if field_count == 0 else complete_fields / field_count
            )
            completeness_sum += completeness_ratio
            if record_valid:
                valid_records += 1

        # Per-dataset metrics
        pack.metrics.data.append(
            {
                "key": "completeness",
                "value": str(
                    round(completeness_sum / max(1, total_records), 4)
                ),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )

    validity_ratio = 0 if total_records == 0 else valid_records / total_records
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(validity_ratio, 2)),
            "scope": {"perimeter": "dataset", "value": datasets[0][0]},
        }
    )
    pack.metrics.data.append(
        {
            "key": "validity_ratio",
            "value": str(round(validity_ratio, 4)),
            "scope": {"perimeter": "dataset", "value": datasets[0][0]},
        }
    )

    pack.metrics.save()
