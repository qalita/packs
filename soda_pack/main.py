from soda.scan import Scan
from qalita_core.pack import Pack
import pandas as pd
from qalita_core.utils import (
    determine_recommendation_level,
    replace_whitespaces_with_underscores,
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

    ########################### Support single or multiple datasets
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
        else:
            base = (
                pack.source_config["name"].replace(" ", "_").replace("-", "_")
            )
            items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
    else:
        items = [
            (pack.source_config["name"], _load_parquet_if_path(raw_df_source))
        ]

    for dataset_label, df_raw in items:
        # Dictionary to hold the association between slugified and original column names
        df, column_name_association = replace_whitespaces_with_underscores(
            df_raw
        )

        print("Slugified Columns :", df.columns)
        print(
            "Association table between original col names and slugified ones :",
            column_name_association,
        )

        ########################### Scan
        scan = Scan()
        data_source_name = dataset_label.replace(" ", "_").replace("-", "_")
        scan.set_data_source_name(data_source_name)
        scan.add_pandas_dataframe(
            data_source_name=data_source_name,
            dataset_name=data_source_name,
            pandas_df=df,
        )

        # Add check YAML files
        scan.add_sodacl_yaml_files("checks.yaml")

        # Execute the scan
        scan.execute()

        results = scan.get_scan_results()
        checks = results["checks"]
        ############################ Metrics

        # Reformat the checks object to the desired format
        for check in results["metrics"]:
            identity_parts = check["identity"].split("-")
            source_column = (
                identity_parts[3] if len(identity_parts) > 3 else None
            )
            if source_column == check["metricName"]:
                scope = {"perimeter": "dataset", "value": dataset_label}
            else:
                original_column_name = column_name_association.get(
                    source_column
                )
                scope = {
                    "perimeter": "column",
                    "value": original_column_name,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_label,
                    },
                }

            pack.metrics.data.append(
                {
                    "key": check["metricName"],
                    "value": check["value"],
                    "scope": scope,
                }
            )

        # Initialize counters for pass and total checks
        total_pass_count = 0
        total_checks = len(checks)
        for check in checks:
            if check["outcome"] == "pass":
                total_pass_count += 1

        dataset_score = (
            total_pass_count / total_checks if total_checks > 0 else 0
        )
        print(f"[{dataset_label}] Total Checks: {total_checks}")
        print(f"[{dataset_label}] Passed Checks: {total_pass_count}")
        print(f"[{dataset_label}] Score: {dataset_score:.2f}")

        pack.metrics.data.append(
            {
                "key": "score",
                "value": round(dataset_score, 2),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "check_passed",
                "value": total_pass_count,
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "check_failed",
                "value": (total_checks - total_pass_count),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )

        # Per-column score
        column_pass_count = {}
        column_total_checks = {}
        for check in checks:
            column = check.get("column") or "dataset"
            column_pass_count[column] = column_pass_count.get(column, 0) + (
                1 if check["outcome"] == "pass" else 0
            )
            column_total_checks[column] = (
                column_total_checks.get(column, 0) + 1
            )

        for column, total in column_total_checks.items():
            pass_count = column_pass_count.get(column, 0)
            score = pass_count / total if total > 0 else 0
            column_name = (
                column.replace('"', "")
                if column != "dataset"
                else dataset_label
            )
            original_column_name = column_name_association.get(
                column_name, column_name
            )

            pack.metrics.data.append(
                {
                    "key": "check_completion_score",
                    "value": round(
                        score if column != "dataset" else dataset_score, 2
                    ),
                    "scope": {
                        "perimeter": (
                            "column" if column != "dataset" else "dataset"
                        ),
                        "value": original_column_name,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_label,
                        },
                    },
                }
            )

        ################# RECOMMENDATIONS #################
        score = dataset_score
        if score is not None and score < 1:
            pack.recommendations.data.append(
                {
                    "content": f"The dataset '{dataset_label}' has PASSED {total_pass_count}/{total_checks} checks giving a score of {score*100}%.",
                    "type": "Checks Failed",
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                    "level": determine_recommendation_level(score),
                }
            )

        for check in checks:
            if check["outcome"] != "pass":
                if check["column"] is not None:
                    original_column_name = column_name_association.get(
                        check["column"]
                    )
                    pack.recommendations.data.append(
                        {
                            "content": check["definition"],
                            "type": check["name"],
                            "scope": {
                                "perimeter": "column",
                                "value": original_column_name,
                                "parent_scope": {
                                    "perimeter": "dataset",
                                    "value": dataset_label,
                                },
                            },
                            "level": "high",
                        }
                    )
                else:
                    pack.recommendations.data.append(
                        {
                            "content": check["definition"],
                            "type": "Checks Failed",
                            "scope": {
                                "perimeter": "dataset",
                                "value": dataset_label,
                            },
                            "level": "high",
                        }
                    )

    pack.recommendations.save()
    pack.metrics.save()
