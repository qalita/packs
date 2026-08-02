"""
Numeric Validation Pack

Validates numeric data against configurable ranges and constraints.
Covers  checks:
- number_below_min_value, number_above_max_value
- number_in_range_percent, integer_in_range_percent
- negative_values, negative_values_percent
- min_in_range, max_in_range, sum_in_range, mean_in_range
- valid_latitude_percent, valid_longitude_percent
"""

import pandas as pd
import numpy as np
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level


def validate_range(series: pd.Series, min_value=None, max_value=None) -> dict:
    """
    Validate a numeric series against min/max range.

    Returns:
        dict with validation metrics
    """
    data = series.dropna()
    total = len(data)

    if total == 0:
        return {
            "below_min": 0,
            "above_max": 0,
            "in_range_count": 0,
            "in_range_percent": 1.0,
            "min_value": None,
            "max_value": None,
            "sum_value": None,
            "mean_value": None,
        }

    below_min = 0
    above_max = 0

    if min_value is not None:
        below_min = int((data < min_value).sum())
    if max_value is not None:
        above_max = int((data > max_value).sum())

    in_range_count = total - below_min - above_max
    in_range_percent = in_range_count / total if total > 0 else 1.0

    return {
        "below_min": below_min,
        "above_max": above_max,
        "in_range_count": in_range_count,
        "in_range_percent": round(in_range_percent, 4),
        "min_value": float(data.min()),
        "max_value": float(data.max()),
        "sum_value": float(data.sum()),
        "mean_value": float(data.mean()),
    }


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
            base = pack.source_config["name"]
            items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
    else:
        items = [
            (pack.source_config["name"], _load_parquet_if_path(raw_df_source))
        ]

    # Get validation rules from config
    validation_rules = pack.pack_config.get("job", {}).get("rules", [])

    # Track overall validity score
    total_checks = 0
    total_valid_percent = 0

    # Résultats par colonne, accumulés pour les figures. Une même colonne peut être
    # validée plusieurs fois (source chunkée par qalita_core, ou plusieurs règles
    # ciblant la même colonne dans pack_conf) : on ne compte qu'un contrôle par
    # colonne et on additionne ses violations, pour ne produire qu'une ligne par
    # colonne — le dédoublonneur de dimensions du moteur exige un agrégat, pas les
    # exécutions brutes.
    check_outcomes = {}

    pack.figures.declare_measure(
        "score",
        unit="score",
        direction="higher_is_better",
        target=0.95,
        warn=0.8,
        label="Score de validité",
    )
    pack.figures.declare_measure(
        "n_checks", unit="count", direction="neutral", label="Contrôles"
    )
    pack.figures.declare_measure(
        "n_violations",
        unit="count",
        direction="lower_is_better",
        target=0,
        label="Valeurs hors bornes",
    )

    for dataset_label, df_curr in items:
        print(f"Validating numeric ranges for {dataset_label}")

        for rule in validation_rules:
            column = rule.get("column")
            rule_type = rule.get("type")
            min_value = rule.get("min_value")
            max_value = rule.get("max_value")

            if column not in df_curr.columns:
                print(f"Column '{column}' not found in dataset. Skipping.")
                continue

            if not pd.api.types.is_numeric_dtype(df_curr[column]):
                print(f"Column '{column}' is not numeric. Skipping.")
                continue

            col_scope = {
                "perimeter": "column",
                "value": column,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": dataset_label,
                },
            }

            col_data = df_curr[column].dropna()
            if len(col_data) == 0:
                print(f"Column '{column}' is empty. Skipping.")
                continue

            # Handle special types
            if rule_type == "latitude":
                min_value = -90
                max_value = 90
            elif rule_type == "longitude":
                min_value = -180
                max_value = 180
            elif rule_type == "percentage":
                min_value = 0
                max_value = 100
            elif rule_type == "non_negative":
                min_value = 0

            # Validate range
            result = validate_range(col_data, min_value, max_value)

            outcome_entry = check_outcomes.setdefault(
                column, {"column": column, "n_violations": 0}
            )
            outcome_entry["n_violations"] += int(
                result["below_min"] + result["above_max"]
            )

            # Add metrics ( naming convention)
            if result["below_min"] > 0:
                pack.metrics.data.append(
                    {
                        "key": "number_below_min_value",
                        "value": result["below_min"],
                        "scope": col_scope.copy(),
                    }
                )
            if result["above_max"] > 0:
                pack.metrics.data.append(
                    {
                        "key": "number_above_max_value",
                        "value": result["above_max"],
                        "scope": col_scope.copy(),
                    }
                )

            pack.metrics.data.extend(
                [
                    {
                        "key": "number_in_range_percent",
                        "value": str(result["in_range_percent"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "min_value",
                        "value": (
                            str(round(result["min_value"], 4))
                            if result["min_value"] is not None
                            else "null"
                        ),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "max_value",
                        "value": (
                            str(round(result["max_value"], 4))
                            if result["max_value"] is not None
                            else "null"
                        ),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "sum_value",
                        "value": (
                            str(round(result["sum_value"], 4))
                            if result["sum_value"] is not None
                            else "null"
                        ),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "mean_value",
                        "value": (
                            str(round(result["mean_value"], 4))
                            if result["mean_value"] is not None
                            else "null"
                        ),
                        "scope": col_scope.copy(),
                    },
                ]
            )

            # Special metrics for latitude/longitude
            if rule_type == "latitude":
                invalid_lat = result["below_min"] + result["above_max"]
                pack.metrics.data.append(
                    {
                        "key": "invalid_latitude",
                        "value": invalid_lat,
                        "scope": col_scope.copy(),
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "valid_latitude_percent",
                        "value": str(result["in_range_percent"]),
                        "scope": col_scope.copy(),
                    }
                )
            elif rule_type == "longitude":
                invalid_lon = result["below_min"] + result["above_max"]
                pack.metrics.data.append(
                    {
                        "key": "invalid_longitude",
                        "value": invalid_lon,
                        "scope": col_scope.copy(),
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "valid_longitude_percent",
                        "value": str(result["in_range_percent"]),
                        "scope": col_scope.copy(),
                    }
                )

            # Track for overall score
            total_checks += 1
            total_valid_percent += result["in_range_percent"]

            # Add recommendation if values out of range
            out_of_range = result["below_min"] + result["above_max"]
            if out_of_range > 0:
                out_of_range_percent = 1 - result["in_range_percent"]
                range_desc = ""
                if min_value is not None and max_value is not None:
                    range_desc = f"[{min_value}, {max_value}]"
                elif min_value is not None:
                    range_desc = f">= {min_value}"
                elif max_value is not None:
                    range_desc = f"<= {max_value}"

                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {out_of_range} values ({out_of_range_percent*100:.2f}%) outside the expected range {range_desc}.",
                        "type": "Numeric Range Violation",
                        "scope": col_scope.copy(),
                        "level": determine_recommendation_level(
                            out_of_range_percent
                        ),
                    }
                )

            print(
                f"  [{column}] in_range: {result['in_range_percent']*100:.2f}%, below_min: {result['below_min']}, above_max: {result['above_max']}"
            )

        # Check for negative values in numeric columns if configured
        check_negative = pack.pack_config.get("job", {}).get(
            "check_negative_values", False
        )
        if check_negative:
            for col in df_curr.select_dtypes(include=[np.number]).columns:
                col_data = df_curr[col].dropna()
                if len(col_data) == 0:
                    continue

                col_scope = {
                    "perimeter": "column",
                    "value": col,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_label,
                    },
                }

                negative_count = int((col_data < 0).sum())
                negative_percent = (
                    negative_count / len(col_data) if len(col_data) > 0 else 0
                )

                pack.metrics.data.extend(
                    [
                        {
                            "key": "negative_values",
                            "value": negative_count,
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "negative_values_percent",
                            "value": str(round(negative_percent, 4)),
                            "scope": col_scope.copy(),
                        },
                    ]
                )

                if negative_count > 0:
                    pack.recommendations.data.append(
                        {
                            "content": f"Column '{col}' has {negative_count} negative values ({negative_percent*100:.2f}%).",
                            "type": "Negative Values Found",
                            "scope": col_scope.copy(),
                            "level": "info",
                        }
                    )

    #########################################################
    ## Figures
    dataset_scope = {
        "perimeter": "dataset",
        "value": pack.source_config["name"],
    }

    if check_outcomes:
        # Verdicts : une composition dont les modalités sont pass/fail. Le moteur
        # leur donne les couleurs de statut, toujours avec icône et libellé. Un
        # contrôle par colonne (voir la collecte plus haut), donc pas de double
        # comptage même si la source est chunkée ou si plusieurs règles ciblent la
        # même colonne.
        outcome = {"pass": 0, "fail": 0}
        for entry in check_outcomes.values():
            outcome["fail" if entry["n_violations"] > 0 else "pass"] += 1
        pack.figures.add(
            "checks_outcome",
            intent="composition",
            frame=[{"status": k, "n_checks": v} for k, v in outcome.items()],
            dims=["status"],
            measures=["n_checks"],
            scope=dataset_scope,
            title="Résultat des contrôles",
        )

        # Ventilation des valeurs hors bornes : ce qui explique un score dégradé.
        violations = [
            e for e in check_outcomes.values() if e["n_violations"] > 0
        ]
        if violations:
            pack.figures.add(
                "violations_by_column",
                intent="breakdown",
                of="score",
                frame=violations,
                dims=["column"],
                measures=["n_violations"],
                scope=dataset_scope,
                title="Valeurs hors bornes par colonne",
            )

    # Compute overall score
    if total_checks > 0:
        score = total_valid_percent / total_checks
    else:
        score = 1.0

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
    pack.recommendations.save()
    pack.figures.save()
