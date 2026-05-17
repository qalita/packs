"""
Pattern Validation Pack

Validates data formats using predefined and custom regex patterns.
Covers  checks:
- invalid_email_format_found, invalid_email_format_percent
- invalid_uuid_format_found, invalid_uuid_format_percent
- invalid_ip4_address_format_found, invalid_ip6_address_format_found
- text_not_matching_regex_found, texts_not_matching_regex_percent
- text_not_matching_date_pattern_found
"""

import re
import pandas as pd
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

# Predefined patterns for common data formats
BUILTIN_PATTERNS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "ipv4": r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    "ipv6": r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}$",
    "url": r"^https?://[^\s/$.?#].[^\s]*$",
    "phone_international": r"^\+?[1-9]\d{1,14}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "date_us": r"^\d{2}/\d{2}/\d{4}$",
    "date_eu": r"^\d{2}-\d{2}-\d{4}$",
    "datetime_iso": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
    "credit_card": r"^(?:\d[ -]*?){13,16}$",
    "hex_color": r"^#(?:[0-9a-fA-F]{3}){1,2}$",
    "mac_address": r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",
    "postal_code_us": r"^\d{5}(?:-\d{4})?$",
    "alphanumeric": r"^[A-Za-z0-9]+$",
}


def validate_pattern(series: pd.Series, pattern: str) -> tuple:
    """
    Validate a series against a regex pattern.

    Returns:
        tuple: (invalid_count, invalid_percent, valid_percent)
    """
    compiled = re.compile(pattern)

    # Convert to string and check matches
    str_series = series.astype(str).fillna("")
    matches = str_series.apply(
        lambda x: bool(compiled.match(x)) if x else True
    )

    total = len(series)
    invalid_count = (~matches).sum()
    invalid_percent = invalid_count / total if total > 0 else 0
    valid_percent = 1 - invalid_percent

    return (
        int(invalid_count),
        round(invalid_percent, 4),
        round(valid_percent, 4),
    )


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
    validation_rules = pack.pack_config.get("job", {}).get("patterns", [])

    # Track overall validity score
    total_checks = 0
    total_valid_percent = 0

    for dataset_label, df_curr in items:
        print(f"Validating patterns for {dataset_label}")

        for rule in validation_rules:
            column = rule.get("column")
            pattern_type = rule.get("type")
            custom_regex = rule.get("regex")

            if column not in df_curr.columns:
                print(f"Column '{column}' not found in dataset. Skipping.")
                continue

            # Get the pattern to use
            if pattern_type == "regex" and custom_regex:
                pattern = custom_regex
                pattern_name = "custom_regex"
            elif pattern_type in BUILTIN_PATTERNS:
                pattern = BUILTIN_PATTERNS[pattern_type]
                pattern_name = pattern_type
            else:
                print(
                    f"Unknown pattern type '{pattern_type}' for column '{column}'. Skipping."
                )
                continue

            # Validate the column
            col_data = df_curr[column].dropna()
            if len(col_data) == 0:
                print(f"Column '{column}' is empty. Skipping.")
                continue

            invalid_count, invalid_percent, valid_percent = validate_pattern(
                col_data, pattern
            )

            col_scope = {
                "perimeter": "column",
                "value": column,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": dataset_label,
                },
            }

            # Add metrics based on pattern type ( naming convention)
            if pattern_name == "email":
                pack.metrics.data.extend(
                    [
                        {
                            "key": "invalid_email_format_found",
                            "value": invalid_count,
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "invalid_email_format_percent",
                            "value": str(invalid_percent),
                            "scope": col_scope.copy(),
                        },
                    ]
                )
            elif pattern_name == "uuid":
                pack.metrics.data.extend(
                    [
                        {
                            "key": "invalid_uuid_format_found",
                            "value": invalid_count,
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "invalid_uuid_format_percent",
                            "value": str(invalid_percent),
                            "scope": col_scope.copy(),
                        },
                    ]
                )
            elif pattern_name == "ipv4":
                pack.metrics.data.append(
                    {
                        "key": "invalid_ip4_address_format_found",
                        "value": invalid_count,
                        "scope": col_scope.copy(),
                    }
                )
            elif pattern_name == "ipv6":
                pack.metrics.data.append(
                    {
                        "key": "invalid_ip6_address_format_found",
                        "value": invalid_count,
                        "scope": col_scope.copy(),
                    }
                )
            else:
                # Generic pattern validation (: text_not_matching_regex)
                pack.metrics.data.extend(
                    [
                        {
                            "key": "text_not_matching_regex_found",
                            "value": invalid_count,
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "texts_not_matching_regex_percent",
                            "value": str(invalid_percent),
                            "scope": col_scope.copy(),
                        },
                    ]
                )

            # Always add valid_percent metric
            pack.metrics.data.append(
                {
                    "key": f"valid_{pattern_name}_percent",
                    "value": str(valid_percent),
                    "scope": col_scope.copy(),
                }
            )

            # Track for overall score
            total_checks += 1
            total_valid_percent += valid_percent

            # Add recommendation if invalid count > 0
            if invalid_count > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {invalid_count} values ({invalid_percent*100:.2f}%) that don't match the {pattern_name} pattern.",
                        "type": f"Invalid {pattern_name.replace('_', ' ').title()} Format",
                        "scope": col_scope.copy(),
                        "level": determine_recommendation_level(
                            invalid_percent
                        ),
                    }
                )

            print(
                f"  [{column}] {pattern_name}: {invalid_count} invalid ({invalid_percent*100:.2f}%), {valid_percent*100:.2f}% valid"
            )

        # Auto-detect common patterns if no explicit rules configured
        if not validation_rules:
            print("No explicit patterns configured. Running auto-detection...")

            for col in df_curr.columns:
                col_lower = col.lower()
                col_scope = {
                    "perimeter": "column",
                    "value": col,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_label,
                    },
                }

                col_data = df_curr[col].dropna()
                if len(col_data) == 0:
                    continue

                # Auto-detect email columns
                if "email" in col_lower or "mail" in col_lower:
                    invalid_count, invalid_percent, valid_percent = (
                        validate_pattern(col_data, BUILTIN_PATTERNS["email"])
                    )
                    pack.metrics.data.extend(
                        [
                            {
                                "key": "invalid_email_format_found",
                                "value": invalid_count,
                                "scope": col_scope.copy(),
                            },
                            {
                                "key": "invalid_email_format_percent",
                                "value": str(invalid_percent),
                                "scope": col_scope.copy(),
                            },
                        ]
                    )
                    total_checks += 1
                    total_valid_percent += valid_percent
                    if invalid_count > 0:
                        pack.recommendations.data.append(
                            {
                                "content": f"Column '{col}' has {invalid_count} invalid email formats.",
                                "type": "Invalid Email Format",
                                "scope": col_scope.copy(),
                                "level": determine_recommendation_level(
                                    invalid_percent
                                ),
                            }
                        )

                # Auto-detect UUID columns
                if "uuid" in col_lower or "guid" in col_lower:
                    invalid_count, invalid_percent, valid_percent = (
                        validate_pattern(col_data, BUILTIN_PATTERNS["uuid"])
                    )
                    pack.metrics.data.extend(
                        [
                            {
                                "key": "invalid_uuid_format_found",
                                "value": invalid_count,
                                "scope": col_scope.copy(),
                            },
                            {
                                "key": "invalid_uuid_format_percent",
                                "value": str(invalid_percent),
                                "scope": col_scope.copy(),
                            },
                        ]
                    )
                    total_checks += 1
                    total_valid_percent += valid_percent
                    if invalid_count > 0:
                        pack.recommendations.data.append(
                            {
                                "content": f"Column '{col}' has {invalid_count} invalid UUID formats.",
                                "type": "Invalid UUID Format",
                                "scope": col_scope.copy(),
                                "level": determine_recommendation_level(
                                    invalid_percent
                                ),
                            }
                        )

                # Auto-detect IP address columns
                if (
                    "ip" in col_lower
                    and "address" in col_lower
                    or col_lower in ["ip", "ip_address", "ipaddress"]
                ):
                    invalid_count, invalid_percent, valid_percent = (
                        validate_pattern(col_data, BUILTIN_PATTERNS["ipv4"])
                    )
                    pack.metrics.data.append(
                        {
                            "key": "invalid_ip4_address_format_found",
                            "value": invalid_count,
                            "scope": col_scope.copy(),
                        }
                    )
                    total_checks += 1
                    total_valid_percent += valid_percent
                    if invalid_count > 0:
                        pack.recommendations.data.append(
                            {
                                "content": f"Column '{col}' has {invalid_count} invalid IPv4 address formats.",
                                "type": "Invalid IP Address Format",
                                "scope": col_scope.copy(),
                                "level": determine_recommendation_level(
                                    invalid_percent
                                ),
                            }
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
