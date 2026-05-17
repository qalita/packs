"""
Text Validation Pack

Validates text data for length constraints, word counts, and whitespace issues.
Covers ks:
- text_min_length, text_max_length, text_mean_length
- text_length_below_min_length, text_length_above_max_length
- text_length_in_range_percent
- min_word_count, max_word_count
- empty_text_found, whitespace_text_found, null_placeholder_text_found
- text_surrounded_by_whitespace_found
"""

import pandas as pd
import numpy as np
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

# Common null placeholder patterns
NULL_PLACEHOLDERS = [
    "null",
    "NULL",
    "Null",
    "none",
    "NONE",
    "None",
    "n/a",
    "N/A",
    "NA",
    "na",
    "nan",
    "NaN",
    "NAN",
    "-",
    "--",
    "---",
    ".",
    "..",
    "undefined",
    "UNDEFINED",
    "missing",
    "MISSING",
    "unknown",
    "UNKNOWN",
    "#N/A",
    "#NA",
    "#NULL!",
    "(blank)",
    "(empty)",
    "<null>",
    "<NULL>",
]


def analyze_text_column(
    series: pd.Series, min_length=None, max_length=None
) -> dict:
    """
    Analyze a text column for length and whitespace issues.

    Returns:
        dict with text validation metrics
    """
    # Convert to string and handle NaN
    str_series = series.fillna("").astype(str)
    non_null = series.dropna().astype(str)

    total = len(series)
    non_null_count = len(non_null)

    if non_null_count == 0:
        return {
            "min_length": 0,
            "max_length": 0,
            "mean_length": 0,
            "below_min_length": 0,
            "above_max_length": 0,
            "in_range_percent": 1.0,
            "empty_text_count": 0,
            "whitespace_only_count": 0,
            "null_placeholder_count": 0,
            "surrounded_by_whitespace_count": 0,
            "min_word_count": 0,
            "max_word_count": 0,
        }

    # Calculate lengths
    lengths = non_null.str.len()

    # Length stats
    actual_min_length = int(lengths.min())
    actual_max_length = int(lengths.max())
    mean_length = float(lengths.mean())

    # Check against constraints
    below_min = 0
    above_max = 0
    if min_length is not None:
        below_min = int((lengths < min_length).sum())
    if max_length is not None:
        above_max = int((lengths > max_length).sum())

    in_range_count = non_null_count - below_min - above_max
    in_range_percent = (
        in_range_count / non_null_count if non_null_count > 0 else 1.0
    )

    # Empty and whitespace checks
    empty_text_count = int((non_null == "").sum())
    whitespace_only_count = (
        int(non_null.str.strip().eq("").sum()) - empty_text_count
    )

    # Null placeholder check
    null_placeholder_count = int(
        non_null.str.lower().isin([p.lower() for p in NULL_PLACEHOLDERS]).sum()
    )

    # Surrounded by whitespace check
    stripped = non_null.str.strip()
    surrounded_by_whitespace_count = int((non_null != stripped).sum())

    # Word count
    word_counts = non_null.str.split().str.len().fillna(0)
    min_word_count = int(word_counts.min())
    max_word_count = int(word_counts.max())

    return {
        "min_length": actual_min_length,
        "max_length": actual_max_length,
        "mean_length": round(mean_length, 2),
        "below_min_length": below_min,
        "above_max_length": above_max,
        "in_range_percent": round(in_range_percent, 4),
        "empty_text_count": empty_text_count,
        "whitespace_only_count": whitespace_only_count,
        "null_placeholder_count": null_placeholder_count,
        "surrounded_by_whitespace_count": surrounded_by_whitespace_count,
        "min_word_count": min_word_count,
        "max_word_count": max_word_count,
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
    analyze_all_text = pack.pack_config.get("job", {}).get(
        "analyze_all_text_columns", True
    )

    # Track overall validity score
    total_checks = 0
    total_valid_percent = 0
    total_issues = 0

    for dataset_label, df_curr in items:
        print(f"Validating text columns for {dataset_label}")

        # Build column rules map
        col_rules = {r.get("column"): r for r in validation_rules}

        # Determine which columns to analyze
        text_columns = df_curr.select_dtypes(
            include=["object", "string"]
        ).columns.tolist()

        if not analyze_all_text:
            text_columns = [c for c in text_columns if c in col_rules]

        for column in text_columns:
            col_scope = {
                "perimeter": "column",
                "value": column,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": dataset_label,
                },
            }

            col_data = df_curr[column]
            if col_data.dropna().empty:
                print(f"Column '{column}' is empty. Skipping.")
                continue

            # Get constraints from rules if defined
            rule = col_rules.get(column, {})
            min_length = rule.get("min_length")
            max_length = rule.get("max_length")

            # Analyze the column
            result = analyze_text_column(col_data, min_length, max_length)

            # Add metrics ( naming convention)
            pack.metrics.data.extend(
                [
                    {
                        "key": "text_min_length",
                        "value": result["min_length"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "text_max_length",
                        "value": result["max_length"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "text_mean_length",
                        "value": str(result["mean_length"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "min_word_count",
                        "value": result["min_word_count"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "max_word_count",
                        "value": result["max_word_count"],
                        "scope": col_scope.copy(),
                    },
                ]
            )

            # Length constraint metrics
            if min_length is not None:
                pack.metrics.data.append(
                    {
                        "key": "text_length_below_min_length",
                        "value": result["below_min_length"],
                        "scope": col_scope.copy(),
                    }
                )
            if max_length is not None:
                pack.metrics.data.append(
                    {
                        "key": "text_length_above_max_length",
                        "value": result["above_max_length"],
                        "scope": col_scope.copy(),
                    }
                )
            if min_length is not None or max_length is not None:
                pack.metrics.data.append(
                    {
                        "key": "text_length_in_range_percent",
                        "value": str(result["in_range_percent"]),
                        "scope": col_scope.copy(),
                    }
                )

            # Whitespace/empty metrics
            pack.metrics.data.extend(
                [
                    {
                        "key": "empty_text_found",
                        "value": result["empty_text_count"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "whitespace_text_found",
                        "value": result["whitespace_only_count"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "null_placeholder_text_found",
                        "value": result["null_placeholder_count"],
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "text_surrounded_by_whitespace_found",
                        "value": result["surrounded_by_whitespace_count"],
                        "scope": col_scope.copy(),
                    },
                ]
            )

            # Track for overall score
            total_non_null = len(col_data.dropna())
            if total_non_null > 0:
                issues = (
                    result["empty_text_count"]
                    + result["whitespace_only_count"]
                    + result["null_placeholder_count"]
                )
                issue_percent = issues / total_non_null
                valid_percent = 1 - issue_percent

                total_checks += 1
                total_valid_percent += valid_percent
                total_issues += issues

            # Add recommendations
            if result["empty_text_count"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['empty_text_count']} empty text values.",
                        "type": "Empty Text Found",
                        "scope": col_scope.copy(),
                        "level": "info",
                    }
                )

            if result["whitespace_only_count"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['whitespace_only_count']} whitespace-only values.",
                        "type": "Whitespace Only Text",
                        "scope": col_scope.copy(),
                        "level": "warning",
                    }
                )

            if result["null_placeholder_count"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['null_placeholder_count']} null placeholder values (N/A, None, etc.).",
                        "type": "Null Placeholder Found",
                        "scope": col_scope.copy(),
                        "level": "warning",
                    }
                )

            if result["surrounded_by_whitespace_count"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['surrounded_by_whitespace_count']} values with leading/trailing whitespace.",
                        "type": "Text Surrounded By Whitespace",
                        "scope": col_scope.copy(),
                        "level": "info",
                    }
                )

            if result["below_min_length"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['below_min_length']} values shorter than minimum length {min_length}.",
                        "type": "Text Too Short",
                        "scope": col_scope.copy(),
                        "level": "warning",
                    }
                )

            if result["above_max_length"] > 0:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {result['above_max_length']} values longer than maximum length {max_length}.",
                        "type": "Text Too Long",
                        "scope": col_scope.copy(),
                        "level": "warning",
                    }
                )

            print(
                f"  [{column}] len: {result['min_length']}-{result['max_length']} (avg {result['mean_length']}), empty: {result['empty_text_count']}, whitespace: {result['whitespace_only_count']}, placeholders: {result['null_placeholder_count']}"
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

    pack.metrics.data.append(
        {
            "key": "total_text_issues",
            "value": total_issues,
            "scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        }
    )

    pack.metrics.save()
    pack.recommendations.save()
