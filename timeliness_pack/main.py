"""
Timeliness Pack

Assesses the freshness of date-like columns and computes per-column and
per-dataset timeliness metrics.

Two things used to make this pack unbounded:

- date sniffing called ``df[column].dropna().unique()`` on every column, i.e. an
  exact distinct set over the whole dataset, only to look at ten values;
- parsing went through ``pd.to_datetime(format="mixed")`` / ``dateutil``, which
  needs every value in memory as a pandas Series.

Sniffing is now a single bounded pass that brings back at most
``SNIFF_VALUES`` values per column, and parsing is a ``pl.coalesce`` over the
formats those values actually matched. Polars has no per-value "mixed" mode, so
the supported formats are the explicit list below — see the README.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.aggregation import TimelinessAggregator
from qalita_core.pack import Pack

# Number of values per column used to sniff the date format. Bounded by
# construction: a high-cardinality column costs the same as a constant one.
SNIFF_VALUES = 50

# Supported textual date formats, in the order they are tried.
#
# Month-first is tried before day-first for the ambiguous patterns because that
# is what the previous pandas/dateutil path defaulted to. `pl.coalesce` falls
# through per value, so "13/01/2024" still parses as 13 January: only genuinely
# ambiguous values such as "01/02/2024" are resolved month-first.
DATE_PATTERNS: List[Tuple[Any, List[str]]] = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), ["%Y-%m-%d"]),
    (re.compile(r"^\d{4}/\d{2}/\d{2}$"), ["%Y/%m/%d"]),
    (re.compile(r"^\d{4}\.\d{2}\.\d{2}$"), ["%Y.%m.%d"]),
    (re.compile(r"^\d{2}-\d{2}-\d{4}$"), ["%m-%d-%Y", "%d-%m-%Y"]),
    (re.compile(r"^\d{2}/\d{2}/\d{4}$"), ["%m/%d/%Y", "%d/%m/%Y"]),
    (re.compile(r"^\d{2}\.\d{2}\.\d{4}$"), ["%m.%d.%Y", "%d.%m.%Y"]),
]

DATETIME_PATTERNS: List[Tuple[Any, List[str]]] = [
    (
        re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$"),
        ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"],
    ),
]

# Priority order used to build the coalesce chain, so the parsed result does not
# depend on the order the sample happened to be read in.
FORMAT_PRIORITY = [
    fmt for _, formats in DATE_PATTERNS + DATETIME_PATTERNS for fmt in formats
]

YEAR_PATTERN = re.compile(r"^\d{4}$")

MAX_EXAMPLE_ROWS = 1000
DEFAULT_EXAMPLE_ROWS = 10
DEFAULT_MAX_EXAMPLE_COLUMNS = 20


def is_temporal(dtype: Any) -> bool:
    """True for a column the Parquet footers already type as a date/datetime."""
    return dtype == pl.Date or dtype.base_type() is pl.Datetime


def is_sniffable(dtype: Any) -> bool:
    """True for a column whose values can be rendered as text for sniffing.

    Nested and binary columns are excluded: casting them to Utf8 is either an
    error or a meaningless repr, and neither can be a date.
    """
    return dtype.is_numeric() or dtype in (
        pl.String,
        pl.Categorical,
        pl.Enum,
    )


def matching_formats(value: str) -> List[str]:
    """Formats from the supported list that actually parse ``value``."""
    found: List[str] = []
    for pattern, formats in DATE_PATTERNS + DATETIME_PATTERNS:
        if not pattern.match(value):
            continue
        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
            except ValueError:
                continue
            found.append(fmt)
    return found


def is_date(string: Any) -> Any:
    """Classify a sampled value: ``"year_only"``, ``True`` or ``False``."""
    string = str(string)

    if YEAR_PATTERN.match(string):
        year = int(string)
        if 1900 <= year <= datetime.now().year:
            return "year_only"
        return False

    return bool(matching_formats(string))


def calculate_timeliness_score(days_since: float) -> float:
    # Score is 1.0 if days_since is 0, and decreases linearly to 0.0 at 365.
    return max(0.0, 1 - (days_since / 365))


def dataset_labels(pack: Pack, trigger: str = "source") -> Dict[str, str]:
    """Dataset scope name to publish for each logical object.

    A chunked single-object source stays ONE dataset. The previous code labelled
    each parquet part ``<source>_<n>`` and then had to detect after the fact
    that those "datasets" were really chunks of one.
    """
    tables = pack.tables(trigger)
    if len(tables) == 1:
        return {tables[0]: pack.source_config.get("name") or tables[0]}
    return {name: name for name in tables}


def sniff_samples(
    lf: "pl.LazyFrame", columns: List[str], n: int = SNIFF_VALUES
) -> Dict[str, List[str]]:
    """At most ``n`` non-null values per column, in ONE streaming pass.

    ``implode`` collapses each column to a single list-valued cell, which is what
    lets every column travel in the same aggregate instead of one query each.
    """
    if not columns:
        return {}
    key = {column: str(i) for i, column in enumerate(columns)}
    raw = analytics.agg(
        lf,
        {
            key[column]: pl.col(column)
            .drop_nulls()
            .cast(pl.Utf8, strict=False)
            .head(n)
            .implode()
            for column in columns
        },
    )
    return {
        column: [v for v in (raw.get(key[column]) or []) if v is not None]
        for column in columns
    }


def classify_columns(
    schema: Dict[str, Any], samples: Dict[str, List[str]]
) -> Tuple[Dict[str, List[str]], List[str]]:
    """Split columns into date columns (with their formats) and year columns.

    Columns already typed as Date/Datetime by the Parquet footers need no
    sniffing at all and carry an empty format list.
    """
    date_columns: Dict[str, List[str]] = {}
    year_columns: List[str] = []

    for column, dtype in schema.items():
        if is_temporal(dtype):
            date_columns[column] = []
            continue

        values = samples.get(column) or []
        if not values:
            continue

        verdicts = {is_date(value) for value in values}
        if "year_only" in verdicts:
            year_columns.append(column)
        elif True in verdicts:
            formats: List[str] = []
            for value in values:
                formats.extend(matching_formats(value))
            ordered = [f for f in FORMAT_PRIORITY if f in set(formats)]
            if ordered:
                date_columns[column] = ordered

    return date_columns, year_columns


def date_expression(column: str, dtype: Any, formats: List[str]) -> "pl.Expr":
    """A Date-typed expression for one column, whatever it is stored as."""
    if dtype == pl.Date:
        return pl.col(column)
    if is_temporal(dtype):
        return pl.col(column).dt.date()

    text = pl.col(column).cast(pl.Utf8, strict=False)
    parsed = []
    for fmt in formats:
        if "%H" in fmt:
            parsed.append(text.str.to_datetime(fmt, strict=False).dt.date())
        else:
            parsed.append(text.str.to_date(fmt, strict=False))
    return pl.coalesce(parsed)


def example_limit(pack_config: Dict[str, Any]) -> int:
    job = (pack_config or {}).get("job", {}) or {}
    try:
        limit = int(job.get("examples_limit", DEFAULT_EXAMPLE_ROWS))
    except (TypeError, ValueError):
        limit = DEFAULT_EXAMPLE_ROWS
    return max(0, min(limit, MAX_EXAMPLE_ROWS))


def max_example_columns(pack_config: Dict[str, Any]) -> int:
    job = (pack_config or {}).get("job", {}) or {}
    try:
        return max(
            0,
            int(job.get("max_example_columns", DEFAULT_MAX_EXAMPLE_COLUMNS)),
        )
    except (TypeError, ValueError):
        return DEFAULT_MAX_EXAMPLE_COLUMNS


def staleness_days(source_config: Dict[str, Any]) -> Any:
    """Days since the source files were last modified, or None."""
    config = source_config.get("config", {}) or {}
    path = config.get("path")
    if not path or not os.path.exists(path):
        return None

    if source_config.get("type") == "file":
        latest = os.path.getmtime(path)
    else:
        latest = 0.0
        for root, _dirs, files in os.walk(path):
            for name in files:
                latest = max(
                    latest, os.path.getmtime(os.path.join(root, name))
                )
        if latest <= 0:
            return None

    return (datetime.now() - datetime.fromtimestamp(latest)).days


def main() -> None:
    with Pack() as pack:
        if pack.source_config.get("type") == "database":
            table_or_query = pack.source_config.get("config", {}).get(
                "table_or_query"
            )
            if not table_or_query:
                raise ValueError(
                    "For a 'database' type source, you must specify "
                    "'table_or_query' in the config."
                )
            pack.load_data("source", table_or_query=table_or_query)
        else:
            pack.load_data("source")

        compute_score_columns = pack.pack_config.get("job", {}).get(
            "compute_score_columns"
        )
        limit = example_limit(pack.pack_config)
        date_columns_count = 0

        for table, dataset_label in dataset_labels(pack, "source").items():
            lf = pack.scan("source", table)
            schema = pack.schema("source", table)

            candidates = [
                column
                for column, dtype in schema.items()
                if not is_temporal(dtype) and is_sniffable(dtype)
            ]
            samples = sniff_samples(lf, candidates)
            date_columns, year_columns = classify_columns(schema, samples)
            if not date_columns and not year_columns:
                continue

            key = {
                column: str(i)
                for i, column in enumerate(
                    list(date_columns) + list(year_columns)
                )
            }
            exprs: Dict[str, pl.Expr] = {}
            for column, formats in date_columns.items():
                parsed = date_expression(column, schema[column], formats)
                exprs[f"{key[column]}|min"] = parsed.min()
                exprs[f"{key[column]}|max"] = parsed.max()
                exprs[f"{key[column]}|unparsed"] = (
                    pl.col(column).is_not_null() & parsed.is_null()
                ).sum()
            for column in year_columns:
                years = pl.col(column).cast(pl.Int64, strict=False)
                exprs[f"{key[column]}|min"] = years.min()
                exprs[f"{key[column]}|max"] = years.max()

            # ONE pass for the bounds of every date and year column.
            bounds = analytics.agg(lf, exprs)

            aggregator = TimelinessAggregator()
            for column in year_columns:
                earliest = bounds.get(f"{key[column]}|min")
                latest = bounds.get(f"{key[column]}|max")
                if earliest is None or latest is None:
                    continue
                aggregator.add_year_obs(column, int(earliest), int(latest))

            examples_left = max_example_columns(pack.pack_config)
            for column, formats in date_columns.items():
                earliest = bounds.get(f"{key[column]}|min")
                latest = bounds.get(f"{key[column]}|max")
                if earliest is None or latest is None:
                    continue
                date_columns_count += 1
                aggregator.add_date_obs(column, earliest, latest)

                unparsed = int(bounds.get(f"{key[column]}|unparsed") or 0)
                if not unparsed:
                    continue
                pack.recommendations.data.append(
                    {
                        "content": (
                            f"Column '{column}' has {unparsed} values that "
                            f"none of the supported date formats "
                            f"({', '.join(formats) or 'native'}) could parse."
                        ),
                        "type": "Unparsable Date Values",
                        "scope": {
                            "perimeter": "column",
                            "value": column,
                            "parent_scope": {
                                "perimeter": "dataset",
                                "value": dataset_label,
                            },
                        },
                        "level": "warning",
                    }
                )
                if not limit or not examples_left:
                    continue
                examples_left -= 1
                parsed = date_expression(column, schema[column], formats)
                _, rows = analytics.failures(
                    lf,
                    pl.col(column).is_not_null() & parsed.is_null(),
                    limit=limit,
                    columns=[column],
                )
                pack.metrics.data.append(
                    {
                        "key": "unparsed_date_examples",
                        "value": rows.to_dicts(),
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

            metrics, recommendations = aggregator.finalize_metrics(
                dataset_scope_name=dataset_label,
                compute_score_columns=compute_score_columns,
                calc_timeliness_score=calculate_timeliness_score,
            )
            pack.metrics.data.extend(metrics)
            pack.recommendations.data.extend(recommendations)

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

        if pack.source_config.get("type") in ("file", "folder"):
            try:
                days = staleness_days(pack.source_config)
            except OSError as exc:
                days = None
                print(f"Could not compute data staleness: {exc}")
            if days is not None:
                pack.metrics.data.append(
                    {
                        "key": "data_staleness_days",
                        "value": str(days),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )
                print(
                    f"Data staleness: {days} days since last file modification"
                )

        pack.metrics.save()
        pack.recommendations.save()


if __name__ == "__main__":
    main()
