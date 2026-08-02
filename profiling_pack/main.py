"""
Profiling Pack — streaming.

Statistics are computed over the WHOLE dataset. The previous implementation
loaded the source into pandas and, above one million rows, profiled
``head(500_000)`` while reporting the result as a profile of the dataset. On a
chunked source ``head()`` reads only the first part files, so every
distribution described the first partition instead of the data.

Everything here goes through ``pack.scan()`` -> ``qalita_core.profiling`` /
``qalita_core.analytics``, which read the source in a bounded number of
streaming passes:

- one pass for every scalar statistic of every column (``profile``);
- one pass for the statistics ``profile`` does not cover (skewness, kurtosis,
  monotonicity, infinities, byte sizes);
- two passes for approximate quantiles, plus two more for string lengths;
- one bounded pass per column for top values.

Distinct counts (HyperLogLog) and quantiles (histogram) are approximate by
default; set ``exact: true`` in pack_conf.json to compute them exactly. Every
metric derived from an approximate statistic ships a ``<key>_method`` sibling
naming the method, so the UI can label it instead of guessing.
"""

import json
import logging
import os
from datetime import datetime

import polars as pl

from qalita_core.pack import Pack, _sanitize_for_json
from qalita_core import analytics, profiling
from qalita_core.utils import determine_level, round_if_numeric

logger = logging.getLogger(__name__)

# Probabilities requested from the profiler. They cover both the ydata-style
# percentile keys ("5%" .. "95%") and the percentile_* keys the pack has always
# emitted, so a single quantile computation feeds both families.
PROFILE_QUANTILES = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# quantile probability -> metric key, for the two families.
PERCENT_KEYS = {0.05: "5%", 0.25: "25%", 0.50: "50%", 0.75: "75%", 0.95: "95%"}
PERCENTILE_KEYS = {
    0.10: "percentile_10",
    0.25: "percentile_25",
    0.75: "percentile_75",
    0.90: "percentile_90",
}

DEFAULT_TOP_K = 10

# A text column with more distinct values than this is flagged, which is the
# threshold ydata-profiling used for its "High cardinality" alert.
DEFAULT_HIGH_CARDINALITY = 50

# |skewness| above this is flagged, as ydata-profiling did.
SKEWNESS_THRESHOLD = 20.0

# Share of zeros above which a numeric column is flagged.
ZEROS_THRESHOLD = 0.5

# Distinct-to-non-null ratio above which a column is called unique when the
# distinct count is approximate. Polars' HyperLogLog was measured landing
# within ~14% of the truth in both directions, so an equality test would report
# a genuine primary key as non-unique. `is_unique_method` says which test ran.
APPROX_UNIQUE_RATIO = 0.9

# Estimated in-memory width of the fixed-size dtypes. Used for `memory_size`,
# which describes the uncompressed footprint of the data, not the parquet size.
_DTYPE_BYTES = {
    pl.Boolean: 1,
    pl.Int8: 1,
    pl.UInt8: 1,
    pl.Int16: 2,
    pl.UInt16: 2,
    pl.Int32: 4,
    pl.UInt32: 4,
    pl.Float32: 4,
    pl.Date: 4,
    pl.Time: 8,
    pl.Int64: 8,
    pl.UInt64: 8,
    pl.Float64: 8,
}


def read_options(pack_config):
    """Profiling options, read from pack_conf.json.

    ``exact`` is the product-level switch between approximate statistics
    (HyperLogLog + histogram, memory independent of cardinality) and exact ones
    (O(cardinality) memory — on a primary key that is one entry per row).
    """
    config = pack_config or {}
    job = config.get("job") or {}

    def _get(name, default):
        value = config.get(name)
        if value is None:
            value = job.get(name)
        return default if value is None else value

    return {
        "exact": bool(_get("exact", False)),
        "top_k": int(_get("top_k", DEFAULT_TOP_K)),
        "high_cardinality": int(
            _get("high_cardinality_threshold", DEFAULT_HIGH_CARDINALITY)
        ),
    }


def column_kind(dtype):
    """ydata-style family of a dtype, the value of the `type` metric."""
    if dtype == pl.Boolean:
        return "Boolean"
    if dtype == pl.String:
        return "Text"
    if dtype in (pl.Categorical, pl.Enum):
        return "Categorical"
    if dtype.is_numeric():
        return "Numeric"
    if dtype.is_temporal():
        return "DateTime"
    return "Unsupported"


def extra_stats(lf, schema, *, exact=False):
    """The statistics `profile()` does not compute, in one streaming pass.

    Batched into a single :func:`analytics.agg` call: one expression per column
    per statistic, one read of the source. Issuing them per column would
    re-read the whole dataset once per column.
    """
    numeric = analytics.numeric_columns(schema)
    strings = analytics.string_columns(schema)

    exprs = {}
    for name in numeric:
        col = pl.col(name)
        # bias=False matches pandas' .skew()/.kurt(), which is what the
        # previous ydata-profiling based output reported.
        exprs[f"skew|{name}"] = col.skew(bias=False)
        exprs[f"kurt|{name}"] = col.kurtosis(bias=False)
        # diff() keeps its ordering across part files, so monotonicity still
        # describes the dataset as loaded rather than one partition.
        exprs[f"inc|{name}"] = (col.diff() >= 0).all()
        exprs[f"dec|{name}"] = (col.diff() <= 0).all()
        exprs[f"incs|{name}"] = (col.diff() > 0).all()
        exprs[f"decs|{name}"] = (col.diff() < 0).all()
        if schema[name].is_float():
            exprs[f"inf|{name}"] = col.is_infinite().sum()
            exprs[f"nan|{name}"] = col.is_nan().sum()

    for name in strings:
        col = pl.col(name)
        exprs[f"chars|{name}"] = col.str.len_chars().sum()
        exprs[f"bytes|{name}"] = col.str.len_bytes().sum()

    stats = analytics.agg(lf, exprs) if exprs else {}

    # Median string length needs an ordering statistic, so it cannot ride the
    # scalar pass. The projection means only the string columns are read.
    if strings:
        lengths = lf.select(
            [pl.col(name).str.len_chars().alias(name) for name in strings]
        )
        medians = analytics.quantiles(lengths, strings, [0.5], exact=exact)
        for name, values in medians.items():
            stats[f"medlen|{name}"] = values.get(0.5)

    return stats


def profile_dataset(lf, schema, options):
    """Profile one object: every column, whole dataset, bounded memory."""
    prof = profiling.profile(
        lf,
        schema=schema,
        exact=options["exact"],
        top_k=options["top_k"],
        quantiles=PROFILE_QUANTILES,
    )
    extras = extra_stats(lf, schema, exact=options["exact"])
    for name in prof:
        prof[name]["kind"] = column_kind(schema[name])
        for prefix, key in (
            ("skew", "skewness"),
            ("kurt", "kurtosis"),
            ("inc", "monotonic_increase"),
            ("dec", "monotonic_decrease"),
            ("incs", "monotonic_increase_strict"),
            ("decs", "monotonic_decrease_strict"),
            ("inf", "n_infinite"),
            ("nan", "n_nan"),
            ("chars", "n_characters"),
            ("bytes", "n_bytes"),
            ("medlen", "median_length"),
        ):
            alias = f"{prefix}|{name}"
            if alias in extras:
                prof[name][key] = extras[alias]
    return prof


def _scope(dataset_name, column=None, database=None):
    if column is not None:
        return {
            "perimeter": "column",
            "value": column,
            "parent_scope": {
                "perimeter": "dataset",
                "value": dataset_name,
            },
        }
    scope = {"perimeter": "dataset", "value": dataset_name}
    if database:
        scope["parent_scope"] = {"perimeter": "database", "value": database}
    return scope


def _metric(key, value, scope, decimals=2):
    """A metric entry, or None when the statistic does not exist.

    A statistic that could not be computed is omitted rather than emitted as
    the string "None": a missing metric is readable, a fake one is not.
    """
    if value is None:
        return None
    return {
        "key": key,
        "value": round_if_numeric(value, decimals),
        "scope": scope,
    }


def _quantile_metrics(column, scope, method):
    """The `5%`..`95%` and `percentile_*` keys, plus their method siblings."""
    quantiles = column.get("quantiles") or {}
    entries = []
    for probability, key in list(PERCENT_KEYS.items()) + list(
        PERCENTILE_KEYS.items()
    ):
        value = quantiles.get(str(probability))
        if value is None:
            continue
        entries.append(_metric(key, value, scope, decimals=4))
        entries.append(_metric(f"{key}_method", method, scope))
    return entries


def column_metrics(prof, dataset_name):
    """Every column-scoped metric, mapped from the profiler output."""
    entries = []
    for name, column in prof.items():
        scope = _scope(dataset_name, column=name)
        rows = column["n"]
        count = column["count"]
        methods = column.get("methods") or {}
        distinct_method = methods.get("n_distinct", "exact")
        quantile_method = methods.get("quantiles")

        completeness = round(count / rows, 2) if rows else 0.0
        # HyperLogLog can overshoot, and a distinct count above the non-null
        # count is not an approximation, it is an impossible number.
        distinct = min(column["n_distinct"], count) if count else 0
        entries.extend(
            [
                _metric("type", column["kind"], scope),
                _metric("dtype", column["type"], scope),
                _metric("n", rows, scope),
                _metric("count", count, scope),
                _metric("n_missing", column["n_missing"], scope),
                _metric("p_missing", column["p_missing"], scope, decimals=6),
                _metric("completeness_score", completeness, scope),
                _metric("n_distinct", distinct, scope),
                _metric("p_distinct", column["p_distinct"], scope, decimals=6),
                _metric("n_distinct_method", distinct_method, scope),
                _metric("p_distinct_method", distinct_method, scope),
                # n_unique/p_unique now mean "number of distinct values", not
                # ydata's "values occurring exactly once": counting those needs
                # one group per distinct value, which is unbounded memory on a
                # high-cardinality column.
                _metric("n_unique", distinct, scope),
                _metric("p_unique", column["p_distinct"], scope, decimals=6),
                _metric("n_unique_method", distinct_method, scope),
                _metric("p_unique_method", distinct_method, scope),
                _metric("is_unique", _is_unique(column), scope),
                _metric("is_unique_method", distinct_method, scope),
            ]
        )

        if column["kind"] == "Numeric":
            entries.extend(_numeric_metrics(column, scope, quantile_method))
        elif column["kind"] == "Text":
            entries.extend(_text_metrics(column, scope, methods))
        elif column["kind"] == "DateTime":
            entries.extend(
                [
                    _metric("min", _isoformat(column.get("min")), scope),
                    _metric("max", _isoformat(column.get("max")), scope),
                ]
            )

    return [entry for entry in entries if entry is not None]


def _is_unique(column):
    """Whether the column looks like a key.

    Exact when the distinct count is exact; a threshold on the distinct ratio
    otherwise, because an approximate count cannot answer an equality.
    """
    count = column["count"]
    if not count:
        return False
    ratio = column["n_distinct"] / count
    if (column.get("methods") or {}).get("n_distinct") == "exact":
        return ratio >= 1.0
    return ratio >= APPROX_UNIQUE_RATIO


def _numeric_metrics(column, scope, quantile_method):
    count = column["count"]
    std = column.get("std")
    mean = column.get("mean")
    minimum = column.get("min")
    maximum = column.get("max")
    n_zeros = column.get("n_zeros") or 0
    n_negative = column.get("n_negative") or 0
    n_infinite = column.get("n_infinite")

    variance = std * std if std is not None else None
    # std comes back with ddof=1; the population figures are rescaled from it
    # rather than paid for with a second pass over the data.
    population_variance = None
    if variance is not None and count > 1:
        population_variance = variance * (count - 1) / count
    population_std = (
        population_variance**0.5 if population_variance is not None else None
    )

    entries = [
        _metric("min", minimum, scope, decimals=4),
        _metric("max", maximum, scope, decimals=4),
        _metric(
            "range",
            (
                (maximum - minimum)
                if minimum is not None and maximum is not None
                else None
            ),
            scope,
            decimals=4,
        ),
        _metric("sum", column.get("sum"), scope, decimals=4),
        _metric("mean", mean, scope, decimals=4),
        _metric("std", std, scope, decimals=4),
        _metric("variance", variance, scope, decimals=4),
        _metric("sample_stddev", std, scope, decimals=4),
        _metric("population_stddev", population_std, scope, decimals=4),
        _metric("sample_variance", variance, scope, decimals=4),
        _metric("population_variance", population_variance, scope, decimals=4),
        _metric(
            "cv",
            (std / mean) if std is not None and mean else None,
            scope,
            decimals=4,
        ),
        _metric("n_zeros", n_zeros, scope),
        _metric(
            "p_zeros", (n_zeros / count) if count else 0.0, scope, decimals=6
        ),
        _metric("n_negative", n_negative, scope),
        _metric(
            "p_negative",
            (n_negative / count) if count else 0.0,
            scope,
            decimals=6,
        ),
        _metric("skewness", column.get("skewness"), scope, decimals=4),
        _metric("kurtosis", column.get("kurtosis"), scope, decimals=4),
        _metric("iqr", column.get("iqr"), scope, decimals=4),
    ]

    if n_infinite is not None:
        entries.extend(
            [
                _metric("n_infinite", n_infinite, scope),
                _metric(
                    "p_infinite",
                    (n_infinite / count) if count else 0.0,
                    scope,
                    decimals=6,
                ),
            ]
        )

    increasing = column.get("monotonic_increase")
    decreasing = column.get("monotonic_decrease")
    if increasing is not None or decreasing is not None:
        entries.extend(
            [
                _metric("monotonic_increase", increasing, scope),
                _metric("monotonic_decrease", decreasing, scope),
                _metric(
                    "monotonic_increase_strict",
                    column.get("monotonic_increase_strict"),
                    scope,
                ),
                _metric(
                    "monotonic_decrease_strict",
                    column.get("monotonic_decrease_strict"),
                    scope,
                ),
                _metric("monotonic", bool(increasing or decreasing), scope),
            ]
        )
        if increasing or decreasing:
            entries.append(_metric("ordering", 1 if increasing else 0, scope))

    if quantile_method:
        entries.extend(_quantile_metrics(column, scope, quantile_method))
    return entries


def _text_metrics(column, scope, methods):
    count = column["count"]
    n_empty = column.get("n_empty") or 0
    entries = [
        _metric("min_length", column.get("min_length"), scope),
        _metric("max_length", column.get("max_length"), scope),
        _metric("mean_length", column.get("mean_length"), scope, decimals=4),
        _metric("n_characters", column.get("n_characters"), scope),
        _metric("n_empty", n_empty, scope),
        _metric(
            "p_empty", (n_empty / count) if count else 0.0, scope, decimals=6
        ),
    ]
    median_length = column.get("median_length")
    if median_length is not None:
        entries.append(
            _metric("median_length", median_length, scope, decimals=4)
        )
        entries.append(
            _metric(
                "median_length_method",
                (
                    "exact"
                    if methods.get("n_distinct") == "exact"
                    else "histogram"
                ),
                scope,
            )
        )
    return entries


def _isoformat(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def dataset_summary(prof, schema):
    """Dataset-level aggregates, derived from the per-column profile.

    Nothing here re-reads the source: the profiler already paid for every
    number below in its single scalar pass.
    """
    n_var = len(prof)
    rows = next(iter(prof.values()))["n"] if prof else 0
    n_cells_missing = sum(column["n_missing"] for column in prof.values())
    total_cells = rows * n_var
    kinds = {}
    for column in prof.values():
        kinds[column["kind"]] = kinds.get(column["kind"], 0) + 1

    memory_size = 0
    for name, column in prof.items():
        if schema[name] == pl.String:
            memory_size += int(column.get("n_bytes") or 0)
        else:
            memory_size += _DTYPE_BYTES.get(schema[name], 8) * rows

    return {
        "n": rows,
        "n_var": n_var,
        "n_cells_missing": n_cells_missing,
        "p_cells_missing": (
            n_cells_missing / total_cells if total_cells else 0.0
        ),
        "n_vars_with_missing": sum(
            1 for column in prof.values() if column["n_missing"] > 0
        ),
        "n_vars_all_missing": sum(
            1 for column in prof.values() if column["count"] == 0
        ),
        "memory_size": memory_size,
        "record_size": int(memory_size / rows) if rows else 0,
        "types_numeric": kinds.get("Numeric", 0),
        "types_text": kinds.get("Text", 0),
        "types_datetime": kinds.get("DateTime", 0),
        "types_boolean": kinds.get("Boolean", 0),
        "types_categorical": kinds.get("Categorical", 0),
        "types_unsupported": kinds.get("Unsupported", 0),
    }


def dataset_metrics(summary, dataset_name, database=None):
    """Dataset-scoped metrics, including the completeness score."""
    scope = _scope(dataset_name, database=database)
    ratio_keys = {"p_cells_missing"}
    entries = [
        _metric(key, value, scope, decimals=6 if key in ratio_keys else 2)
        for key, value in summary.items()
    ]
    score = max(min(1 - summary["p_cells_missing"], 1), 0)
    entries.append(
        {
            "key": "score",
            "value": str(round(score, 2)),
            "scope": scope,
        }
    )
    return [entry for entry in entries if entry is not None]


def build_recommendations(prof, summary, dataset_name, options):
    """Findings, rebuilt natively from the profile.

    They used to be scraped out of the ydata-profiling HTML report; the same
    alert families are derived here from the streamed statistics instead.
    """
    recommendations = []

    def add(content, kind, scope, level):
        recommendations.append(
            {
                "content": content,
                "type": kind,
                "scope": scope,
                "level": level,
            }
        )

    for name, column in prof.items():
        scope = _scope(dataset_name, column=name)
        share_missing = column["p_missing"]
        if share_missing > 0:
            content = (
                f"{name} has {column['n_missing']} "
                f"({share_missing * 100:.1f}%) missing values"
            )
            add(content, "Missing", scope, determine_level(content))
        if column["count"] and column["n_distinct"] <= 1:
            add(
                f"{name} has a constant value",
                "Constant",
                scope,
                "warning",
            )
        elif _is_unique(column):
            add(f"{name} has unique values", "Unique", scope, "info")
        elif (
            column["kind"] == "Text"
            and column["n_distinct"] > options["high_cardinality"]
        ):
            add(
                f"{name} has {column['n_distinct']} distinct values",
                "High cardinality",
                scope,
                "info",
            )

        if column["kind"] == "Numeric":
            count = column["count"]
            zeros = column.get("n_zeros") or 0
            if count and zeros / count > ZEROS_THRESHOLD:
                content = (
                    f"{name} has {zeros} ({zeros / count * 100:.1f}%) zeros"
                )
                add(content, "Zeros", scope, determine_level(content))
            skewness = column.get("skewness")
            if skewness is not None and abs(skewness) > SKEWNESS_THRESHOLD:
                add(
                    f"{name} is highly skewed (γ1 = {skewness:.2f})",
                    "Skewed",
                    scope,
                    "warning",
                )
            infinite = column.get("n_infinite") or 0
            if infinite:
                content = (
                    f"{name} has {infinite} "
                    f"({infinite / count * 100:.1f}%) infinite values"
                    if count
                    else f"{name} has {infinite} infinite values"
                )
                add(content, "Infinite", scope, determine_level(content))

    if summary["n_vars_all_missing"]:
        scope = _scope(dataset_name)
        add(
            f"{dataset_name} has {summary['n_vars_all_missing']} "
            f"columns that are entirely missing",
            "Missing",
            scope,
            "high",
        )
    return recommendations


def build_schemas(prof, dataset_name, database=None):
    """One entry per column, plus the dataset itself."""
    entries = [
        {
            "key": "column",
            "value": name,
            "scope": _scope(dataset_name, column=name),
        }
        for name in prof
    ]
    entries.append(
        {
            "key": "dataset",
            "value": dataset_name,
            "scope": _scope(dataset_name, database=database),
        }
    )
    return entries


def add_figures(figures, prof, dataset_name):
    """Aggregates that explain the metrics. Bounded by construction."""
    scope = _scope(dataset_name)

    missing_rows = [
        {"column": name, "p_missing": float(column["p_missing"])}
        for name, column in prof.items()
        if column["p_missing"] > 0
    ]
    if missing_rows:
        figures.add(
            "missing_by_column",
            intent="breakdown",
            of="p_cells_missing",
            frame=missing_rows,
            dims=["column"],
            measures=["p_missing"],
            scope=scope,
            title="Valeurs manquantes par colonne",
        )

    type_counts = {}
    for column in prof.values():
        type_counts[column["kind"]] = type_counts.get(column["kind"], 0) + 1
    if type_counts:
        figures.add(
            "column_types",
            intent="composition",
            frame=[
                {"type": kind, "n_columns": total}
                for kind, total in sorted(type_counts.items())
            ],
            dims=["type"],
            measures=["n_columns"],
            scope=scope,
            title="Répartition des types de colonnes",
        )

    for name, column in prof.items():
        rows = _top_value_rows(column.get("top_values"))
        if rows:
            figures.add(
                f"top_values_{name}",
                intent="distribution",
                frame=rows,
                dims=["value"],
                measures=["count"],
                scope=_scope(dataset_name, column=name),
                title=f"Valeurs les plus fréquentes — {name}",
            )


def _top_value_rows(top_values):
    """Top values as figure rows, with the labels made distinct.

    Two different values can render to the same label (a null and the string
    "None"), and a figure rejects a repeated dimension tuple — which would
    abort the whole run over a cosmetic collision.
    """
    rows = []
    seen = set()
    for item in top_values or []:
        value = item["value"]
        label = None if value is None else str(value)
        if label in seen:
            continue
        seen.add(label)
        rows.append({"value": label, "count": int(item["count"])})
    return rows


def dataset_name_for(pack, object_name, object_count):
    """Scope name of a logical object.

    A single-object source keeps the source name, which is what the previous
    implementation scoped its metrics to and what the platform keys on. With
    several objects the object name is used, and the source becomes the parent
    database scope.
    """
    if object_count <= 1:
        return pack.source_config.get("name") or object_name
    return object_name


def write_report(prof, summary, dataset_name):
    """Persist the profile next to the metrics, as the pack has always done."""
    payload = {"table": summary, "variables": prof}
    with open(f"{dataset_name}_report.json", "w", encoding="utf-8") as file:
        json.dump(_sanitize_for_json(payload), file, indent=4)
    return payload


def copy_report_beside_source(payload, source_config):
    """Drop a copy of the report next to a file source, best effort."""
    try:
        source_dir = os.path.dirname(source_config["config"]["path"])
        today = datetime.now().strftime("%Y%m%d")
        path = os.path.join(
            source_dir,
            f"{today}_profiling_report_{source_config['name']}.json",
        )
        with open(path, "w", encoding="utf-8") as file:
            json.dump(_sanitize_for_json(payload), file, indent=4)
        print(f"Profiling report saved to {path}")
    except (OSError, KeyError) as error:
        logger.warning(
            f"Could not write the report next to the source: {error}"
        )


def main():
    with Pack() as pack:
        pack.figures.declare_measure(
            "p_cells_missing",
            unit="ratio",
            direction="lower_is_better",
            target=0.05,
            warn=0.10,
            label="Taux de cellules manquantes",
        )
        pack.figures.declare_measure(
            "p_missing",
            unit="ratio",
            direction="lower_is_better",
            target=0.05,
            warn=0.10,
            label="Taux de valeurs manquantes",
        )
        pack.figures.declare_measure(
            "n_columns", unit="count", direction="neutral", label="Colonnes"
        )
        pack.figures.declare_measure(
            "count", unit="count", direction="neutral", label="Occurrences"
        )

        is_database = pack.source_config.get("type") == "database"
        if is_database:
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

        options = read_options(pack.pack_config)
        objects = pack.tables("source")
        database = pack.source_config["name"] if is_database else None
        print(f"Generating profile for {len(objects)} dataset(s)")

        for object_name in objects:
            dataset_name = dataset_name_for(pack, object_name, len(objects))
            schema = pack.schema("source", object_name)
            # One LazyFrame per logical object: the parts of a chunked object
            # are a single dataset to the engine, so there is no chunk
            # detection left to do and no partition to profile in isolation.
            lf = pack.scan("source", object_name)

            prof = profile_dataset(lf, schema, options)
            summary = dataset_summary(prof, schema)
            print(
                f"{dataset_name}: {summary['n']} rows, "
                f"{summary['n_var']} columns profiled"
            )

            pack.metrics.data.extend(column_metrics(prof, dataset_name))
            pack.metrics.data.extend(
                dataset_metrics(summary, dataset_name, database=database)
            )
            pack.recommendations.data.extend(
                build_recommendations(prof, summary, dataset_name, options)
            )
            pack.schemas.data.extend(
                build_schemas(prof, dataset_name, database=database)
            )
            add_figures(pack.figures, prof, dataset_name)

            payload = write_report(prof, summary, dataset_name)
            if pack.source_config["type"] == "file" and len(objects) == 1:
                copy_report_beside_source(payload, pack.source_config)

        pack.metrics.save()
        pack.recommendations.save()
        pack.schemas.save()
        pack.figures.save()


if __name__ == "__main__":
    main()
