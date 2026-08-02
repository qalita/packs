"""
Accuracy Pack

Decimal-precision consistency of float columns, plus latitude/longitude range
validation.

Everything is computed by streaming aggregation over a Polars LazyFrame. The
source is read a fixed number of times whatever its width — the previous
implementation decompressed every parquet part into pandas at once and then
walked each float column with a Python ``.apply``, which is the shape that
cannot survive a chunked 100 GiB source.
"""

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

# A float64 renders at most ~17 significant decimals, so the per-column bucket
# enumeration below is bounded even on values such as 0.30000000000000004.
MAX_DECIMAL_BUCKETS = 32

# Expressions per streaming pass. A 10k-column table would otherwise build a
# single projection with 10k+ nodes; batching keeps the plan sane while still
# reading the source once per batch instead of once per column.
MAX_EXPRS_PER_PASS = 512

# Bounded row-level evidence. Each example set costs one filtered pass, so the
# number of checks that get examples is capped as well as the row count.
DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000
DEFAULT_MAX_EXAMPLE_CHECKS = 20

# Ranges a geographic coordinate must fall in.
GEO_RANGES = {
    "latitude": (-90, 90),
    "longitude": (-180, 180),
}


def defined(column: str) -> pl.Expr:
    """The column with NaN folded into null.

    pandas' ``dropna()`` dropped NaN as well as null. Polars does not, so the
    fold is what keeps the migrated counts comparable with the previous ones.
    """
    col = pl.col(column)
    return pl.when(col.is_not_nan()).then(col)


def decimals_expr(column: str) -> pl.Expr:
    """Digits after the decimal point, per row, null where the value is not.

    The derived key has a tiny cardinality (0..17), which is why this pack can
    stay exact: counting its buckets costs nothing next to sorting the column.
    """
    col = pl.col(column)
    return pl.when(col.is_not_null() & col.is_not_nan()).then(
        col.cast(pl.Utf8)
        .str.split(".")
        .list.get(1, null_on_oob=True)
        .str.len_chars()
        .fill_null(0)
    )


def agg_batched(lf: "pl.LazyFrame", exprs: dict) -> dict:
    """``analytics.agg`` with a bound on the size of a single projection."""
    out: dict = {}
    for batch in analytics.batched(exprs.items(), MAX_EXPRS_PER_PASS):
        out.update(analytics.agg(lf, dict(batch)))
    return out


def float_columns(schema: dict) -> list:
    """Float columns of a schema, read from the parquet footers."""
    return [
        name
        for name, dtype in schema.items()
        if dtype in (pl.Float32, pl.Float64)
    ]


def geo_checks(schema: dict) -> list:
    """Coordinate columns detected by name, with the range they must respect."""
    numeric = set(analytics.numeric_columns(schema))
    checks = []
    for name in schema:
        lowered = name.lower()
        if name not in numeric:
            continue
        if "lat" in lowered:
            checks.append((name, "latitude"))
        if "lon" in lowered or "lng" in lowered:
            checks.append((name, "longitude"))
    return checks


def profile(lf: "pl.LazyFrame", columns: list, geo: list) -> tuple:
    """Decimal-count distributions and coordinate violations.

    Two streaming passes whatever the number of columns: the first resolves how
    many decimal buckets each column actually needs (and settles the geographic
    ranges on the way), the second counts those buckets for every column at
    once.

    Returns:
        ``({column: {"max_decimals", "valid_points", "counts"}},
          {column: {"kind", "invalid", "valid_points"}})``
    """
    first = {}
    for i, col in enumerate(columns):
        first[f"dmax|{i}"] = decimals_expr(col).max()
        first[f"n|{i}"] = defined(col).count()
    for j, (col, kind) in enumerate(geo):
        low, high = GEO_RANGES[kind]
        value = defined(col)
        first[f"gbad|{j}"] = ((value < low) | (value > high)).sum()
        first[f"gn|{j}"] = value.count()

    bounds = agg_batched(lf, first) if first else {}

    bucket_exprs = {}
    for i, col in enumerate(columns):
        valid_points = int(bounds.get(f"n|{i}") or 0)
        top = bounds.get(f"dmax|{i}")
        if not valid_points or top is None:
            continue
        # Clamping only ever drops buckets no float64 can populate, so the mode
        # is never the one that gets cut.
        top = min(int(top), MAX_DECIMAL_BUCKETS)
        dec = decimals_expr(col)
        for d in range(top + 1):
            bucket_exprs[f"c|{i}|{d}"] = (dec == d).sum()

    buckets = agg_batched(lf, bucket_exprs) if bucket_exprs else {}

    decimals = {}
    for i, col in enumerate(columns):
        valid_points = int(bounds.get(f"n|{i}") or 0)
        if not valid_points:
            continue
        top = bounds.get(f"dmax|{i}")
        top = 0 if top is None else min(int(top), MAX_DECIMAL_BUCKETS)
        decimals[col] = {
            "max_decimals": int(bounds.get(f"dmax|{i}") or 0),
            "valid_points": valid_points,
            "counts": {
                d: int(buckets.get(f"c|{i}|{d}") or 0) for d in range(top + 1)
            },
        }

    coordinates = {}
    for j, (col, kind) in enumerate(geo):
        valid_points = int(bounds.get(f"gn|{j}") or 0)
        if not valid_points:
            continue
        coordinates[col] = {
            "kind": kind,
            "invalid": int(bounds.get(f"gbad|{j}") or 0),
            "valid_points": valid_points,
        }
    return decimals, coordinates


def most_common(counts: dict) -> tuple:
    """``(value, occurrences)`` of the most frequent decimal count.

    Ties resolve to the smallest count, which is what ``Series.mode()[0]``
    returned before.
    """
    best_value, best_n = None, -1
    for value in sorted(counts):
        if counts[value] > best_n:
            best_value, best_n = value, counts[value]
    return best_value, max(best_n, 0)


class Examples:
    """Bounded row-level evidence for failing checks.

    The pack emitted counts with nothing to look at; this attaches at most
    ``job.example_rows`` failing rows to a check. Both the row count and the
    number of checks that get rows are capped: every example set is one extra
    filtered pass over the source.
    """

    def __init__(self, pack_config: dict):
        job = pack_config.get("job", {}) or {}
        enabled = job.get("examples", True)
        rows = int(job.get("example_rows", DEFAULT_EXAMPLE_ROWS))
        self.limit = min(max(rows, 0), MAX_EXAMPLE_ROWS) if enabled else 0
        self.budget = int(
            job.get("example_max_checks", DEFAULT_MAX_EXAMPLE_CHECKS)
        )
        self.id_columns = list(job.get("id_columns") or [])

    def rows(self, lf, predicate, column: str, schema: dict) -> list:
        if self.limit <= 0 or self.budget <= 0:
            return []
        self.budget -= 1
        keep = [
            name
            for name in self.id_columns
            if name in schema and name != column
        ]
        keep.append(column)
        _, examples = analytics.failures(
            lf, predicate, limit=self.limit, columns=keep
        )
        return examples.to_dicts()


def column_scope(column: str, dataset_label: str) -> dict:
    return {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
    }


def run(pack: Pack) -> None:
    """Analyse every logical object of the source."""
    examples = Examples(pack.pack_config)

    for dataset_label in pack.tables("source"):
        lf = pack.scan("source", table=dataset_label)
        schema = pack.schema("source", table=dataset_label)

        floats = float_columns(schema)
        geo = geo_checks(schema)
        if not floats:
            print(f"[{dataset_label}] No float columns found.")
        decimals, coordinates = profile(lf, floats, geo)

        total_proportion_score = 0.0
        valid_columns_count = 0
        float_total_proportion_score = 0.0
        valid_points_count = 0

        for column in floats:
            stats = decimals.get(column)
            if not stats:
                continue
            valid_data_points = stats["valid_points"]
            mode, mode_count = most_common(stats["counts"])
            proportion_score = (
                mode_count / valid_data_points if mode is not None else 0
            )

            total_proportion_score += proportion_score
            valid_columns_count += 1
            float_total_proportion_score += (
                proportion_score * valid_data_points
            )
            valid_points_count += valid_data_points

            scope = column_scope(column, dataset_label)
            if stats["max_decimals"] > 0:
                pack.metrics.data.append(
                    {
                        "key": "decimal_precision",
                        "value": str(stats["max_decimals"]),
                        "scope": scope.copy(),
                    }
                )
            pack.metrics.data.append(
                {
                    "key": "proportion_score",
                    "value": str(round(proportion_score, 2)),
                    "scope": scope.copy(),
                }
            )
            if mode is not None:
                pack.metrics.data.append(
                    {
                        "key": "most_common_decimals",
                        "value": str(mode),
                        "scope": scope.copy(),
                    }
                )

            if proportion_score < 0.9:
                rows = examples.rows(
                    lf, decimals_expr(column) != mode, column, schema
                )
                if rows:
                    pack.metrics.data.append(
                        {
                            "key": "uneven_decimals_examples",
                            "value": rows,
                            "scope": scope.copy(),
                        }
                    )
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {(1-proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                        "type": "Unevenly Rounded Data",
                        "scope": scope.copy(),
                        "level": determine_recommendation_level(
                            1 - proportion_score
                        ),
                    }
                )

        # A dataset without a single float column has no decimal score to
        # report; one whose float columns are all empty scores 0, as before.
        if floats:
            mean_proportion_score = (
                total_proportion_score / valid_columns_count
                if valid_columns_count > 0
                else 0
            )
            float_mean_proportion_score = (
                float_total_proportion_score / valid_points_count
                if valid_points_count > 0
                else 0
            )
            dataset_scope = {"perimeter": "dataset", "value": dataset_label}
            pack.metrics.data.append(
                {
                    "key": "float_score",
                    "value": str(round(float_mean_proportion_score, 2)),
                    "scope": dataset_scope.copy(),
                }
            )
            pack.metrics.data.append(
                {
                    "key": "score",
                    "value": str(round(mean_proportion_score, 2)),
                    "scope": dataset_scope.copy(),
                }
            )
            if mean_proportion_score < 0.9:
                pack.recommendations.data.append(
                    {
                        "content": f"The dataset '{dataset_label}' has {(1-mean_proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                        "type": "Unevenly Rounded Data",
                        "scope": dataset_scope.copy(),
                        "level": determine_recommendation_level(
                            1 - mean_proportion_score
                        ),
                    }
                )

        for column, stats in coordinates.items():
            kind = stats["kind"]
            low, high = GEO_RANGES[kind]
            invalid = stats["invalid"]
            valid_percent = 1 - (invalid / stats["valid_points"])
            scope = column_scope(column, dataset_label)
            pack.metrics.data.append(
                {
                    "key": f"invalid_{kind}",
                    "value": invalid,
                    "scope": scope.copy(),
                }
            )
            pack.metrics.data.append(
                {
                    "key": f"valid_{kind}_percent",
                    "value": str(round(valid_percent, 4)),
                    "scope": scope.copy(),
                }
            )
            if invalid > 0:
                value = defined(column)
                rows = examples.rows(
                    lf, (value < low) | (value > high), column, schema
                )
                if rows:
                    pack.metrics.data.append(
                        {
                            "key": f"invalid_{kind}_examples",
                            "value": rows,
                            "scope": scope.copy(),
                        }
                    )
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {invalid} invalid {kind} values (outside {low} to {high} range).",
                        "type": f"Invalid {kind.capitalize()}",
                        "scope": scope.copy(),
                        "level": determine_recommendation_level(
                            invalid / stats["valid_points"]
                        ),
                    }
                )


if __name__ == "__main__":
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

        run(pack)

        pack.metrics.save()
        pack.recommendations.save()
