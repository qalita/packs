"""Tests for the streaming timeliness pack.

The two properties worth protecting are that sniffing stays bounded (it used to
build an exact distinct set per column) and that the coalesce chain parses what
the old ``format="mixed"`` path parsed for the documented formats.
"""

import datetime as dt

import polars as pl
import pytest

from qalita_core import analytics

import main


class FakePack:
    def __init__(self, tables, name):
        self._tables = tables
        self.source_config = {"name": name}

    def tables(self, trigger):
        return list(self._tables)


def test_is_date_recognises_years_dates_and_rejects_the_rest():
    assert main.is_date("1999") == "year_only"
    assert main.is_date(2001) == "year_only"
    assert main.is_date("1800") is False
    assert main.is_date("2999") is False
    assert main.is_date("2024-01-15") is True
    assert main.is_date("2024-01-15 10:30:00") is True
    assert main.is_date("hello") is False
    assert main.is_date("") is False


def test_matching_formats_resolves_ambiguity_per_value():
    # Unambiguous day-first: month 15 does not exist, so only %d/%m/%Y parses.
    assert main.matching_formats("15/01/2024") == ["%d/%m/%Y"]
    # Genuinely ambiguous: month-first comes first, as the pandas path did.
    assert main.matching_formats("01/02/2024") == ["%m/%d/%Y", "%d/%m/%Y"]
    assert main.matching_formats("2024-13-01") == []


def test_sniff_samples_is_bounded_and_skips_leading_nulls():
    frame = pl.LazyFrame(
        {
            "sparse": [None] * 100 + ["2024-01-15"] * 100,
            "dense": [f"value-{i}" for i in range(200)],
        }
    )
    samples = main.sniff_samples(frame, ["sparse", "dense"], n=50)

    assert len(samples["sparse"]) == 50
    assert set(samples["sparse"]) == {"2024-01-15"}
    assert len(samples["dense"]) == 50


def test_sniff_samples_uses_one_pass_for_every_column(monkeypatch):
    calls = []
    real_agg = analytics.agg

    def counting_agg(lf, exprs):
        calls.append(sorted(exprs))
        return real_agg(lf, exprs)

    monkeypatch.setattr(main.analytics, "agg", counting_agg)
    frame = pl.LazyFrame({"a": ["1"], "b": ["2"], "c": ["3"]})
    main.sniff_samples(frame, ["a", "b", "c"])
    assert len(calls) == 1
    assert calls[0] == ["0", "1", "2"]


def test_classify_columns_splits_native_text_and_year_columns():
    frame = pl.LazyFrame(
        {
            "native": [dt.date(2024, 1, 15), dt.date(2024, 3, 20)],
            "textual": ["2024-01-15", "20/03/2024"],
            "year": [1990, 2020],
            "label": ["alpha", "beta"],
        }
    )
    schema = dict(frame.collect_schema())
    samples = main.sniff_samples(
        frame, [c for c in schema if not main.is_temporal(schema[c])]
    )
    date_columns, year_columns = main.classify_columns(schema, samples)

    assert date_columns["native"] == []
    assert date_columns["textual"] == ["%Y-%m-%d", "%d/%m/%Y"]
    assert year_columns == ["year"]
    assert "label" not in date_columns


def test_date_expression_parses_every_sniffed_format_in_one_column():
    frame = pl.LazyFrame({"d": ["2024-01-15", "20/03/2024", "bogus", None]})
    parsed = main.date_expression("d", pl.String, ["%Y-%m-%d", "%d/%m/%Y"])
    result = analytics.agg(
        frame,
        {
            "min": parsed.min(),
            "max": parsed.max(),
            "unparsed": (pl.col("d").is_not_null() & parsed.is_null()).sum(),
        },
    )
    assert result["min"] == dt.date(2024, 1, 15)
    assert result["max"] == dt.date(2024, 3, 20)
    # "bogus" is counted; the null is not a parse failure.
    assert result["unparsed"] == 1


def test_date_expression_handles_datetime_formats():
    frame = pl.LazyFrame({"d": ["2024-01-15 10:30:00"]})
    parsed = main.date_expression("d", pl.String, ["%Y-%m-%d %H:%M:%S"])
    assert analytics.agg(frame, {"min": parsed.min()})["min"] == dt.date(
        2024, 1, 15
    )


def test_native_datetime_columns_are_read_without_parsing():
    frame = pl.LazyFrame(
        {"d": [dt.datetime(2024, 1, 15, 8, 0), dt.datetime(2024, 3, 20, 9, 0)]}
    )
    dtype = dict(frame.collect_schema())["d"]
    parsed = main.date_expression("d", dtype, [])
    result = analytics.agg(frame, {"min": parsed.min(), "max": parsed.max()})
    assert result["min"] == dt.date(2024, 1, 15)
    assert result["max"] == dt.date(2024, 3, 20)


def test_unparsed_examples_are_bounded():
    frame = pl.LazyFrame({"d": ["bad"] * 500 + ["2024-01-15"]})
    parsed = main.date_expression("d", pl.String, ["%Y-%m-%d"])
    count, rows = analytics.failures(
        frame,
        pl.col("d").is_not_null() & parsed.is_null(),
        limit=main.DEFAULT_EXAMPLE_ROWS,
        columns=["d"],
    )
    assert count == 500
    assert rows.height == 10


def test_timeliness_score_decays_linearly_over_a_year():
    assert main.calculate_timeliness_score(0) == 1.0
    assert main.calculate_timeliness_score(365) == 0.0
    assert main.calculate_timeliness_score(10_000) == 0.0
    assert round(main.calculate_timeliness_score(182.5), 2) == 0.5


def test_aggregator_emits_the_historical_metric_keys():
    aggregator = main.TimelinessAggregator()
    aggregator.add_date_obs("d", dt.date(2020, 1, 1), dt.date(2020, 6, 1))
    aggregator.add_year_obs("y", 1990, 2000)
    metrics, recommendations = aggregator.finalize_metrics(
        dataset_scope_name="ds",
        compute_score_columns=None,
        calc_timeliness_score=main.calculate_timeliness_score,
    )
    keys = {m["key"] for m in metrics}
    assert {
        "earliest_date",
        "latest_date",
        "days_since_earliest_date",
        "days_since_latest_date",
        "timeliness_score",
        "earliest_year",
        "latest_year",
        "days_since_earliest_year",
        "days_since_latest_year",
        "score",
    } <= keys
    assert recommendations


def test_single_object_source_stays_one_dataset():
    pack = FakePack(["csv_events_part"], "events")
    assert main.dataset_labels(pack) == {"csv_events_part": "events"}
    multi = FakePack(["db_a", "db_b"], "warehouse")
    assert main.dataset_labels(multi) == {"db_a": "db_a", "db_b": "db_b"}


def test_example_limit_is_clamped():
    assert main.example_limit({}) == 10
    assert main.example_limit({"job": {"examples_limit": 5000}}) == 1000
    assert main.example_limit({"job": {"examples_limit": 0}}) == 0


@pytest.mark.parametrize(
    "dtype,sniffable",
    [
        (pl.String, True),
        (pl.Int64, True),
        (pl.Float64, True),
        (pl.Binary, False),
        (pl.List(pl.Int64), False),
    ],
)
def test_only_castable_columns_are_sniffed(dtype, sniffable):
    assert main.is_sniffable(dtype) is sniffable


def test_pack_does_not_import_pandas_or_dateutil():
    import ast

    tree = ast.parse(open(main.__file__, encoding="utf-8").read())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert "pandas" not in imported
    assert "dateutil" not in imported

    # Docstrings are excluded on purpose: they describe what was removed.
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    # The exact distinct set per column is what made sniffing unbounded.
    assert "unique" not in called
    assert "read_parquet" not in called
    assert "dropna" not in called
    # Every materialization goes through qalita_core.analytics.
    assert "collect" not in called
