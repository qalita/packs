"""Tests for the streaming text validation pack.

Everything here works on LazyFrames and asserts on the values the pack would
publish, so a regression in the expression mapping shows up as a wrong metric
rather than as a slow job.
"""

import polars as pl
import pytest

from qalita_core import analytics

import main


# One row of every case the pack reports on, plus a null the metrics must skip.
SAMPLE = pl.LazyFrame(
    {
        "text": [
            "abc",
            None,
            "",
            "  ",
            " hi ",
            "N/A",
            "one two three",
        ],
        "number": [1, 2, 3, 4, 5, 6, 7],
    }
)


class FakePack:
    def __init__(self, tables, name):
        self._tables = tables
        self.source_config = {"name": name}

    def tables(self, trigger):
        return list(self._tables)


def stats_for(min_length=None, max_length=None):
    exprs = main.column_expressions("text", "0", min_length, max_length)
    return analytics.agg(SAMPLE, exprs)


def test_text_columns_reads_dtypes_not_data():
    schema = dict(SAMPLE.collect_schema())
    assert main.text_columns(schema) == ["text"]


def test_categorical_columns_are_analyzed_like_strings():
    frame = pl.LazyFrame(
        {"text": ["a", " bb ", None]}, schema={"text": pl.Categorical}
    )
    assert main.text_columns(dict(frame.collect_schema())) == ["text"]
    stats = analytics.agg(
        frame, main.column_expressions("text", "0", None, None)
    )
    result = main.column_result(stats, "0", None, None)
    assert result["non_null"] == 2
    assert result["max_length"] == 4
    assert result["surrounded_by_whitespace_count"] == 1

    count, rows = analytics.failures(
        frame,
        main.violation_predicate("text", None, None),
        limit=5,
        columns=["text"],
    )
    assert count == 1
    assert rows.height == 1


def test_column_result_matches_the_pandas_semantics():
    result = main.column_result(stats_for(2, 5), "0", 2, 5)

    assert result["non_null"] == 6
    assert result["min_length"] == 0
    assert result["max_length"] == 13
    assert result["mean_length"] == 4.17
    assert result["empty_text_count"] == 1
    # "" is excluded from the whitespace-only count, exactly as before.
    assert result["whitespace_only_count"] == 1
    assert result["null_placeholder_count"] == 1
    assert result["surrounded_by_whitespace_count"] == 2
    assert result["min_word_count"] == 0
    assert result["max_word_count"] == 3
    assert result["below_min_length"] == 1
    assert result["above_max_length"] == 1
    assert result["in_range_percent"] == 0.6667


def test_length_constraints_are_absent_when_no_rule_is_given():
    stats = stats_for()
    assert "0|below_min" not in stats
    result = main.column_result(stats, "0", None, None)
    assert result["below_min_length"] == 0
    assert result["in_range_percent"] == 1.0


def test_all_null_column_yields_the_empty_result():
    empty = pl.LazyFrame({"text": [None, None]}, schema={"text": pl.String})
    stats = analytics.agg(
        empty, main.column_expressions("text", "0", None, None)
    )
    result = main.column_result(stats, "0", None, None)
    assert result["non_null"] == 0
    assert result["in_range_percent"] == 1.0


def test_every_column_travels_in_a_single_agg_call(monkeypatch):
    calls = []
    real_agg = analytics.agg

    def counting_agg(lf, exprs):
        calls.append(len(exprs))
        return real_agg(lf, exprs)

    monkeypatch.setattr(analytics, "agg", counting_agg)

    frame = pl.LazyFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
    exprs = {}
    for i, column in enumerate(["a", "b", "c"]):
        exprs.update(main.column_expressions(column, str(i), None, None))
    analytics.agg(frame, exprs)

    assert len(calls) == 1
    assert calls[0] == 30


def test_failures_are_bounded_and_counted_exactly():
    count, rows = analytics.failures(
        SAMPLE,
        main.violation_predicate("text", 2, 5),
        limit=2,
        columns=["text"],
    )
    assert count == 5
    assert rows.height == 2
    assert rows.columns == ["text"]


def test_example_limit_is_clamped_to_the_hard_cap():
    assert main.example_limit({}) == main.DEFAULT_EXAMPLE_ROWS
    assert main.example_limit({"job": {"examples_limit": 0}}) == 0
    assert main.example_limit({"job": {"examples_limit": 10**9}}) == 1000
    assert main.example_limit({"job": {"examples_limit": "nope"}}) == 10
    assert main.example_limit({"job": {"examples_limit": -5}}) == 0


def test_single_object_source_stays_one_dataset():
    pack = FakePack(["csv_people_part"], "people")
    assert main.dataset_labels(pack) == {"csv_people_part": "people"}


def test_multi_object_source_keeps_object_names():
    pack = FakePack(["db_users", "db_orders"], "warehouse")
    assert main.dataset_labels(pack) == {
        "db_users": "db_users",
        "db_orders": "db_orders",
    }


def test_metric_keys_are_preserved():
    result = main.column_result(stats_for(2, 5), "0", 2, 5)
    keys = {
        m["key"] for m in main.metrics_for_column("ds", "text", result, 2, 5)
    }
    assert keys == {
        "text_min_length",
        "text_max_length",
        "text_mean_length",
        "min_word_count",
        "max_word_count",
        "text_length_below_min_length",
        "text_length_above_max_length",
        "text_length_in_range_percent",
        "empty_text_found",
        "whitespace_text_found",
        "null_placeholder_text_found",
        "text_surrounded_by_whitespace_found",
    }


def test_recommendations_only_fire_on_non_zero_counts():
    result = main.column_result(stats_for(2, 5), "0", 2, 5)
    types = {
        r["type"]
        for r in main.recommendations_for_column("ds", "text", result, 2, 5)
    }
    assert types == {
        "Empty Text Found",
        "Whitespace Only Text",
        "Null Placeholder Found",
        "Text Surrounded By Whitespace",
        "Text Too Short",
        "Text Too Long",
    }

    clean = pl.LazyFrame({"text": ["abc", "defg"]})
    stats = analytics.agg(
        clean, main.column_expressions("text", "0", None, None)
    )
    assert (
        main.recommendations_for_column(
            "ds",
            "text",
            main.column_result(stats, "0", None, None),
            None,
            None,
        )
        == []
    )


def test_pack_does_not_import_pandas_or_numpy():
    import sys

    assert "pandas" not in dir(main)
    assert "numpy" not in dir(main)
    source = open(main.__file__, encoding="utf-8").read()
    assert "import pandas" not in source
    assert "import numpy" not in source
    # The eager preamble is gone: no pack code materializes a parquet part.
    assert "pd.read_parquet(" not in source
    assert "pl.read_parquet(" not in source
    del sys


@pytest.mark.parametrize(
    "value,expected",
    [("N/A", True), ("n/a", True), ("#NULL!", True), ("hello", False)],
)
def test_null_placeholders_are_matched_case_insensitively(value, expected):
    frame = pl.LazyFrame({"text": [value]})
    stats = analytics.agg(
        frame, main.column_expressions("text", "0", None, None)
    )
    assert bool(stats["0|placeholder"]) is expected
