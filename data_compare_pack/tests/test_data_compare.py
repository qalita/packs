"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Tests for the streaming data comparison.
"""

import polars as pl
import pytest

import main


SOURCE = pl.LazyFrame(
    {
        "id": [1, 2, 3, 4],
        "amount": [10.0, 20.0, 30.0, 40.0],
        "label": ["a", "b", "c", "d"],
    }
)
# id 4 missing, id 5 extra, amount differs on 2 (beyond tolerance) and on 3
# (within tolerance), label differs on 3.
TARGET = pl.LazyFrame(
    {
        "id": [1, 2, 3, 5],
        "amount": [10.0, 25.0, 30.00001, 50.0],
        "label": ["a", "b", "C", "e"],
    }
)

SOURCE_SCHEMA = {"id": pl.Int64, "amount": pl.Float64, "label": pl.String}
TARGET_SCHEMA = dict(SOURCE_SCHEMA)


def prepare(source=SOURCE, target=TARGET, compare=(), ids=("id",)):
    columns, id_columns = main.resolve_compare_columns(
        SOURCE_SCHEMA, TARGET_SCHEMA, list(compare), list(ids)
    )
    suffix = main.target_suffix(columns)
    joined = main.build_join(source, target, columns, id_columns, suffix)
    value_columns = [c for c in columns if c not in id_columns]
    return joined, columns, id_columns, value_columns, suffix


def test_resolve_compare_columns_keeps_source_order():
    columns, ids = main.resolve_compare_columns(
        SOURCE_SCHEMA, TARGET_SCHEMA, [], ["id"]
    )
    assert columns == ["id", "amount", "label"]
    assert ids == ["id"]


def test_resolve_compare_columns_falls_back_to_every_column_as_key():
    _, ids = main.resolve_compare_columns(SOURCE_SCHEMA, TARGET_SCHEMA, [], [])
    assert ids == ["id", "amount", "label"]


def test_resolve_compare_columns_rejects_missing_column():
    with pytest.raises(ValueError, match="missing in target"):
        main.resolve_compare_columns(
            {"a": pl.Int64, "b": pl.Int64}, {"a": pl.Int64}, ["a", "b"], ["a"]
        )


def test_target_suffix_avoids_ambiguous_renaming():
    assert main.target_suffix(["id", "amount"]) == "_target"
    assert (
        main.target_suffix(["id", "amount_target"])
        == main.FALLBACK_TARGET_SUFFIX
    )


def test_compare_stats_counts():
    joined, _, _, value_columns, suffix = prepare()
    stats = main.compare_stats(
        joined,
        value_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0001,
        0.0,
        suffix,
    )
    assert stats["rows_in_common"] == 3
    assert stats["source_only"] == 1
    assert stats["target_only"] == 1
    # amount: only id 2 is beyond the tolerance. label: only id 3.
    assert stats["unequal_by_column"] == {"amount": 1, "label": 1}
    assert stats["unequal_values"] == 2
    assert stats["unequal_rows"] == 2


def test_tolerance_is_honoured():
    joined, _, _, value_columns, suffix = prepare()
    strict = main.compare_stats(
        joined,
        value_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0,
        0.0,
        suffix,
    )
    # Without a tolerance, the 1e-5 difference on id 3 becomes a mismatch.
    assert strict["unequal_by_column"]["amount"] == 2


def test_nulls_compare_equal_to_nulls():
    source = pl.LazyFrame({"id": [1, 2], "v": [None, "x"]})
    target = pl.LazyFrame({"id": [1, 2], "v": [None, None]})
    schema = {"id": pl.Int64, "v": pl.String}
    columns, ids = main.resolve_compare_columns(schema, schema, [], ["id"])
    suffix = main.target_suffix(columns)
    joined = main.build_join(source, target, columns, ids, suffix)
    stats = main.compare_stats(
        joined, ["v"], schema, schema, 0.0001, 0.0, suffix
    )
    assert stats["unequal_by_column"]["v"] == 1
    assert stats["rows_in_common"] == 2


def test_null_join_keys_are_matched():
    source = pl.LazyFrame({"id": [None, 1], "v": ["a", "b"]})
    target = pl.LazyFrame({"id": [None, 1], "v": ["a", "b"]})
    schema = {"id": pl.Int64, "v": pl.String}
    columns, ids = main.resolve_compare_columns(schema, schema, [], ["id"])
    suffix = main.target_suffix(columns)
    joined = main.build_join(source, target, columns, ids, suffix)
    stats = main.compare_stats(
        joined, ["v"], schema, schema, 0.0001, 0.0, suffix
    )
    assert stats["rows_in_common"] == 2
    assert stats["source_only"] == 0


def test_mismatched_dtypes_are_compared_as_text():
    source = pl.LazyFrame({"id": [1], "v": [1]})
    target = pl.LazyFrame({"id": [1], "v": ["1"]})
    source_schema = {"id": pl.Int64, "v": pl.Int64}
    target_schema = {"id": pl.Int64, "v": pl.String}
    columns, ids = main.resolve_compare_columns(
        source_schema, target_schema, [], ["id"]
    )
    suffix = main.target_suffix(columns)
    joined = main.build_join(source, target, columns, ids, suffix)
    stats = main.compare_stats(
        joined, ["v"], source_schema, target_schema, 0.0001, 0.0, suffix
    )
    assert stats["unequal_by_column"]["v"] == 0


def test_mismatch_examples_are_bounded_and_renamed():
    joined, _, id_columns, _, suffix = prepare()
    count, rows = main.mismatch_examples(
        joined,
        ["amount", "label"],
        id_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0001,
        0.0,
        suffix,
        limit=1,
    )
    assert count == 2
    assert rows.height == 1
    assert rows.columns == [
        "id",
        "amount_source",
        "amount_target",
        "label_source",
        "label_target",
    ]


def test_mismatch_examples_respect_a_zero_limit():
    joined, _, id_columns, _, suffix = prepare()
    count, rows = main.mismatch_examples(
        joined,
        ["amount"],
        id_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0001,
        0.0,
        suffix,
        limit=0,
    )
    assert (count, rows.height) == (0, 0)


def test_mismatches_table_flags_truncation():
    rows = pl.DataFrame({"id": [1], "amount_source": [10.0]})
    table = main.mismatches_table(rows, total_mismatches=5, limit=1)
    assert table["columnLabels"] == ["id", "amount_source"]
    assert table["data"] == [[{"value": 1}, {"value": 10.0}]]
    assert table["truncated"] is True
    assert table["total_mismatches"] == 5
    assert "truncated" not in main.mismatches_table(rows, 1, 10)


def test_scores():
    stats = {"rows_in_common": 3, "unequal_rows": 2}
    computed = main.scores(stats, source_rows=4, target_rows=4)
    assert computed["precision"] == 0.75
    assert computed["recall"] == 0.75
    assert computed["f1_score"] == 0.75
    assert computed["score"] == 0.5


def test_scores_on_empty_sides():
    stats = {"rows_in_common": 0, "unequal_rows": 0}
    computed = main.scores(stats, source_rows=0, target_rows=0)
    assert computed == {
        "score": 0.0,
        "precision": 0.0,
        "recall": 0.0,
        "f1_score": 0.0,
    }


def test_independent_head_sampling_would_have_been_wrong():
    """Why the previous per-side head() sampling produced wrong metrics.

    Both sides hold the same 10 keys in a different order and every value
    agrees. Joining the full sides finds all 10 rows in common; taking the
    first 5 rows of each side independently leaves NO key in common, which
    would have reported precision = recall = f1 = 0 on identical data.
    """
    keys = list(range(10))
    source = pl.LazyFrame({"id": keys, "v": keys})
    target = pl.LazyFrame({"id": keys[::-1], "v": keys[::-1]})
    schema = {"id": pl.Int64, "v": pl.Int64}
    columns, ids = main.resolve_compare_columns(schema, schema, [], ["id"])
    suffix = main.target_suffix(columns)

    full = main.compare_stats(
        main.build_join(source, target, columns, ids, suffix),
        ["v"],
        schema,
        schema,
        0.0,
        0.0,
        suffix,
    )
    assert full["rows_in_common"] == 10
    assert full["unequal_rows"] == 0

    sampled = main.compare_stats(
        main.build_join(source.head(5), target.head(5), columns, ids, suffix),
        ["v"],
        schema,
        schema,
        0.0,
        0.0,
        suffix,
    )
    assert sampled["rows_in_common"] == 0


def test_build_metrics_keeps_the_historical_keys():
    joined, columns, _, value_columns, suffix = prepare()
    stats = main.compare_stats(
        joined,
        value_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0001,
        0.0,
        suffix,
    )
    metrics = main.build_metrics(
        stats,
        "src",
        "tgt",
        columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        4,
        4,
        0.0001,
        0.0,
    )
    keys = {m["key"] for m in metrics}
    for expected in (
        "dataframe_summary_number_columns_src",
        "dataframe_summary_number_columns_tgt",
        "dataframe_summary_number_rows_src",
        "dataframe_summary_number_rows_tgt",
        "column_summary_number_of_columns_in_common",
        "column_summary_number_of_columns_in_src_but_not_in_tgt",
        "column_summary_number_of_columns_in_tgt_but_not_in_src",
        "row_summary_default_absolute_tolerance",
        "row_summary_default_relative_tolerance",
        "row_summary_number_of_rows_in_common",
        "row_summary_number_of_rows_in_src_but_not_in_tgt",
        "row_summary_number_of_rows_in_tgt_but_not_in_src",
        "row_summary_number_of_rows_with_some_compared_columns_unequal",
        "row_summary_number_of_rows_with_all_compared_columns_equal",
        "column_comparison_number_of_columns_compared_with_some_values_unequal",
        "column_comparison_number_of_columns_compared_with_all_values_equal",
        "column_comparison_total_number_of_values_which_compare_unequal",
    ):
        assert expected in keys

    values = {m["key"]: m["value"] for m in metrics}
    assert values["row_summary_number_of_rows_in_common"] == "3"
    assert values["row_summary_number_of_rows_in_src_but_not_in_tgt"] == "1"
    assert values["row_summary_number_of_rows_in_tgt_but_not_in_src"] == "1"
    assert (
        values[
            "column_comparison_total_number_of_values_which_compare_unequal"
        ]
        == "2"
    )
    assert values["dataframe_summary_number_rows_src"] == 4


def test_large_counts_are_not_truncated_by_thousand_separators():
    """The old report parser read '12,345' as 12: the digits before the comma."""
    ids = list(range(2000))
    source = pl.LazyFrame({"id": ids, "v": ids})
    target = pl.LazyFrame({"id": ids, "v": [i + 1 for i in ids]})
    schema = {"id": pl.Int64, "v": pl.Int64}
    columns, key_columns = main.resolve_compare_columns(
        schema, schema, [], ["id"]
    )
    suffix = main.target_suffix(columns)
    stats = main.compare_stats(
        main.build_join(source, target, columns, key_columns, suffix),
        ["v"],
        schema,
        schema,
        0.0,
        0.0,
        suffix,
    )
    metrics = main.build_metrics(
        stats, "s", "t", columns, schema, schema, 2000, 2000, 0.0, 0.0
    )
    values = {m["key"]: m["value"] for m in metrics}
    assert values["row_summary_number_of_rows_in_common"] == "2000"


def test_build_report_mentions_both_sides():
    joined, columns, id_columns, value_columns, suffix = prepare()
    stats = main.compare_stats(
        joined,
        value_columns,
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
        0.0001,
        0.0,
        suffix,
    )
    report = main.build_report(
        "src", "tgt", stats, columns, id_columns, 4, 4, 0.0001, 0.0
    )
    assert "Number of rows in common: 3" in report
    assert "Number of rows in src but not in tgt: 1" in report
    assert "Matched on: id" in report


def test_examples_limit_is_clamped():
    assert main._examples_limit({}) == main.DEFAULT_MISMATCH_EXAMPLES
    assert main._examples_limit({"job": {"mismatch_examples": 3}}) == 3
    assert main._examples_limit({"job": {"mismatch_examples": -5}}) == 0
    assert (
        main._examples_limit({"job": {"mismatch_examples": 10**6}})
        == main.MAX_MISMATCH_EXAMPLES
    )


def test_pairings_by_name_not_by_parquet_part():
    assert main._pairings(["a", "b"], ["x", "y"]) == [("a", "x"), ("b", "y")]
    assert main._pairings(["a", "b"], ["x"]) == [("a", "x")]


def test_write_excel_writes_a_readable_file(tmp_path):
    frame = pl.DataFrame({"id": [1], "v_source": ["a"], "v_target": ["b"]})
    written = main.write_excel(frame, str(tmp_path / "mismatches.xlsx"))
    assert len(open(written, "rb").read()) > 0
