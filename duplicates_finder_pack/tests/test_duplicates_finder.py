"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Tests for the streaming duplicates finder.
"""

import polars as pl
import pytest

import main


def make_lf():
    """3 duplicate rows on (a, b): two extra 'x' rows and one extra null row."""
    return pl.LazyFrame(
        {
            "a": [1, 1, 1, 2, None, None, 3],
            "b": ["x", "x", "x", "y", None, None, "z"],
            "payload": [10, 11, 12, 13, 14, 15, 16],
        }
    )


def test_resolve_uniqueness_columns_defaults_to_every_column():
    schema = {"a": pl.Int64, "b": pl.String}
    assert main.resolve_uniqueness_columns({}, schema) == ["a", "b"]


def test_resolve_uniqueness_columns_uses_configuration():
    schema = {"a": pl.Int64, "b": pl.String}
    config = {"job": {"compute_uniqueness_columns": ["b"]}}
    assert main.resolve_uniqueness_columns(config, schema) == ["b"]


def test_resolve_uniqueness_columns_rejects_unknown_column():
    config = {"job": {"compute_uniqueness_columns": ["nope"]}}
    with pytest.raises(ValueError, match="nope"):
        main.resolve_uniqueness_columns(config, {"a": pl.Int64})


def test_duplicate_counts_exact():
    total, duplicates, method = main.duplicate_counts(
        make_lf(), ["a", "b"], exact=True
    )
    # 7 rows, 4 distinct combinations -> 3 duplicates, nulls forming a group.
    assert (total, duplicates, method) == (7, 3, "exact")


def test_duplicate_counts_on_a_subset_of_columns():
    total, duplicates, _ = main.duplicate_counts(make_lf(), ["a"], exact=True)
    # a = [1, 1, 1, 2, null, null, 3] -> 4 groups -> 3 duplicates.
    assert (total, duplicates) == (7, 3)


def test_duplicate_counts_approximate_reports_its_method():
    total, duplicates, method = main.duplicate_counts(
        make_lf(), ["a", "b"], exact=False
    )
    assert method == "hyperloglog"
    assert total == 7
    # HyperLogLog is exact at this cardinality; the point is the label.
    assert duplicates == 3


def test_duplicate_counts_requires_columns():
    with pytest.raises(ValueError):
        main.duplicate_counts(make_lf(), [], exact=True)


def test_duplicate_counts_on_a_dataset_without_duplicates():
    lf = pl.LazyFrame({"a": [1, 2, 3]})
    assert main.duplicate_counts(lf, ["a"], exact=True) == (3, 0, "exact")


def test_duplicate_rows_are_bounded():
    count, rows = main.duplicate_rows(make_lf(), ["a", "b"], limit=2)
    # Rows living in a duplicated group: 3 x (1,x) + 2 x (null, null).
    assert count == 5
    assert rows.height == 2
    assert rows.columns == ["a", "b", "payload"]
    assert main.duplicate_rows(make_lf(), ["a", "b"], limit=0)[1].height == 0


def test_duplicate_rows_keeps_null_keyed_duplicates():
    _, rows = main.duplicate_rows(make_lf(), ["a", "b"], limit=100)
    payloads = set(rows["payload"].to_list())
    assert {14, 15}.issubset(payloads)


def test_duplicate_metrics_keys_and_values():
    metrics = main.duplicate_metrics("ds", 7, 3, "exact")
    values = {m["key"]: m["value"] for m in metrics}
    assert values["score"] == "0.57"
    assert values["duplicates"] == 3
    assert values["distinct_count"] == 4
    assert values["distinct_percent"] == "0.5714"
    assert values["duplicates_method"] == "exact"
    assert values["distinct_count_method"] == "exact"
    assert values["rows"] == 7
    assert all(
        m["scope"] == {"perimeter": "dataset", "value": "ds"} for m in metrics
    )


def test_duplicate_metrics_on_an_empty_dataset():
    metrics = main.duplicate_metrics("ds", 0, 0, "exact")
    values = {m["key"]: m["value"] for m in metrics}
    assert values["score"] == "1.0"
    assert values["distinct_percent"] == "0.0"


def test_export_limit_is_clamped():
    assert main._export_limit({}) == main.DEFAULT_DUPLICATE_ROWS_LIMIT
    assert main._export_limit({"job": {"duplicate_rows_limit": 5}}) == 5
    assert main._export_limit({"job": {"duplicate_rows_limit": -1}}) == 0
    assert (
        main._export_limit({"job": {"duplicate_rows_limit": 10**9}})
        == main.MAX_DUPLICATE_ROWS_LIMIT
    )
    assert (
        main._export_limit({"job": {"duplicate_rows_limit": "oops"}})
        == main.DEFAULT_DUPLICATE_ROWS_LIMIT
    )


def test_write_excel_writes_a_readable_file(tmp_path):
    frame = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = str(tmp_path / "report.xlsx")
    written = main.write_excel(frame, path)
    assert written.endswith((".xlsx", ".csv"))
    assert len(open(written, "rb").read()) > 0


def test_counts_are_stable_across_chunk_boundaries(tmp_path):
    """The bug this rewrite closes: parts 2..N used to be dropped."""
    left = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    right = pl.DataFrame({"a": [1, 3], "b": ["x", "z"]})
    paths = []
    for index, frame in enumerate((left, right)):
        path = tmp_path / f"src_part_{index}.parquet"
        frame.write_parquet(path)
        paths.append(str(path))

    lf = pl.scan_parquet(paths)
    total, duplicates, _ = main.duplicate_counts(lf, ["a", "b"], exact=True)
    assert total == 5
    # (1, x) appears 3 times across the two parts.
    assert duplicates == 2


def test_exact_counts_agree_with_the_shared_aggregator():
    """Pins this pack to the qalita_core.aggregation definition of a duplicate.

    The pack does the group-by itself because it also needs the distinct count
    and the approximate mode; this check is what keeps the two from drifting.
    """
    from qalita_core.aggregation import DuplicateAggregator

    lf = make_lf()
    aggregator = DuplicateAggregator(["a", "b"])
    aggregator.add_lf(lf)

    total, duplicates, _ = main.duplicate_counts(lf, ["a", "b"], exact=True)
    assert (total, duplicates) == (
        aggregator.total_rows,
        aggregator.duplicate_count(),
    )
