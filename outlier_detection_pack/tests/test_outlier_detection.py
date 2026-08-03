"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Tests for the streaming IQR / z-score outlier detector.

Every case is built from a parquet file rather than an in-memory frame: the
pack reads parquet in production, and scanning it is what exercises the lazy
path the streaming engine actually takes.
"""

import polars as pl
import pytest

import main


@pytest.fixture
def dataset(tmp_path):
    """100 values in [0, 99], one obvious high outlier, one constant column.

    ``with_null`` also carries nulls, so the null handling of both the counting
    pass and the example rows is covered by the same file. Its own outlier sits
    on row 50 while ``value``'s sits on row 100, which keeps the row-level
    count (2) distinguishable from a per-column sum that happened to collide.
    """
    values = list(range(100)) + [10_000]
    frame = pl.DataFrame(
        {
            "id": list(range(101)),
            "value": values,
            "flat": [7] * 101,
            "with_null": (
                [None] * 50 + [-5_000.0] + [float(i) for i in range(1, 51)]
            ),
            "label": [f"row-{i}" for i in range(101)],
        }
    )
    path = tmp_path / "data.parquet"
    frame.write_parquet(path)
    return path


@pytest.fixture
def lf(dataset):
    return pl.scan_parquet(dataset)


@pytest.fixture
def schema(lf):
    return dict(lf.collect_schema())


def _metric(metrics, key, column=None):
    for metric in metrics:
        if metric["key"] != key:
            continue
        scope = metric["scope"]
        if column is None and scope["perimeter"] == "dataset":
            return metric["value"]
        if column is not None and scope.get("value") == column:
            return metric["value"]
    raise AssertionError(f"metric {key!r} (column={column!r}) not emitted")


@pytest.mark.parametrize("exact", [True, False])
def test_iqr_isolates_the_planted_outlier(lf, exact):
    results = main.detect(lf, ["value", "flat"], method="iqr", exact=exact)

    lower, upper = main.fences(results)["value"]
    # q1 ~ 25, q3 ~ 75, so the fence lands well inside the plain range and far
    # below the planted 10 000.
    assert 0.0 < upper < 200.0
    assert lower < 0.0

    assert results["value"]["outlier_count"] == 1
    assert results["value"]["non_null"] == 101
    assert results["value"]["bounds_method"] == (
        "exact" if exact else "histogram"
    )
    # A constant column has no spread, so it gets no fence and no outlier.
    assert "flat" not in main.fences(results)
    assert results["flat"]["outlier_count"] == 0
    assert results["flat"]["normality_score"] == 1.0


def test_zscore_bounds_and_counts(lf):
    results = main.detect(lf, ["value"], method="zscore", zscore_threshold=3.0)
    assert results["value"]["bounds_method"] == "exact"
    lower, upper = main.fences(results)["value"]
    assert lower < 0.0 < upper < 10_000.0
    assert results["value"]["outlier_count"] == 1
    assert results["value"]["non_null"] == 101


def test_zscore_threshold_widens_the_fence(lf):
    tight = main.detect(lf, ["value"], method="zscore", zscore_threshold=1.0)
    wide = main.detect(lf, ["value"], method="zscore", zscore_threshold=20.0)
    assert tight["value"]["outlier_count"] >= 1
    # 10 000 sits ~10 standard deviations out, so a 20-sigma fence clears it.
    assert wide["value"]["outlier_count"] == 0


def test_nulls_are_not_outliers(lf):
    stats = main.detect(lf, ["with_null"], method="iqr")["with_null"]
    assert stats["non_null"] == 51
    assert stats["outlier_count"] == 1


def test_zero_spread_column_gets_no_fence(tmp_path):
    """A column with a zero IQR is skipped, planted outlier included.

    Tukey fences on a zero spread collapse onto the quartile itself, which
    would flag every value that is not exactly it. `streaming_outliers` skips
    the column instead, so the extreme value below goes unreported. This is a
    real blind spot of the exact IQR path, not an accident — it is asserted so
    it cannot change silently.
    """
    frame = pl.DataFrame({"a": [1.0] * 50 + [-5_000.0]})
    path = tmp_path / "spike.parquet"
    frame.write_parquet(path)
    lazy = pl.scan_parquet(path)

    results = main.detect(lazy, ["a"], method="iqr", exact=True)
    assert main.fences(results) == {}
    assert results["a"]["outlier_count"] == 0
    # The z-score path has no such blind spot: the standard deviation is not 0.
    zscore = main.detect(lazy, ["a"], method="zscore", exact=True)
    assert zscore["a"]["outlier_count"] == 1


def test_unknown_method_raises(lf):
    with pytest.raises(ValueError, match="unknown outlier method"):
        main.detect(lf, ["value"], method="isolation-forest")


def test_analyze_dataset_metrics(lf, schema):
    metrics, recommendations = main.analyze_dataset(
        lf,
        "sales",
        schema=schema,
        id_columns=["id"],
        method="iqr",
        exact=True,
        example_rows=5,
    )

    assert _metric(metrics, "n") == 101
    assert _metric(metrics, "outliers", "value") == 1
    assert _metric(metrics, "outliers", "flat") == 0
    assert _metric(metrics, "normality_score", "value") == round(
        1 - 1 / 101, 2
    )
    assert _metric(metrics, "normality_score", "flat") == 1.0
    assert _metric(metrics, "normality_score_method", "value") == "exact"
    assert _metric(metrics, "outliers_method", "value") == "exact"
    assert _metric(metrics, "outlier_method") == "iqr"

    # 'value' and 'with_null' each contribute one outlier, on two different
    # rows, so the row-level count is 2 and the column total is 2.
    assert _metric(metrics, "total_outliers_count") == 2
    assert _metric(metrics, "outliers") == 2
    assert _metric(metrics, "outlier_rows") == 2
    assert _metric(metrics, "normality_score_dataset") == round(1 - 2 / 101, 2)
    assert _metric(metrics, "score") == str(round(1 - 2 / 101, 2))

    # The id column is excluded from detection even though it is numeric, and
    # the string column is never analysed at all.
    for ignored in ("id", "label"):
        with pytest.raises(AssertionError):
            _metric(metrics, "outliers", ignored)

    assert all(rec["type"] == "Outliers" for rec in recommendations)
    assert any("has 1 outliers" in rec["content"] for rec in recommendations)


def test_outliers_table_is_bounded_and_labelled(lf, schema):
    metrics, _ = main.analyze_dataset(
        lf,
        "sales",
        schema=schema,
        id_columns=["id"],
        method="iqr",
        exact=True,
        example_rows=1,
    )
    table = _metric(metrics, "outliers_table")
    assert table["columnLabels"] == [
        "index",
        "id",
        "OutlierAttribute",
        "value",
    ]
    # One example row was requested, and that row breaches exactly one fence.
    assert len(table["data"]) == 1
    row = table["data"][0]
    assert row[2]["value"] in {"value", "with_null"}
    assert row[3]["value"] in {10_000, -5_000.0}


def test_example_rows_are_capped(lf, schema):
    metrics, _ = main.analyze_dataset(
        lf,
        "sales",
        schema=schema,
        method="iqr",
        exact=True,
        example_rows=10**9,
    )
    table = _metric(metrics, "outliers_table")
    # Only two rows are outliers here; the point is that the request for a
    # billion rows is clamped rather than honoured.
    assert len(table["data"]) <= main.MAX_EXAMPLE_ROWS


def test_no_examples_when_disabled(lf, schema):
    metrics, _ = main.analyze_dataset(
        lf,
        "sales",
        schema=schema,
        method="iqr",
        exact=True,
        example_rows=0,
    )
    assert _metric(metrics, "outliers_table")["data"] == []
    # The count itself stays exact when the evidence is switched off.
    assert _metric(metrics, "outlier_rows") == 2


def test_clean_dataset_scores_one(tmp_path):
    frame = pl.DataFrame({"a": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]})
    path = tmp_path / "clean.parquet"
    frame.write_parquet(path)
    lazy = pl.scan_parquet(path)

    metrics, recommendations = main.analyze_dataset(
        lazy, "clean", schema=dict(lazy.collect_schema()), exact=True
    )
    assert _metric(metrics, "normality_score", "a") == 1.0
    assert _metric(metrics, "score") == "1.0"
    assert _metric(metrics, "outliers_table")["data"] == []
    assert len(recommendations) == 1


def test_chunked_object_is_one_dataset(tmp_path):
    """Parts of one object must be seen as a single frame.

    The zip(table_names, paths) idiom this replaces silently dropped parts
    2..N, which made the outlier count depend on how the source was chunked.
    """
    for part, offset in enumerate((0, 1000)):
        pl.DataFrame(
            {"a": [float(offset + i) for i in range(50)]}
        ).write_parquet(tmp_path / f"obj_part_{part}.parquet")
    lazy = pl.scan_parquet(sorted(str(p) for p in tmp_path.glob("*.parquet")))

    metrics, _ = main.analyze_dataset(
        lazy, "obj", schema=dict(lazy.collect_schema()), exact=True
    )
    assert _metric(metrics, "n") == 100


def test_all_null_column_is_skipped(tmp_path):
    frame = pl.DataFrame(
        {"a": [1.0, 2.0, 3.0], "empty": [None, None, None]},
        schema={"a": pl.Float64, "empty": pl.Float64},
    )
    path = tmp_path / "nulls.parquet"
    frame.write_parquet(path)
    lazy = pl.scan_parquet(path)

    assert "empty" not in main.fences(
        main.detect(lazy, ["a", "empty"], method="iqr")
    )

    metrics, _ = main.analyze_dataset(
        lazy, "nulls", schema=dict(lazy.collect_schema()), exact=True
    )
    assert _metric(metrics, "outliers", "empty") == 0
    assert _metric(metrics, "normality_score", "empty") == 1.0
