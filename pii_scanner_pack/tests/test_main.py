"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import polars as pl

import main
from conftest import write_parts

EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
SSN = r"\b\d{3}-\d{2}-\d{4}\b"


def _conf(patterns=None, **job):
    base = {
        "pii_patterns": (
            patterns
            if patterns is not None
            else [
                {"key": "email", "regex": EMAIL},
                {"key": "usa_ssn", "regex": SSN},
            ]
        )
    }
    base.update(job)
    return {"job": base}


def _by_key(pack, key):
    return [item for item in pack.metrics.data if item["key"] == key]


def _clean(rows):
    return pl.DataFrame(
        {
            "note": [f"row {i}" for i in range(rows)],
            "contact": ["-"] * rows,
        }
    )


def _dirty(rows):
    return pl.DataFrame(
        {
            "note": [f"ssn 123-45-678{i % 10}" for i in range(rows)],
            "contact": [f"user{i}@example.com" for i in range(rows)],
        }
    )


# --------------------------------------------------------------------------
# the bug being fixed
# --------------------------------------------------------------------------


def test_multi_part_source_is_fully_scanned(make_pack, tmp_path):
    """Regression: parts 2..N are scanned too.

    Part 1 is clean, so a pack that zipped the table-name list against the
    parquet parts (and therefore kept only the first) reports no PII at all.
    """
    parts = write_parts(
        tmp_path / "d",
        "csv_customers",
        [_clean(100), _dirty(100), _dirty(100)],
    )
    pack = make_pack(_conf(), {"csv_customers": parts})
    main.run(pack)

    assert _by_key(pack, "rows")[0]["value"] == 300
    assert _by_key(pack, "pii_rows")[0]["value"] == 200
    assert _by_key(pack, "pii_records_ratio")[0]["value"] == "0.6667"
    assert _by_key(pack, "pii_columns")[0]["value"] == "2"


def test_hits_are_counted_per_column_and_per_pattern(make_pack, tmp_path):
    parts = write_parts(tmp_path / "d", "csv_customers", [_dirty(10)])
    pack = make_pack(_conf(), {"csv_customers": parts})
    main.run(pack)

    per_pattern = {
        (item["key"], item["scope"]["value"]): item["value"]
        for item in pack.metrics.data
        if item["key"].startswith("pii_hits_")
    }
    assert per_pattern[("pii_hits_usa_ssn", "note")] == 10
    assert per_pattern[("pii_hits_email", "contact")] == 10
    assert ("pii_hits_email", "note") not in per_pattern

    totals = {
        item["scope"]["value"]: item["value"]
        for item in _by_key(pack, "pii_hits")
    }
    assert totals == {"note": 10, "contact": 10}


def test_a_row_matching_several_patterns_counts_once(make_pack, tmp_path):
    """The ratio counts ROWS, which is what the discarded index set was for."""
    frame = pl.DataFrame(
        {
            "note": ["ssn 123-45-6789", "nothing"],
            "contact": ["a@b.com", "nothing"],
        }
    )
    parts = write_parts(tmp_path / "d", "csv_customers", [frame])
    pack = make_pack(_conf(), {"csv_customers": parts})
    main.run(pack)

    assert _by_key(pack, "pii_rows")[0]["value"] == 1
    assert _by_key(pack, "rows")[0]["value"] == 2
    assert _by_key(pack, "pii_records_ratio")[0]["value"] == "0.5"


def test_rows_with_pii_is_exact_across_column_batches(monkeypatch, tmp_path):
    """More columns than one batch: per-batch 'any' counts overlap.

    Summing them would double-count a row whose PII sits in two batches, so the
    exact answer has to come from a reduction over every column.
    """
    monkeypatch.setattr(main, "COLUMN_BATCH", 1)
    frame = pl.DataFrame({"a": ["x@y.com", "-"], "b": ["p@q.com", "-"]})
    total, rows_with_pii, hits = main._scan_dataset(
        frame.lazy(), ["a", "b"], [("email", EMAIL)]
    )
    assert (total, rows_with_pii) == (2, 1)
    assert hits == {("a", "email"): 1, ("b", "email"): 1}


# --------------------------------------------------------------------------
# the rule this pack exists under: never export a failing row
# --------------------------------------------------------------------------


def test_no_metric_ever_carries_a_row(make_pack, tmp_path):
    """A failing row in a PII scan IS the personal data. Counts only."""
    parts = write_parts(tmp_path / "d", "csv_customers", [_dirty(50)])
    pack = make_pack(_conf(examples=1000), {"csv_customers": parts})
    main.run(pack)

    for item in pack.metrics.data:
        assert "example" not in item["key"]
        assert "sample" not in item["key"]
        assert "@example.com" not in str(item["value"])
        assert "123-45" not in str(item["value"])
    for item in pack.recommendations.data:
        assert "@example.com" not in item["content"]
        assert "123-45" not in item["content"]


def test_the_pack_never_calls_the_failure_row_helper(make_pack, tmp_path):
    from qalita_core import analytics

    def _forbidden(*args, **kwargs):
        raise AssertionError(
            "pii_scanner_pack must never emit failing rows: they are PII"
        )

    parts = write_parts(tmp_path / "d", "csv_customers", [_dirty(20)])
    pack = make_pack(_conf(), {"csv_customers": parts})
    original = analytics.failures
    analytics.failures = _forbidden
    try:
        main.run(pack)
    finally:
        analytics.failures = original


# --------------------------------------------------------------------------
# pattern handling
# --------------------------------------------------------------------------


def test_unsupported_regex_is_skipped_not_fatal(caplog):
    patterns = [
        {"key": "lookahead", "regex": r"(?=secret)"},
        {"key": "email", "regex": EMAIL},
    ]
    assert main._supported_patterns(patterns) == [("email", EMAIL)]
    assert "lookahead" in caplog.text


def test_patterns_without_a_key_or_regex_are_ignored():
    assert main._supported_patterns([{"key": "x"}, {"regex": "y"}]) == []


def test_non_string_columns_are_matched_as_text(make_pack, tmp_path):
    frame = pl.DataFrame({"zip": [75001, 12345, 7]})
    parts = write_parts(tmp_path / "d", "csv_customers", [frame])
    pack = make_pack(
        _conf(patterns=[{"key": "zip5", "regex": r"^\d{5}$"}]),
        {"csv_customers": parts},
    )
    main.run(pack)
    assert _by_key(pack, "pii_hits")[0]["value"] == 2


def test_no_pattern_configured_yields_a_zero_ratio(make_pack, tmp_path):
    parts = write_parts(tmp_path / "d", "csv_customers", [_dirty(10)])
    pack = make_pack(_conf(patterns=[]), {"csv_customers": parts})
    main.run(pack)
    assert _by_key(pack, "pii_records_ratio")[0]["value"] == "0.0"
    assert _by_key(pack, "pii_columns")[0]["value"] == "0"
    assert _by_key(pack, "rows")[0]["value"] == 10
