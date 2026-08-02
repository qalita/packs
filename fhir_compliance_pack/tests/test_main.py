"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import json

import polars as pl

import main
from conftest import write_parts


JOB = {
    "resource_type": "Patient",
    "field_mappings": {
        "id": "id",
        "gender": "gender",
        "birthDate": "birthDate",
        "active": "active",
    },
    "required_fields": ["id"],
    "enums": {"gender": ["male", "female", "other", "unknown"]},
    "patterns": {"id": "^[A-Za-z0-9.-]{1,64}$"},
    "date_fields": ["birthDate"],
    "boolean_fields": ["active"],
}


def _conf(**job):
    merged = dict(JOB)
    merged.update(job)
    return {"job": merged}


def _by_key(pack, key):
    return [item for item in pack.metrics.data if item["key"] == key]


def _valid(rows, offset=0):
    return pl.DataFrame(
        {
            "id": [f"p-{i + offset}" for i in range(rows)],
            "gender": ["male", "female"] * (rows // 2),
            "birthDate": ["1980-01-02"] * rows,
            "active": ["true", "false"] * (rows // 2),
        }
    )


def _invalid(rows):
    return pl.DataFrame(
        {
            "id": ["bad id!"] * rows,
            "gender": ["martian"] * rows,
            "birthDate": ["not-a-date"] * rows,
            "active": ["maybe"] * rows,
        }
    )


def _rules(schema, job):
    return main._rules(schema, job)


def _check(frame, job):
    """Per-field violation counts for one small frame."""
    schema = dict(frame.lazy().collect_schema())
    violations, presence, _ = _rules(schema, job)
    stats = main._evaluate(frame.lazy(), violations, presence)
    return {
        field: int(stats[f"violated|{field}"] or 0) for field in violations
    }, int(stats.get("__invalid") or 0)


# --------------------------------------------------------------------------
# the bug being fixed
# --------------------------------------------------------------------------


def test_multi_part_source_is_fully_validated(make_pack, tmp_path):
    """Regression: part 1 is valid, parts 2 and 3 are not.

    A pack reading ``paths[0]`` scores this dataset a perfect 1.0.
    """
    parts = write_parts(
        tmp_path / "d",
        "csv_patients",
        [_valid(100), _invalid(100), _invalid(100)],
    )
    pack = make_pack(_conf(), {"csv_patients": parts})
    main.run(pack)

    assert _by_key(pack, "records")[0]["value"] == 300
    assert _by_key(pack, "invalid_records")[0]["value"] == 200
    assert _by_key(pack, "valid_records")[0]["value"] == 100
    assert _by_key(pack, "score")[0]["value"] == "0.33"
    assert _by_key(pack, "validity_ratio")[0]["value"] == "0.3333"


def test_completeness_is_the_share_of_populated_mapped_fields(
    make_pack, tmp_path
):
    frame = pl.DataFrame(
        {
            "id": ["a", "b"],
            "gender": ["male", None],
            "birthDate": ["1980-01-02", ""],
            "active": ["true", None],
        }
    )
    parts = write_parts(tmp_path / "d", "csv_patients", [frame])
    pack = make_pack(_conf(), {"csv_patients": parts})
    main.run(pack)
    # 5 populated cells out of 2 rows x 4 fields.
    assert _by_key(pack, "completeness")[0]["value"] == "0.625"


# --------------------------------------------------------------------------
# rules, one by one
# --------------------------------------------------------------------------


def test_required_field_flags_null_and_blank():
    frame = pl.DataFrame(
        {
            "id": ["a", None, "  "],
            "gender": ["male"] * 3,
            "birthDate": ["1980-01-02"] * 3,
            "active": ["true"] * 3,
        }
    )
    counts, invalid = _check(frame, JOB)
    assert counts["id"] == 2
    assert invalid == 2


def test_enum_flags_only_populated_values_outside_the_list():
    frame = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "gender": ["male", "martian", None],
            "birthDate": ["1980-01-02"] * 3,
            "active": ["true"] * 3,
        }
    )
    counts, _ = _check(frame, JOB)
    assert counts["gender"] == 1


def test_pattern_is_anchored_at_the_start_like_re_match():
    frame = pl.DataFrame(
        {
            "id": ["ok-1", "bad id!", "x" * 65],
            "gender": ["male"] * 3,
            "birthDate": ["1980-01-02"] * 3,
            "active": ["true"] * 3,
        }
    )
    counts, _ = _check(frame, JOB)
    assert counts["id"] == 2


def test_date_rule_rejects_impossible_calendar_dates():
    frame = pl.DataFrame(
        {
            "id": ["a", "b", "c", "d", "e"],
            "gender": ["male"] * 5,
            "birthDate": [
                "1980-01-02",
                "1990-12-31T10:00:00",
                "2023-02-30",
                "not-a-date",
                None,
            ],
            "active": ["true"] * 5,
        }
    )
    counts, _ = _check(frame, JOB)
    # Only the impossible date and the free text are violations; the null is an
    # absent optional value, not a malformed one.
    assert counts["birthDate"] == 2


def test_boolean_rule_accepts_the_usual_literals():
    frame = pl.DataFrame(
        {
            "id": ["a", "b", "c", "d"],
            "gender": ["male"] * 4,
            "birthDate": ["1980-01-02"] * 4,
            "active": ["TRUE", "no", "1", "maybe"],
        }
    )
    counts, _ = _check(frame, JOB)
    assert counts["active"] == 1


def test_a_native_boolean_column_is_accepted():
    frame = pl.DataFrame(
        {
            "id": ["a", "b"],
            "gender": ["male", "female"],
            "birthDate": ["1980-01-02"] * 2,
            "active": [True, False],
        }
    )
    counts, invalid = _check(frame, JOB)
    assert counts["active"] == 0
    assert invalid == 0


def test_a_native_date_column_is_accepted():
    frame = pl.DataFrame(
        {
            "id": ["a", "b"],
            "gender": ["male", "female"],
            "birthDate": pl.Series(["1980-01-02", "1990-12-31"]).str.to_date(),
            "active": ["true", "false"],
        }
    )
    counts, invalid = _check(frame, JOB)
    assert counts["birthDate"] == 0
    assert invalid == 0


def test_a_required_field_on_an_absent_column_fails_every_record():
    """A scalar null would have been counted once instead of once per row."""
    frame = pl.DataFrame({"other": [1, 2, 3, 4]})
    counts, invalid = _check(frame, JOB)
    assert counts["id"] == 4
    assert invalid == 4


def test_an_optional_field_on_an_absent_column_is_not_a_violation():
    frame = pl.DataFrame({"id": ["a", "b"]})
    counts, invalid = _check(frame, JOB)
    assert counts["gender"] == 0
    assert counts["birthDate"] == 0
    assert counts["active"] == 0
    assert invalid == 0


# --------------------------------------------------------------------------
# bounded evidence
# --------------------------------------------------------------------------


def test_invalid_examples_are_bounded(make_pack, tmp_path):
    parts = write_parts(tmp_path / "d", "csv_patients", [_invalid(400)])
    pack = make_pack(_conf(examples=5), {"csv_patients": parts})
    main.run(pack)

    payload = _by_key(pack, "invalid_record_examples")[0]["value"]
    rows = json.loads(payload)
    assert len(rows) == 5
    assert set(rows[0]) == {"id", "gender", "birthDate", "active"}


def test_examples_can_be_turned_off(make_pack, tmp_path):
    parts = write_parts(tmp_path / "d", "csv_patients", [_invalid(10)])
    pack = make_pack(_conf(examples=0), {"csv_patients": parts})
    main.run(pack)
    assert _by_key(pack, "invalid_record_examples") == []


def test_example_limit_is_capped():
    assert main._example_limit({"examples": 10**9}) == main.MAX_EXAMPLE_ROWS
    assert main._example_limit({"examples": "nope"}) == (
        main.DEFAULT_EXAMPLE_ROWS
    )


# --------------------------------------------------------------------------
# per-field reporting
# --------------------------------------------------------------------------


def test_per_field_metrics_are_scoped_to_the_mapped_column(
    make_pack, tmp_path
):
    parts = write_parts(tmp_path / "d", "csv_patients", [_invalid(10)])
    pack = make_pack(_conf(), {"csv_patients": parts})
    main.run(pack)

    violations = {
        item["scope"]["value"]: item["value"]
        for item in _by_key(pack, "field_violations")
    }
    assert violations == {
        "id": 10,
        "gender": 10,
        "birthDate": 10,
        "active": 10,
    }
