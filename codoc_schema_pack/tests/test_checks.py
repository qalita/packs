"""Conformance checks of one table, on in-memory frames."""

import datetime as dt

import polars as pl
import pytest

from codoc_model.checks import (
    COLUMN_PRESENT,
    COLUMN_TYPE,
    MAX_LENGTH,
    NOT_NULL,
    PRIMARY_KEY,
    TABLE_PRESENT,
    UNEXPECTED_COLUMN,
    check_table,
    classify_type,
    family_of,
    missing_table,
)
from codoc_model.spec import load_model

MODEL = load_model()
SITE = MODEL.tables["dwh_thesaurus_site"]
# site_num bigint PK, site_code varchar(30) req, site_str varchar(400) req,
# instance_id bigint, upload_id bigint, update_date timestamptz


def conformant_site(**overrides):
    data = {
        "site_num": [1, 2, 3],
        "site_code": ["A", "B", "C"],
        "site_str": ["Site A", "Site B", "Site C"],
        "instance_id": [10, 10, None],
        "upload_id": [20250915000000] * 3,
        "update_date": [dt.datetime(2025, 9, 15, tzinfo=dt.timezone.utc)] * 3,
    }
    data.update(overrides)
    return pl.DataFrame(data).lazy()


def failing(result, check=None):
    return [
        (f.check, f.column)
        for f in result.findings
        if not f.passed and (check is None or f.check == check)
    ]


def test_conformant_table_scores_one():
    result = check_table(SITE, conformant_site(), strict_types=True)
    assert result.rows == 3
    assert failing(result) == []
    assert result.score == 1.0


def test_missing_table_scores_zero():
    result = missing_table(SITE)
    assert not result.present
    assert failing(result) == [(TABLE_PRESENT, None)]
    assert result.score == 0.0


def test_missing_and_unexpected_columns():
    frame = (
        conformant_site()
        .drop("site_str")
        .with_columns(pl.lit(1).alias("extra"))
    )
    result = check_table(SITE, frame)
    assert failing(result, COLUMN_PRESENT) == [(COLUMN_PRESENT, "site_str")]
    unexpected = [f for f in result.findings if f.check == UNEXPECTED_COLUMN]
    assert [f.column for f in unexpected] == ["extra"]
    assert not unexpected[0].scored  # reported, but never moves the score
    assert 0 < result.score < 1


def test_column_names_match_case_insensitively():
    frame = conformant_site().rename({"site_num": "SITE_NUM"})
    assert failing(check_table(SITE, frame)) == []


def test_required_column_with_nulls():
    result = check_table(SITE, conformant_site(site_code=["A", None, None]))
    [finding] = [f for f in result.findings if not f.passed]
    assert (finding.check, finding.column, finding.violations) == (
        NOT_NULL,
        "site_code",
        2,
    )


def test_primary_key_duplicates_and_nulls():
    result = check_table(SITE, conformant_site(site_num=[1, 1, None]))
    pk = [f for f in result.findings if f.check == PRIMARY_KEY]
    assert pk[0].violations == 2  # one duplicate, one null
    assert failing(result, NOT_NULL) == [(NOT_NULL, "site_num")]


def test_varchar_length_limit():
    result = check_table(
        SITE, conformant_site(site_code=["A", "x" * 31, "x" * 30])
    )
    [finding] = [
        f for f in result.findings if f.check == MAX_LENGTH and not f.passed
    ]
    assert (finding.column, finding.violations) == ("site_code", 1)


def test_text_that_converts_passes_unless_strict():
    as_text = conformant_site(
        site_num=["1", "2", "3"],
        update_date=["2025-09-15 00:00:00"] * 3,
    )
    assert failing(check_table(SITE, as_text, strict_types=False)) == []
    assert sorted(failing(check_table(SITE, as_text, strict_types=True))) == [
        (COLUMN_TYPE, "site_num"),
        (COLUMN_TYPE, "update_date"),
    ]


def test_text_timestamps_with_or_without_offset_convert():
    frame = conformant_site(
        update_date=["2025-09-15 00:00:00+02:00", "2025-09-15T08:00:00Z", None]
    )
    assert failing(check_table(SITE, frame)) == []
    frame = conformant_site(update_date=["2025-09-15 00:00:00", "never", None])
    assert failing(check_table(SITE, frame)) == [(COLUMN_TYPE, "update_date")]


def test_text_that_does_not_convert_fails():
    frame = conformant_site(site_num=["1", "two", None])
    result = check_table(SITE, frame, strict_types=False)
    [finding] = failing(result, COLUMN_TYPE)
    assert finding == (COLUMN_TYPE, "site_num")
    detail = [
        f for f in result.findings if f.check == COLUMN_TYPE and not f.passed
    ]
    assert detail[0].violations == 1


def test_wrong_type_is_a_mismatch():
    frame = conformant_site(update_date=[True, False, True])
    assert failing(check_table(SITE, frame)) == [(COLUMN_TYPE, "update_date")]


def test_all_null_column_says_nothing_about_type():
    frame = conformant_site(instance_id=[None, None, None])
    assert failing(check_table(SITE, frame, strict_types=True)) == []


def test_integral_floats_convert_to_integer():
    frame = conformant_site(instance_id=[10.0, 11.0, None])
    assert failing(check_table(SITE, frame)) == []
    frame = conformant_site(instance_id=[10.5, 11.0, None])
    assert failing(check_table(SITE, frame)) == [(COLUMN_TYPE, "instance_id")]


def test_extension_columns_only_required_when_enabled():
    data = MODEL.tables["dwh_data"]
    frame = pl.DataFrame(
        {c.name: [None] for c in data.expected_columns()}
    ).lazy()
    assert not failing(check_table(data, frame), COLUMN_PRESENT)
    missing = failing(
        check_table(data, frame, extensions=["pmsi"]), COLUMN_PRESENT
    )
    assert {column for _, column in missing} == {
        "activity_form",
        "intersectoriel_number",
        "residency_zip_code",
    }


def test_extension_columns_present_without_flag_are_checked_not_unexpected():
    data = MODEL.tables["dwh_data"]
    columns = {c.name: [None] for c in data.expected_columns()}
    columns["route"] = ["x" * 51]  # drugs extension, varchar(50)
    result = check_table(data, pl.DataFrame(columns).lazy())
    assert failing(result, UNEXPECTED_COLUMN) == []
    assert failing(result, MAX_LENGTH) == [(MAX_LENGTH, "route")]


@pytest.mark.parametrize(
    "dtype, family",
    [
        (pl.Int32, "integer"),
        (pl.Decimal(38, 0), "integer"),
        (pl.Decimal(10, 2), "float"),
        (pl.Float32, "float"),
        (pl.String, "string"),
        (pl.Datetime("us", "UTC"), "datetime"),
        (pl.Date, "date"),
        (pl.Boolean, "boolean"),
        (pl.Null, "null"),
        (pl.List(pl.Int64), "other"),
    ],
)
def test_family_of(dtype, family):
    assert family_of(dtype) == family


def test_classify_type():
    assert classify_type("float", "integer") == "match"
    assert classify_type("string", "integer") == "convertible"
    assert classify_type("integer", "string") == "convertible"
    assert classify_type("datetime", "boolean") == "mismatch"
