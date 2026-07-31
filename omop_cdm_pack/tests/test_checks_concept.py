"""Tests for concept-level checks: plausibleGender,
plausibleGenderUseDescendants, plausibleUnitConceptIds.

Every param value used below mirrors what CONCEPT_CHECK_SPECS actually
produces from OMOP_CDMv5.4_Concept_Level.csv (verified with
``load_catalog('5.4')``), not the raw prose brief: ``params["value"]``
for the two gender checks is the literal string "Male" or "Female" --
never a gender concept id -- and ``params["conceptId"]`` for
plausibleGenderUseDescendants can be a comma-separated list of
ancestor concept ids (3 of the 4 real instances are multi-id), matching
concept_plausible_gender_use_descendants.sql's ``ancestor_concept_id
IN (@conceptId)`` versus concept_plausible_gender.sql's plain
``= @conceptId`` equality.
"""

import polars as pl
import pytest

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus
from tests.fixtures import write_mini_cdm


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="CONCEPT",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="characterization",
        kahn_category="Plausibility",
        description="d",
        param_items=tuple(sorted(params.items())),
    )
    return get_check(check_name)(ctx, instance)


# --- plausibleGender ---------------------------------------------------


def test_plausible_gender_flags_the_wrong_gender(mini_cdm):
    # concept 201826 is recorded for persons 1 (MALE 8507) and
    # 3 (MALE 8507); requiring Female makes every row a violation.
    # value is the literal string the CSV carries, per
    # concept_plausible_gender.sql's
    # ``{@plausibleGender == 'Male'} ? {8507} : {8532}`` ternary --
    # not a gender concept id.
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Female",
    )
    assert result.num_denominator_rows == 4
    assert result.num_violated_rows == 4


def test_plausible_gender_passes_for_the_right_gender(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 4


def test_plausible_gender_anything_but_male_means_female(mini_cdm):
    # concept_plausible_gender.sql's ternary has no real "else Female"
    # validation -- any value that is not literally 'Male' takes the
    # 8532 branch. Confirm the pack reproduces that default-to-female
    # behaviour rather than treating an unrecognised value as
    # NOT_APPLICABLE.
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Female",
    )
    same_as_bogus = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="not-a-real-gender",
    )
    assert same_as_bogus.num_violated_rows == result.num_violated_rows


def test_unknown_concept_yields_an_empty_denominator(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="123456789",
        value="Male",
    )
    assert result.num_denominator_rows == 0
    assert result.num_violated_rows == 0


def test_plausible_gender_null_gender_is_not_a_violation(tmp_path):
    # SQL's ``p.gender_concept_id <> 8507`` is UNKNOWN, not TRUE, when
    # gender_concept_id IS NULL -- so a NULL-gender person's row must
    # never be counted as violated. This cannot be exercised by
    # mini_cdm's stock data (no CONDITION_OCCURRENCE row references
    # person_id 4, the one NULL-gender person in PERSON), so it is
    # built locally.
    paths = write_mini_cdm(str(tmp_path))
    condition = pl.DataFrame(
        {
            "condition_occurrence_id": [900],
            "person_id": [4],  # NULL gender_concept_id in PERSON
            "condition_concept_id": [201826],
            "condition_start_date": ["2015-01-01"],
            "condition_end_date": ["2015-01-02"],
            "condition_source_value": ["Z"],
            "visit_occurrence_id": [None],
        },
        schema_overrides={"condition_concept_id": pl.Int64},
    ).with_columns(
        pl.col("condition_start_date").str.to_date(),
        pl.col("condition_end_date").str.to_date(),
    )
    extra_path = str(tmp_path / "condition_occurrence_extra.parquet")
    condition.write_parquet(extra_path)
    paths["CONDITION_OCCURRENCE"] = paths["CONDITION_OCCURRENCE"] + [
        extra_path
    ]
    ctx = CdmContext.from_paths(paths)

    result = _run(
        ctx,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    # 4 stock rows (2 MALE persons, compliant) + 1 new NULL-gender row.
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 0


def test_plausible_gender_denominator_ignores_orphan_person_id(tmp_path):
    # concept_plausible_gender.sql's denominator subquery has NO JOIN
    # to person at all (``SELECT COUNT_BIG(*) ... WHERE @cdmFieldName
    # = @conceptId``); only the violated-rows subquery joins PERSON,
    # and it is an INNER JOIN. So a row referencing a person_id absent
    # from PERSON inflates the denominator but can never be counted as
    # violated (it never survives the inner join).
    paths = write_mini_cdm(str(tmp_path))
    condition = pl.DataFrame(
        {
            "condition_occurrence_id": [901],
            "person_id": [999],  # does not exist in PERSON
            "condition_concept_id": [201826],
            "condition_start_date": ["2015-01-01"],
            "condition_end_date": ["2015-01-02"],
            "condition_source_value": ["Z"],
            "visit_occurrence_id": [None],
        },
        schema_overrides={"condition_concept_id": pl.Int64},
    ).with_columns(
        pl.col("condition_start_date").str.to_date(),
        pl.col("condition_end_date").str.to_date(),
    )
    extra_path = str(tmp_path / "condition_occurrence_orphan.parquet")
    condition.write_parquet(extra_path)
    paths["CONDITION_OCCURRENCE"] = paths["CONDITION_OCCURRENCE"] + [
        extra_path
    ]
    ctx = CdmContext.from_paths(paths)

    result = _run(
        ctx,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    # 4 stock rows + the orphan row = 5 in the denominator, but the
    # orphan can never appear in the violated count.
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 0


def test_plausible_gender_is_not_applicable_without_person(tmp_path):
    paths = write_mini_cdm(str(tmp_path))
    del paths["PERSON"]
    ctx = CdmContext.from_paths(paths)
    result = _run(
        ctx,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


# --- plausibleGenderUseDescendants -------------------------------------


def test_plausible_gender_use_descendants_needs_concept_ancestor(
    mini_cdm_no_vocabulary,
):
    # mini_cdm_no_vocabulary strips CONCEPT and CONCEPT_ANCESTOR but
    # keeps PERSON -- this specifically exercises the "no
    # CONCEPT_ANCESTOR" NOT_APPLICABLE path, not "no PERSON" (the
    # brief conflated the two under one test name).
    result = _run(
        mini_cdm_no_vocabulary,
        "plausibleGenderUseDescendants",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_plausible_gender_with_descendants_needs_the_ancestor_table(
    mini_cdm,
):
    result = _run(
        mini_cdm,
        "plausibleGenderUseDescendants",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="Male",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 4


def test_plausible_gender_use_descendants_accepts_a_concept_id_list(
    mini_cdm,
):
    # concept_plausible_gender_use_descendants.sql's WHERE clause is
    # ``ca.ancestor_concept_id IN (@conceptId)`` -- a real IN list,
    # not a single equality like plausibleGender's. 3 of the 4 real
    # catalog instances carry a comma-separated conceptId. A second,
    # never-matching id in the list must not change the result: if the
    # implementation regresses to single-int parsing (as the original
    # brief's _concept_id did), this raises/degrades to
    # NOT_APPLICABLE instead of matching.
    result = _run(
        mini_cdm,
        "plausibleGenderUseDescendants",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826, 987654321",
        value="Male",
    )
    assert result.status != CheckStatus.NOT_APPLICABLE
    assert result.num_denominator_rows == 4
    assert result.num_violated_rows == 0


# --- plausibleUnitConceptIds --------------------------------------------


def test_plausible_unit_concept_ids_is_not_applicable_without_the_column(
    mini_cdm,
):
    result = _run(
        mini_cdm,
        "plausibleUnitConceptIds",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8840",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


@pytest.fixture
def measurement_ctx(tmp_path):
    """A MEASUREMENT table with unit_concept_id, built locally --
    mini_cdm has no unit_concept_id column anywhere.

    Rows for concept 100:
      1: unit=10   (allowed)                  -> not violated, in denom
      2: unit=20   (not allowed)               -> violated, in denom
      3: unit=NULL                             -> not violated, in denom
      4: unit=0    (sentinel, deferred check)  -> excluded entirely
    Row for a different concept (999) is excluded by the conceptId
    filter regardless of its unit.
    """
    measurement = pl.DataFrame(
        {
            "measurement_id": [1, 2, 3, 4, 5],
            "measurement_concept_id": [100, 100, 100, 100, 999],
            "unit_concept_id": [10, 20, None, 0, 20],
        },
        schema_overrides={
            "measurement_concept_id": pl.Int64,
            "unit_concept_id": pl.Int64,
        },
    )
    path = str(tmp_path / "measurement_part_0.parquet")
    measurement.write_parquet(path)
    return CdmContext.from_paths({"MEASUREMENT": [path]})


def test_plausible_unit_concept_ids_null_unit_counts_in_denominator_not_violation(
    measurement_ctx,
):
    # concept_plausible_unit_concept_ids.sql's denominator predicate
    # is ``unit_concept_id != 0 OR unit_concept_id IS NULL`` -- NULL
    # is explicitly folded INTO the denominator, unlike the violated-
    # rows subquery's ``unit_concept_id IS NOT NULL`` guard, which
    # excludes it from ever being a violation. Excluding NULL from the
    # denominator (as the original brief's
    # ``unit_concept_id.is_not_null()`` pre-filter did) would give a
    # denominator of 2, not 3.
    result = _run(
        measurement_ctx,
        "plausibleUnitConceptIds",
        "MEASUREMENT",
        "measurement_concept_id",
        conceptId="100",
        value="10",
    )
    assert result.num_denominator_rows == 3
    assert result.num_violated_rows == 1


def test_plausible_unit_concept_ids_zero_is_excluded_from_the_denominator(
    measurement_ctx,
):
    # Row 4 (unit=0) must never appear in the denominator or the
    # violated count -- it is reserved for
    # standardConceptRecordCompleteness per the SQL's own comment.
    # This is implied by the 3-row denominator above (5 rows share
    # concept 100 and 999; concept 100 alone is 4 rows; only 3 land in
    # the denominator), stated explicitly here as its own guard.
    result = _run(
        measurement_ctx,
        "plausibleUnitConceptIds",
        "MEASUREMENT",
        "measurement_concept_id",
        conceptId="100",
        value="10,20",  # both non-zero units now "allowed"
    )
    # unit=0 is still excluded from the denominator even when nothing
    # else is a violation.
    assert result.num_denominator_rows == 3
    assert result.num_violated_rows == 0


def test_plausible_unit_concept_ids_minus_one_sentinel_flags_any_nonzero_unit(
    tmp_path,
):
    # When plausibleUnitConceptIds is the literal string "-1" (meaning
    # "no unit is plausible"), concept_plausible_unit_concept_ids.sql
    # takes a different branch: ANY non-null, non-zero unit is a
    # violation, including a unit_concept_id of -1 itself -- the
    # general ``NOT IN (list, 0)`` branch would instead treat -1 as an
    # allowed member of a list containing it.
    measurement = pl.DataFrame(
        {
            "measurement_id": [1, 2, 3],
            "measurement_concept_id": [200, 200, 200],
            "unit_concept_id": [-1, 0, None],
        },
        schema_overrides={
            "measurement_concept_id": pl.Int64,
            "unit_concept_id": pl.Int64,
        },
    )
    path = str(tmp_path / "measurement_part_0.parquet")
    measurement.write_parquet(path)
    ctx = CdmContext.from_paths({"MEASUREMENT": [path]})

    result = _run(
        ctx,
        "plausibleUnitConceptIds",
        "MEASUREMENT",
        "measurement_concept_id",
        conceptId="200",
        value="-1",
    )
    # unit=0 excluded from denominator; unit=NULL in denominator, not
    # violated; unit=-1 in denominator AND violated (non-zero).
    assert result.num_denominator_rows == 2
    assert result.num_violated_rows == 1
