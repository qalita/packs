import polars as pl

import omop_dqd.checks  # noqa: F401  (registers implementations)
from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
        param_items=tuple(sorted(params.items())),
    )
    return get_check(check_name)(ctx, instance)


def test_cdm_field_passes_for_an_existing_column(mini_cdm):
    result = _run(mini_cdm, "cdmField", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_cdm_field_flags_a_missing_column(mini_cdm):
    result = _run(mini_cdm, "cdmField", "PERSON", "no_such_column")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_cdm_field_is_not_applicable_when_the_table_is_missing(mini_cdm):
    result = _run(mini_cdm, "cdmField", "DRUG_EXPOSURE", "anything")
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_is_required_counts_nulls(mini_cdm):
    # condition_concept_id has 1 NULL out of 6 rows
    result = _run(
        mini_cdm,
        "isRequired",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_is_required_passes_on_a_fully_populated_column(mini_cdm):
    result = _run(mini_cdm, "isRequired", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_measure_value_completeness_counts_nulls(mini_cdm):
    result = _run(
        mini_cdm,
        "measureValueCompleteness",
        "PERSON",
        "gender_concept_id",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 4


def test_source_value_completeness_pins_the_denominator_formula(mini_cdm):
    # Upstream counts DISTINCT source values whose companion standard-
    # concept field is 0, over a denominator of distinct non-null
    # source values plus one bucket for NULL (if any row has one).
    # condition_source_value has 5 distinct non-null values
    # {A, B, C, D, F} plus row 5's NULL -> denominator 6. No row's
    # condition_concept_id is 0, so there are zero unmapped values.
    result = _run(
        mini_cdm,
        "sourceValueCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_source_value",
        standardConceptFieldName="condition_concept_id",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 6


def test_source_value_completeness_counts_distinct_values_not_rows(
    tmp_path,
):
    # X is unmapped (concept 0) and appears twice; Y is unmapped and
    # appears once; Z is mapped. If the numerator counted rows rather
    # than distinct values, X's repeat would inflate it to 3.
    frame = pl.DataFrame(
        {
            "source_value": ["X", "X", "Y", "Z"],
            "concept_id": [0, 0, 0, 500],
        }
    )
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    result = _run(
        ctx,
        "sourceValueCompleteness",
        "FAKE_TABLE",
        "source_value",
        standardConceptFieldName="concept_id",
    )
    assert result.num_violated_rows == 2  # {X, Y}, not 3 rows
    assert result.num_denominator_rows == 3  # {X, Y, Z}, no NULLs


def test_is_primary_key_detects_the_duplicate(mini_cdm):
    # condition_occurrence_id 104 appears twice among 6 rows. Upstream
    # (field_is_primary_key.sql) counts EVERY row of a duplicated
    # group, not the excess beyond the first -- so a value seen twice
    # contributes 2 violations, not 1. Do not "fix" this back to 1.
    result = _run(
        mini_cdm,
        "isPrimaryKey",
        "CONDITION_OCCURRENCE",
        "condition_occurrence_id",
    )
    assert result.num_violated_rows == 2
    assert result.num_denominator_rows == 6


def test_is_primary_key_passes_on_a_unique_column(mini_cdm):
    result = _run(mini_cdm, "isPrimaryKey", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_plausible_value_low_counts_values_below_the_bound(mini_cdm):
    # years of birth are 1980, 1990, 2000, 1970 -> one below 1975
    result = _run(
        mini_cdm,
        "plausibleValueLow",
        "PERSON",
        "year_of_birth",
        value="1975",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 4


def test_plausible_value_high_counts_values_above_the_bound(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleValueHigh",
        "PERSON",
        "year_of_birth",
        value="1995",
    )
    assert result.num_violated_rows == 1


def test_plausible_start_before_end_detects_inverted_dates(mini_cdm):
    # condition_occurrence_id 101 starts 2015-06-01, ends 2015-05-01
    result = _run(
        mini_cdm,
        "plausibleStartBeforeEnd",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleStartBeforeEndFieldName="condition_end_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_cdm_datatype_passes_for_a_matching_integer(mini_cdm):
    # Upstream is a per-row content check with denominator COUNT(*)
    # over the whole table, not a schema check with denominator 1. An
    # integer-dtype column can hold no violation by construction, but
    # the denominator is still the PERSON row count (4).
    result = _run(
        mini_cdm, "cdmDatatype", "PERSON", "person_id", value="integer"
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 4


def test_cdm_datatype_flags_non_numeric_and_fractional_strings(
    tmp_path,
):
    # Declaring person_id as varchar(50) is no longer meaningful: the
    # real catalog only ever instantiates cdmDatatype for fields
    # declared "integer" (evaluationFilter cdmDatatype=='integer').
    # So this exercises the actual upstream semantics instead: a
    # string column holding some values that parse as whole numbers
    # and some that don't (non-numeric, or numeric with a decimal
    # point). NULLs are excluded from the violation count but still
    # counted in the denominator.
    frame = pl.DataFrame({"value_field": ["123", "abc", "45.6", None, "78"]})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    result = _run(
        ctx, "cdmDatatype", "FAKE_TABLE", "value_field", value="integer"
    )
    assert result.num_violated_rows == 2  # "abc" and "45.6"
    assert result.num_denominator_rows == 5
