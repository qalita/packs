import omop_dqd.checks  # noqa: F401  (registers implementations)
from omop_dqd.catalog import CheckInstance
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


def test_source_value_completeness_treats_null_as_incomplete(mini_cdm):
    result = _run(
        mini_cdm,
        "sourceValueCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_source_value",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_is_primary_key_detects_the_duplicate(mini_cdm):
    # condition_occurrence_id 104 appears twice among 6 rows
    result = _run(
        mini_cdm,
        "isPrimaryKey",
        "CONDITION_OCCURRENCE",
        "condition_occurrence_id",
    )
    assert result.num_violated_rows == 1
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
    result = _run(
        mini_cdm, "cdmDatatype", "PERSON", "person_id", value="integer"
    )
    assert result.num_violated_rows == 0


def test_cdm_datatype_flags_a_mismatch(mini_cdm):
    result = _run(
        mini_cdm,
        "cdmDatatype",
        "PERSON",
        "person_id",
        value="varchar(50)",
    )
    assert result.num_violated_rows == 1
