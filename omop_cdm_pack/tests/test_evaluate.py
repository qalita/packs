import pytest
from dataclasses import replace

from omop_dqd.catalog import CheckInstance
from omop_dqd.evaluate import EvaluatedCheck, evaluate
from omop_dqd.registry import get_check, is_registered, register
from omop_dqd.results import CheckStatus, counted, errored, not_applicable


def _instance(threshold=0.0, check_name="isRequired"):
    return CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name="PERSON",
        cdm_field_name="person_id",
        threshold=threshold,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
    )


def test_pct_violated_rows_is_derived():
    assert counted(1, 4).pct_violated_rows == 25.0


def test_pct_violated_rows_is_zero_when_denominator_is_zero():
    assert counted(0, 0).pct_violated_rows == 0.0


def test_zero_threshold_fails_on_any_violation():
    result = evaluate(_instance(threshold=0.0), counted(1, 100))
    assert result.status == CheckStatus.FAIL


def test_zero_threshold_passes_with_no_violation():
    result = evaluate(_instance(threshold=0.0), counted(0, 100))
    assert result.status == CheckStatus.PASS


def test_non_zero_threshold_tolerates_violations_below_it():
    # 4 violations out of 100 is 4%, threshold is 5%
    result = evaluate(_instance(threshold=5.0), counted(4, 100))
    assert result.status == CheckStatus.PASS


def test_non_zero_threshold_fails_strictly_above_it():
    # 6 violations out of 100 is 6%, threshold is 5%
    result = evaluate(_instance(threshold=5.0), counted(6, 100))
    assert result.status == CheckStatus.FAIL


def test_threshold_boundary_is_inclusive():
    # exactly at the threshold passes, matching DQD
    result = evaluate(_instance(threshold=5.0), counted(5, 100))
    assert result.status == CheckStatus.PASS


def test_empty_denominator_is_not_applicable():
    result = evaluate(_instance(threshold=0.0), counted(0, 0))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_not_applicable_survives_evaluation():
    result = evaluate(_instance(), not_applicable("no vocabulary"))
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "vocabulary" in result.message


def test_registry_round_trip():
    @register("dummyCheckForTest")
    def _dummy(ctx, chk):
        return counted(0, 1)

    assert is_registered("dummyCheckForTest")
    assert get_check("dummyCheckForTest") is _dummy


def test_unregistered_check_raises():
    with pytest.raises(KeyError, match="noSuchCheck"):
        get_check("noSuchCheck")


def test_duplicate_registration_is_rejected():
    @register("duplicateCheckForTest")
    def _first(ctx, chk):
        return counted(0, 1)

    with pytest.raises(ValueError, match="duplicateCheckForTest"):

        @register("duplicateCheckForTest")
        def _second(ctx, chk):
            return counted(0, 1)


def test_error_survives_evaluation():
    result = evaluate(_instance(), errored("boom"))
    assert result.status == CheckStatus.ERROR
    assert "boom" in result.message


def test_error_is_not_recomputed_from_its_counts():
    # counts that would evaluate to PASS if the status were ignored
    poisoned = replace(
        errored("boom"), num_violated_rows=0, num_denominator_rows=100
    )
    assert evaluate(_instance(threshold=0.0), poisoned).status == (
        CheckStatus.ERROR
    )


def test_evaluated_check_pairs_an_instance_with_its_result():
    instance = _instance()
    evaluated = EvaluatedCheck(instance, counted(1, 4))
    assert evaluated.instance is instance
    assert evaluated.result.pct_violated_rows == 25.0
