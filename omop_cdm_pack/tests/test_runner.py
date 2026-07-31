import polars as pl

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance, load_catalog
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check, register
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks


def _instance(check_name, table="PERSON", field="person_id", **params):
    return CheckInstance(
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


def test_runner_evaluates_each_instance(mini_cdm):
    results = run_checks(
        mini_cdm,
        [_instance("isRequired"), _instance("cdmField")],
    )
    assert len(results) == 2
    assert all(
        r.result.status
        in {
            CheckStatus.PASS,
            CheckStatus.FAIL,
            CheckStatus.NOT_APPLICABLE,
        }
        for r in results
    )


def test_runner_reports_a_crashing_check_as_error(mini_cdm):
    @register("explodingCheckForTest")
    def _explode(ctx, chk):
        raise RuntimeError("boom")

    results = run_checks(
        mini_cdm,
        [_instance("explodingCheckForTest"), _instance("isRequired")],
    )
    statuses = {r.instance.check_name: r.result.status for r in results}
    assert statuses["explodingCheckForTest"] == CheckStatus.ERROR
    # the run continued
    assert statuses["isRequired"] == CheckStatus.PASS


def test_runner_marks_unimplemented_checks_as_error(mini_cdm):
    results = run_checks(mini_cdm, [_instance("notImplementedCheck")])
    assert results[0].result.status == CheckStatus.ERROR
    assert "no implementation" in results[0].result.message.lower()


def test_runner_handles_the_full_catalog(mini_cdm):
    catalog = load_catalog("5.4")
    results = run_checks(mini_cdm, catalog)
    assert len(results) == len(catalog)
    # nothing may be left unevaluated
    assert all(r.result.status for r in results)


def test_full_catalog_run_produces_no_errors(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    errors = [r for r in results if r.result.status == CheckStatus.ERROR]
    assert not errors, [
        (r.instance.check_name, r.result.message) for r in errors[:10]
    ]


def test_catalog_5_4_has_2539_instances():
    assert len(load_catalog("5.4")) == 2539


def test_catalog_5_3_has_2021_instances():
    assert len(load_catalog("5.3")) == 2021


# --- notApplicable cross-check (DQD's R/calculateNotApplicableStatus.R) --
#
# Upstream computes pass/fail/error for every check in a batch first,
# then makes a second, batch-wide pass that can reclassify some of
# those results as notApplicable -- in particular, any check on a
# field that a sibling measureValueCompleteness check found to be
# 100% NULL (present, but carrying no data at all). Most of this
# port's checks already produce that outcome unassisted, because
# their own denominator is the field's non-null count (a 100%-NULL
# field yields a zero denominator, and evaluate() already turns a
# zero denominator into NOT_APPLICABLE). But checks whose denominator
# is the whole table -- isPrimaryKey among them -- do not: see the
# proof below.


def test_isprimarykey_on_a_wholly_null_field_passes_without_the_rule(
    tmp_path,
):
    """Non-vacuity proof for the field-empty cross-check.

    isPrimaryKey's denominator is the table's full row count, not the
    field's non-null count, so a 100%-NULL field does not zero out its
    denominator the way it does for e.g. plausibleValueLow. Polars'
    join treats NULL keys as never matching (unlike its group_by,
    which buckets them together), so the duplicate-detecting semi-join
    finds zero violations: called directly, bypassing the runner, this
    check PASSES on data that should never be evaluated at all.
    """
    frame = pl.DataFrame({"id": [None, None, None]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    raw = get_check("isPrimaryKey")(
        ctx, _instance("isPrimaryKey", "FAKE_TABLE", "id")
    )
    assert raw.num_violated_rows == 0
    assert raw.num_denominator_rows == 3


def test_field_empty_reclassifies_a_whole_table_check_as_not_applicable(
    tmp_path,
):
    frame = pl.DataFrame({"id": [None, None, None]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    catalog = [
        _instance("measureValueCompleteness", "FAKE_TABLE", "id"),
        _instance("isPrimaryKey", "FAKE_TABLE", "id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    # measureValueCompleteness itself reports the emptiness for real ...
    assert results["measureValueCompleteness"].status == CheckStatus.FAIL
    assert results["measureValueCompleteness"].num_violated_rows == 3
    # ... and that reclassifies the sibling check, which would
    # otherwise have silently passed (see the proof above).
    assert results["isPrimaryKey"].status == CheckStatus.NOT_APPLICABLE


def test_field_empty_rule_leaves_a_populated_field_alone(tmp_path):
    frame = pl.DataFrame({"id": [1, 1, 2]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    catalog = [
        _instance("measureValueCompleteness", "FAKE_TABLE", "id"),
        _instance("isPrimaryKey", "FAKE_TABLE", "id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["measureValueCompleteness"].status == CheckStatus.PASS
    # id=1 repeats: a real violation, not a reclassification.
    assert results["isPrimaryKey"].status == CheckStatus.FAIL
    assert results["isPrimaryKey"].num_violated_rows == 2
