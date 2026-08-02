import polars as pl

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance, load_catalog
from omop_dqd.context import CdmContext
from omop_dqd.evaluate import EvaluatedCheck, evaluate
from omop_dqd.registry import get_check, register, registered_names
from omop_dqd.results import CheckStatus, counted
from omop_dqd.runner import _NotApplicableContext, _reclassify, run_checks


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


def _table_instance(check_name, table, **params):
    return CheckInstance(
        check_name=check_name,
        check_level="TABLE",
        cdm_table_name=table,
        cdm_field_name=None,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
        param_items=tuple(sorted(params.items())),
    )


def _concept_instance(check_name, table, field, concept_id, **params):
    return CheckInstance(
        check_name=check_name,
        check_level="CONCEPT",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
        param_items=tuple(
            sorted({**params, "conceptId": str(concept_id)}.items())
        ),
    )


# A deliberately naive check with no guard of its own: it never
# consults ctx at all, so nothing except the runner's own
# reclassification logic can make it NOT_APPLICABLE. Every real check
# in checks/*.py checks ctx.has_table/has_column before doing
# anything else, which makes several of the ordered rules below
# unreachable through those checks specifically (see the per-rule
# docstrings) -- this one isolates the runner's logic from that.
@register("bareCheckForTest")
def _bare_check_for_test(ctx, chk):
    return counted(0, 5)


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


def test_every_catalog_check_name_has_a_registered_implementation():
    """The registry and the catalog must name exactly the same checks.

    A dropped or misspelled @register decorator does not fail at
    import: run_checks() turns an unregistered name into a per-
    instance ERROR (see _run_one), so an entire check type can go
    dark while the run still "succeeds". Equally, a @register for a
    name no catalog row ever produces is dead code. Comparing the two
    sets catches both in one assertion.
    """
    catalog_names = {c.check_name for c in load_catalog("5.4")}
    # The registry is a process-global dict, and this very module
    # registers a check of its own to exercise the runner (see
    # `bareCheckForTest` below), so compare only what the production
    # check modules put there.
    implemented = {
        name
        for name in registered_names()
        if get_check(name).__module__.startswith("omop_dqd.checks")
    }
    assert implemented == catalog_names


def test_catalog_5_4_has_2535_instances():
    assert len(load_catalog("5.4")) == 2535


def test_catalog_5_3_has_2005_instances():
    assert len(load_catalog("5.3")) == 2005


# =====================================================================
# notApplicable reclassification (DQD's R/calculateNotApplicableStatus.R)
# =====================================================================
#
# Upstream computes pass/fail/error for every check in a batch first
# (R/evaluateThresholds.R's main loop), then makes a second, batch-
# wide pass -- .applyNotApplicable -- that can reclassify some of
# those results as notApplicable. The nine rules below are numbered
# to match docs/superpowers/plans/2026-07-31-omop-cdm-pack-revision-2.md
# and omop_dqd/runner.py's _reclassify, in their original evaluation
# order; earlier rules win.
#
# Several of these rules are, in this port, unreachable through any
# *currently registered* real check: checks/table_level.py and
# checks/field_level.py already guard on ctx.has_table/has_column (or,
# for measurePersonCompleteness, on both PERSON's and the checked
# table's presence) before computing anything, so those checks report
# their own NOT_APPLICABLE directly and never reach the runner's
# reclassification pass with a PASS/FAIL to reclassify in the first
# place. Where that is the case, the docstring below says so and the
# test either (a) exercises the *other* facet of the same rule that
# check-level guards do NOT cover, reached through run_checks() for
# real, or (b) calls the runner's private _reclassify directly to pin
# the runner's own code regardless of what any check currently does.
# Both are noted per rule.


def test_rule1_measure_person_completeness_ignores_table_emptiness():
    """Rule 1: measurePersonCompleteness -> NA iff its table is
    missing. Nothing else -- in particular, not the checked table
    being merely *empty* -- makes it NA.

    checks/table_level.py's measure_person_completeness() already
    short-circuits to its own NOT_APPLICABLE the instant the checked
    table is *missing*, so that half of rule 1 never reaches
    run_checks()'s reclassification pass in practice. What actually
    distinguishes rule 1 from letting this check fall through to the
    generic rules is the *empty*-table case: without rule 1's
    dedicated early return, an empty (but present) checked table would
    hit rule 6 ("table empty -> NA") on fallthrough. Pinned directly
    against _reclassify, which is exactly where that early return
    lives.
    """
    table = "VISIT_OCCURRENCE"
    cdm_table_instance = _table_instance("cdmTable", table)
    cdm_table_result = evaluate(cdm_table_instance, counted(0, 1))  # present
    mvc_instance = _instance(
        "measureValueCompleteness", table, "visit_occurrence_id"
    )
    mvc_result = evaluate(mvc_instance, counted(0, 0))  # table is empty
    lookups = _NotApplicableContext(
        [
            EvaluatedCheck(cdm_table_instance, cdm_table_result),
            EvaluatedCheck(mvc_instance, mvc_result),
        ]
    )
    mpc_instance = _table_instance("measurePersonCompleteness", table)
    # every PERSON row unmatched -> evaluate() makes this a real FAIL
    raw = evaluate(mpc_instance, counted(4, 4))
    assert raw.status == CheckStatus.FAIL  # sanity: not vacuously true
    reclassified = _reclassify(mpc_instance, raw, lookups)
    assert reclassified.status == CheckStatus.FAIL


def test_rule2_cdmtable_on_a_missing_table_stays_fail_never_na():
    """Rule 2 (cdmTable is never NA) also pins the required *ordering*:
    it must be checked -- and must return -- before rule 4's generic
    "table missing -> NA" is ever reached, even though cdmTable's own
    FAIL is exactly what populates the table_is_missing lookup that
    rule 4 reads. A self-referential case where only precedence saves
    cdmTable from marking itself not-applicable.

    Reachable through run_checks() for real: unlike every other check
    in this port, cdm_table() never shortcuts itself -- it always
    returns a raw counted() result, so its own FAIL genuinely reaches
    the reclassification pass.
    """
    ctx = CdmContext.from_paths({})  # no tables at all
    catalog = [
        _table_instance("cdmTable", "DRUG_EXPOSURE"),
        _instance("cdmField", "DRUG_EXPOSURE", "drug_exposure_id"),
        _instance(
            "measureValueCompleteness", "DRUG_EXPOSURE", "drug_exposure_id"
        ),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["cdmTable"].status == CheckStatus.FAIL


def test_rule3_cdmfield_on_a_missing_field_stays_fail_never_na(tmp_path):
    """Rule 3: cdmField -> NA iff its *table* is missing -- and, like
    rule 2, this pins ordering: cdmField must never fall through to
    rule 4's generic "field missing -> NA", even though its own FAIL
    (when the field is absent but the table is present) is exactly
    what populates the field_is_missing lookup rule 4 reads.

    Reachable through run_checks() for real: cdm_field() only
    shortcuts to its own NOT_APPLICABLE when the *table* is missing;
    when the table is present but the field is absent, it returns a
    raw counted() FAIL, which genuinely reaches the reclassification
    pass.
    """
    frame = pl.DataFrame({"other_col": [1, 2, 3]})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})
    catalog = [
        _table_instance("cdmTable", "FAKE_TABLE"),
        _instance("cdmField", "FAKE_TABLE", "missing_col"),
        _instance("measureValueCompleteness", "FAKE_TABLE", "other_col"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["cdmField"].status == CheckStatus.FAIL


def test_rule4_generic_check_na_when_table_missing():
    """Rule 4: any other check -> NA if its table (or field) is
    missing.

    Every real check in checks/*.py already guards on ctx.has_table
    before computing anything, so this rule is unreachable through any
    of them -- they all report their own NOT_APPLICABLE directly. Uses
    the module-level bareCheckForTest, which deliberately has no such
    guard, to exercise the runner's own rule through run_checks() for
    real rather than reaching into private functions.
    """
    ctx = CdmContext.from_paths({})  # DRUG_EXPOSURE does not exist
    catalog = [
        _table_instance("cdmTable", "DRUG_EXPOSURE"),
        _instance("cdmField", "DRUG_EXPOSURE", "drug_exposure_id"),
        _instance(
            "measureValueCompleteness", "DRUG_EXPOSURE", "drug_exposure_id"
        ),
        _instance("bareCheckForTest", "DRUG_EXPOSURE", "drug_exposure_id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["bareCheckForTest"].status == CheckStatus.NOT_APPLICABLE


def test_rule5_error_not_caused_by_missing_table_stays_error():
    """Rule 5: an error is not reclassified NA, even when its table is
    also missing -- errors stay errors. Implemented as a pre-filter in
    _apply_not_applicable_rules (only PASS/FAIL results ever reach
    _reclassify), pinned here through run_checks() with a table that
    is genuinely missing, so table_is_missing is true for real.
    """

    @register("explodingBareCheckForTest")
    def _explode(ctx, chk):
        raise RuntimeError("boom")

    ctx = CdmContext.from_paths({})
    catalog = [
        _table_instance("cdmTable", "DRUG_EXPOSURE"),
        _instance("cdmField", "DRUG_EXPOSURE", "drug_exposure_id"),
        _instance(
            "measureValueCompleteness", "DRUG_EXPOSURE", "drug_exposure_id"
        ),
        _instance(
            "explodingBareCheckForTest", "DRUG_EXPOSURE", "drug_exposure_id"
        ),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["explodingBareCheckForTest"].status == CheckStatus.ERROR


def test_rule6_generic_check_na_when_table_empty(tmp_path):
    """Rule 6: table empty -> NA, for any check on that table (here, a
    TABLE-level bare check with no field, to keep this test isolated
    from rule 8's field-empty branch, which would otherwise also fire
    on an entirely-empty table and mask rule 6 being removed).
    """
    frame = pl.DataFrame({"id": []}, schema={"id": pl.Int64})
    path = tmp_path / "empty_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})
    catalog = [
        _table_instance("cdmTable", "FAKE_TABLE"),
        _instance("cdmField", "FAKE_TABLE", "id"),
        _instance("measureValueCompleteness", "FAKE_TABLE", "id"),
        _table_instance("bareCheckForTest", "FAKE_TABLE"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["measureValueCompleteness"].num_denominator_rows == 0
    assert results["bareCheckForTest"].status == CheckStatus.NOT_APPLICABLE


def test_rule7_measure_value_completeness_never_na_from_its_own_emptiness(
    tmp_path,
):
    """Rule 7: measureValueCompleteness never becomes NA because its
    own field is empty -- it is the check that measures emptiness, so
    it must keep reporting for real. Without this rule, its own FAIL
    would satisfy rule 8's fieldIsEmpty condition against itself.
    """
    frame = pl.DataFrame({"id": [None, None, None]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})
    catalog = [
        _table_instance("cdmTable", "FAKE_TABLE"),
        _instance("cdmField", "FAKE_TABLE", "id"),
        _instance("measureValueCompleteness", "FAKE_TABLE", "id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["measureValueCompleteness"].status == CheckStatus.FAIL
    assert results["measureValueCompleteness"].num_violated_rows == 3


# --- rule 8, first half: field empty --------------------------------


def test_isprimarykey_on_a_wholly_null_field_passes_without_the_rule(
    tmp_path,
):
    """Non-vacuity groundwork for rule 8's field-empty branch.

    isPrimaryKey's denominator is the table's full row count, not the
    field's non-null count, so a 100%-NULL field does not zero out its
    denominator the way it does for e.g. plausibleValueLow. Polars'
    join treats NULL keys as never matching (unlike its group_by,
    which buckets them together), so the duplicate-detecting semi-join
    finds zero violations: called directly, bypassing evaluate() and
    the runner entirely, this check PASSES on data that should never
    be evaluated at all.
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


def test_rule8_field_empty_reclassifies_a_whole_table_check(tmp_path):
    frame = pl.DataFrame({"id": [None, None, None]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    catalog = [
        _table_instance("cdmTable", "FAKE_TABLE"),
        _instance("cdmField", "FAKE_TABLE", "id"),
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


def test_rule8_field_empty_rule_leaves_a_populated_field_alone(tmp_path):
    frame = pl.DataFrame({"id": [1, 1, 2]}, schema={"id": pl.Int64})
    path = tmp_path / "fake_table.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"FAKE_TABLE": [str(path)]})

    catalog = [
        _table_instance("cdmTable", "FAKE_TABLE"),
        _instance("cdmField", "FAKE_TABLE", "id"),
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


# --- rule 8, second half: concept missing ---------------------------


def test_rule8_concept_missing_when_own_denominator_is_zero(tmp_path):
    """Rule 8's concept branch: a CONCEPT-level check whose own
    denominator is 0 -- here, plausibleGender testing a concept id
    that appears nowhere in gender_concept_id -- is NA.

    plausibleGender's guard() only checks table/field/PERSON
    existence, not whether any row actually matches the concept id
    under test, so a non-matching concept id reaches evaluate() (and
    then the runner) as a genuine 0/0 PASS, not a self-reported
    NOT_APPLICABLE -- this branch is reachable through run_checks()
    for real.
    """
    frame = pl.DataFrame(
        {"person_id": [1, 2], "gender_concept_id": [8507, 8532]},
        schema={"person_id": pl.Int64, "gender_concept_id": pl.Int64},
    )
    path = tmp_path / "person.parquet"
    frame.write_parquet(path)
    ctx = CdmContext.from_paths({"PERSON": [str(path)]})

    catalog = [
        _table_instance("cdmTable", "PERSON"),
        _instance("cdmField", "PERSON", "gender_concept_id"),
        _instance("measureValueCompleteness", "PERSON", "gender_concept_id"),
        _concept_instance(
            "plausibleGender",
            "PERSON",
            "gender_concept_id",
            concept_id=999999,
            value="Male",
        ),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    assert results["plausibleGender"].status == CheckStatus.NOT_APPLICABLE


def test_rule9_normal_check_is_left_alone(mini_cdm):
    """Rule 9 (otherwise, not NA) is the implicit fallthrough -- there
    is no dedicated branch to delete, so this is a positive control
    rather than an alter/revert pin: a check with nothing wrong with
    its table or field must pass straight through the whole
    reclassification pass unmodified.
    """
    catalog = [
        _table_instance("cdmTable", "PERSON"),
        _instance("cdmField", "PERSON", "person_id"),
        _instance("measureValueCompleteness", "PERSON", "person_id"),
        _instance("isRequired", "PERSON", "person_id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(mini_cdm, catalog)
    }
    # person_id is fully populated in mini_cdm: a genuine pass.
    assert results["isRequired"].status == CheckStatus.PASS


def test_special_case_condition_era_completeness_na_when_occurrence_empty(
    tmp_path,
):
    """The measureConditionEraCompleteness special case, evaluated
    *instead of* the 9 ordered rules: NA when CONDITION_OCCURRENCE --
    a different table from its own CONDITION_ERA -- is missing or
    empty.

    checks/table_level.py's measure_condition_era_completeness()
    already handles CONDITION_OCCURRENCE being *missing*, so only the
    *empty* sub-case (present, zero rows) is reachable through
    run_checks() here.
    """
    condition_era = pl.DataFrame(
        {"person_id": [1], "condition_era_id": [1]},
        schema={"person_id": pl.Int64, "condition_era_id": pl.Int64},
    )
    condition_occurrence = pl.DataFrame(
        {
            "person_id": [],
            "condition_concept_id": [],
            "condition_occurrence_id": [],
        },
        schema={
            "person_id": pl.Int64,
            "condition_concept_id": pl.Int64,
            "condition_occurrence_id": pl.Int64,
        },
    )
    era_path = tmp_path / "condition_era.parquet"
    occ_path = tmp_path / "condition_occurrence.parquet"
    condition_era.write_parquet(era_path)
    condition_occurrence.write_parquet(occ_path)
    ctx = CdmContext.from_paths(
        {
            "CONDITION_ERA": [str(era_path)],
            "CONDITION_OCCURRENCE": [str(occ_path)],
        }
    )
    catalog = [
        _table_instance("cdmTable", "CONDITION_ERA"),
        _table_instance("cdmTable", "CONDITION_OCCURRENCE"),
        _instance("cdmField", "CONDITION_ERA", "person_id"),
        _instance("measureValueCompleteness", "CONDITION_ERA", "person_id"),
        _instance(
            "measureValueCompleteness",
            "CONDITION_OCCURRENCE",
            "condition_occurrence_id",
        ),
        _table_instance("measureConditionEraCompleteness", "CONDITION_ERA"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    result = results["measureConditionEraCompleteness"]
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "CONDITION_OCCURRENCE" in result.message


def test_gate_without_measure_value_completeness_leaves_results_alone():
    """.containsNAchecks: the whole reclassification pass runs only
    when the batch contains cdmTable, cdmField AND
    measureValueCompleteness. Here measureValueCompleteness is absent,
    so nothing is reclassified -- the bare check's raw PASS stands
    even though its table is missing, which rule 4 would otherwise
    catch.
    """
    ctx = CdmContext.from_paths({})  # DRUG_EXPOSURE does not exist
    catalog = [
        _table_instance("cdmTable", "DRUG_EXPOSURE"),
        _instance("cdmField", "DRUG_EXPOSURE", "drug_exposure_id"),
        _instance("bareCheckForTest", "DRUG_EXPOSURE", "drug_exposure_id"),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    # cdmTable itself still reports the missing table for real ...
    assert results["cdmTable"].status == CheckStatus.FAIL
    # ... but without measureValueCompleteness in the batch, nothing
    # gets reclassified.
    assert results["bareCheckForTest"].status == CheckStatus.PASS


def test_plausible_before_death_passes_when_death_is_present_but_empty(
    tmp_path,
):
    """Regression test for the scenario that motivated Revision 2.

    plausibleBeforeDeath's denominator comes from an INNER JOIN with
    DEATH -- a table *other than* its own CONDITION_OCCURRENCE. Its
    own guard() only checks that CONDITION_OCCURRENCE's table/field
    exist, and plausible_before_death() separately checks that DEATH
    exists at all -- but neither checks whether DEATH is *empty*. A
    present-but-empty DEATH table is entirely ordinary (a CDM extract
    with no recorded deaths), so this must PASS with a zero
    denominator, not become NOT_APPLICABLE.

    Before Revision 2, evaluate.py's blanket zero-denominator rule
    reported NOT_APPLICABLE here regardless of *why* the denominator
    was zero. No rule in the reclassification pass fires instead:
    rule 6 ("table empty") only ever looks at the check's *own* table
    (CONDITION_OCCURRENCE, which is healthy here) -- DEATH's
    emptiness is invisible to it, exactly as upstream's tableIsEmpty
    is scoped per check row's own cdmTableName, not a table it merely
    joins against. Only measureConditionEraCompleteness gets a
    dedicated foreign-table special case upstream; plausibleBeforeDeath
    does not.
    """
    person = pl.DataFrame(
        {"person_id": [1, 2]}, schema={"person_id": pl.Int64}
    )
    condition_occurrence = pl.DataFrame(
        {
            "condition_occurrence_id": [10, 11],
            "person_id": [1, 2],
            "condition_start_date": ["2020-01-01", "2020-02-01"],
        },
        schema={
            "condition_occurrence_id": pl.Int64,
            "person_id": pl.Int64,
            "condition_start_date": pl.Utf8,
        },
    ).with_columns(pl.col("condition_start_date").str.to_date())
    death = pl.DataFrame(
        {"person_id": [], "death_date": []},
        schema={"person_id": pl.Int64, "death_date": pl.Date},
    )

    person_path = tmp_path / "person.parquet"
    condition_path = tmp_path / "condition_occurrence.parquet"
    death_path = tmp_path / "death.parquet"
    person.write_parquet(person_path)
    condition_occurrence.write_parquet(condition_path)
    death.write_parquet(death_path)

    ctx = CdmContext.from_paths(
        {
            "PERSON": [str(person_path)],
            "CONDITION_OCCURRENCE": [str(condition_path)],
            "DEATH": [str(death_path)],
        }
    )

    catalog = [
        _table_instance("cdmTable", "CONDITION_OCCURRENCE"),
        _instance("cdmField", "CONDITION_OCCURRENCE", "condition_start_date"),
        _instance(
            "measureValueCompleteness",
            "CONDITION_OCCURRENCE",
            "condition_start_date",
        ),
        _instance(
            "plausibleBeforeDeath",
            "CONDITION_OCCURRENCE",
            "condition_start_date",
        ),
    ]
    results = {
        r.instance.check_name: r.result for r in run_checks(ctx, catalog)
    }
    result = results["plausibleBeforeDeath"]
    assert result.status == CheckStatus.PASS
    assert result.num_denominator_rows == 0
