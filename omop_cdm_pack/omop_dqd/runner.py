"""Execution of a check catalog against a CDM context.

Ports two layers of upstream's R/executeDqChecks.R pipeline:

  1. .runCheck / .processCheck -- run every check, catching whatever
     goes wrong with any single one so the batch keeps going.
  2. .evaluateThresholds' final step, .calculateNotApplicableStatus
     (R/calculateNotApplicableStatus.R) -- a *second*, batch-wide pass
     over every already-evaluated result that can reclassify some of
     them as notApplicable using facts only visible once the whole
     batch is in hand (e.g. "this table is empty, because the
     measureValueCompleteness check for one of its fields said so").

evaluate.py deliberately does not attempt any of this -- see its own
docstring for why applicability is this module's job, not its.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.evaluate import EvaluatedCheck, evaluate
from omop_dqd.registry import get_check, is_registered
from omop_dqd.results import CheckResult, CheckStatus, errored, not_applicable

logger = logging.getLogger(__name__)


def _group_by_table(
    catalog: List[CheckInstance],
) -> Dict[str, List[CheckInstance]]:
    """Group instances so each CDM table is handled together.

    Polars caches the parquet scan per table, so grouping keeps the
    working set small and the file handles few.
    """
    grouped = defaultdict(list)
    for instance in catalog:
        grouped[instance.cdm_table_name].append(instance)
    return grouped


def _run_one(ctx: CdmContext, instance: CheckInstance) -> EvaluatedCheck:
    if not is_registered(instance.check_name):
        return EvaluatedCheck(
            instance,
            errored(f"no implementation for check {instance.check_name!r}"),
        )
    try:
        raw = get_check(instance.check_name)(ctx, instance)
    except Exception as exc:  # noqa: BLE001 - one check must not stop the run
        logger.warning(
            "check %s failed on %s: %s",
            instance.check_name,
            instance.qualified_field,
            exc,
        )
        return EvaluatedCheck(instance, errored(str(exc)))
    return EvaluatedCheck(instance, evaluate(instance, raw))


# --- notApplicable reclassification --------------------------------
#
# Ported from R/calculateNotApplicableStatus.R's .applyNotApplicable,
# which upstream runs only after every check in the batch already has
# a raw PASS/FAIL/ERROR result (R/evaluateThresholds.R calls it from
# .evaluateThresholds, gated on .containsNAchecks). It needs the
# *whole* batch: whether a table or field is missing or empty is
# determined by looking at the cdmTable/cdmField/measureValueCompleteness
# results for that table or field elsewhere in the same run, never by
# anything the check itself computed.

_GATE_CHECK_NAMES = frozenset(
    {"cdmTable", "cdmField", "measureValueCompleteness"}
)


def _gate_satisfied(results: List[EvaluatedCheck]) -> bool:
    """Mirrors .containsNAchecks.

    Reclassification runs only when the batch contains all three of
    cdmTable, cdmField and measureValueCompleteness. A batch lacking
    any of them is left exactly as evaluate() produced it -- plain
    pass/fail/error -- matching upstream's behaviour on a restricted
    checkNames run.
    """
    present = {r.instance.check_name for r in results}
    return _GATE_CHECK_NAMES.issubset(present)


class _NotApplicableContext:
    """The derived, batch-wide lookups .applyNotApplicable consults.

    Every lookup defaults to "not missing / not empty" when the
    relevant check is absent from the batch for that table or field,
    matching upstream's ``dplyr::coalesce(..., FALSE)`` after a
    left_join that found no match.
    """

    def __init__(self, results: List[EvaluatedCheck]):
        self._table_missing: Set[str] = set()
        self._field_missing: Set[Tuple[str, str]] = set()
        self._table_empty: Set[str] = set()
        # (table, field) -> (numDenominatorRows, numViolatedRows) of
        # that field's measureValueCompleteness result.
        self._field_completeness: Dict[Tuple[str, str], Tuple[int, int]] = {}

        for evaluated in results:
            instance = evaluated.instance
            result = evaluated.result

            if instance.check_name == "cdmTable":
                if result.status == CheckStatus.FAIL:
                    self._table_missing.add(instance.cdm_table_name)

            elif instance.check_name == "cdmField":
                if (
                    result.status == CheckStatus.FAIL
                    and instance.cdm_field_name is not None
                ):
                    self._field_missing.add(
                        (instance.cdm_table_name, instance.cdm_field_name)
                    )

            elif instance.check_name == "measureValueCompleteness":
                # Only a real, computed result carries a trustworthy
                # denominator; a check that short-circuited to its own
                # NOT_APPLICABLE or ERROR (e.g. guard() firing because
                # the field itself is absent) does not tell us
                # anything about the table's row count.
                if result.status not in (
                    CheckStatus.PASS,
                    CheckStatus.FAIL,
                ):
                    continue
                # calculateNotApplicableStatus.R, ~lines 133-144:
                # tableIsEmpty is one row per table, built from
                # measureValueCompleteness's own denominator --
                # measureValueCompleteness has no WHERE clause at all,
                # so its denominator *is* the table's full row count --
                # rather than a separate, dedicated row-count query.
                if result.num_denominator_rows == 0:
                    self._table_empty.add(instance.cdm_table_name)
                if instance.cdm_field_name is not None:
                    self._field_completeness[
                        (instance.cdm_table_name, instance.cdm_field_name)
                    ] = (
                        result.num_denominator_rows,
                        result.num_violated_rows,
                    )

    def table_is_missing(self, table: str) -> bool:
        return table in self._table_missing

    def field_is_missing(self, table: str, field: str) -> bool:
        return (table, field) in self._field_missing

    def table_is_empty(self, table: str) -> bool:
        return table in self._table_empty

    def field_is_empty(self, table: str, field: str) -> bool:
        denominator_violated = self._field_completeness.get((table, field))
        if denominator_violated is None:
            return False
        denominator, violated = denominator_violated
        return denominator == violated


def _reclassify(
    instance: CheckInstance,
    result: CheckResult,
    lookups: _NotApplicableContext,
) -> CheckResult:
    """Apply .applyNotApplicable's rules, in their original order.

    Only ever called with a PASS/FAIL result (see
    _apply_not_applicable_rules): ERROR and any check's own
    NOT_APPLICABLE never reach here, matching upstream rule 5
    ("errors not related to a missing table or field should not be
    marked NA") and the requirement to never convert NOT_APPLICABLE
    back to pass/fail.
    """
    check_name = instance.check_name
    table = instance.cdm_table_name
    field = instance.cdm_field_name

    # Special case, evaluated *instead of* the 9 ordered rules below
    # (calculateNotApplicableStatus.R's main loop branches on this
    # check name before ever calling .applyNotApplicable):
    # measureConditionEraCompleteness is NA exactly when
    # CONDITION_OCCURRENCE -- a table *other than* its own
    # cdm_table_name, which is CONDITION_ERA -- is missing or empty.
    if check_name == "measureConditionEraCompleteness":
        if lookups.table_is_missing(
            "CONDITION_OCCURRENCE"
        ) or lookups.table_is_empty("CONDITION_OCCURRENCE"):
            return not_applicable("Table CONDITION_OCCURRENCE is empty.")
        return result

    # Rule 1: measurePersonCompleteness -> NA iff its table is
    # missing. Nothing else (in particular, not the checked table
    # being merely empty) makes it NA.
    if check_name == "measurePersonCompleteness":
        if lookups.table_is_missing(table):
            return not_applicable(f"Table {table} does not exist.")
        return result

    # Rule 2: cdmTable -> never NA, whatever else is true.
    if check_name == "cdmTable":
        return result

    # Rule 3: cdmField -> NA iff its table is missing.
    if check_name == "cdmField":
        if lookups.table_is_missing(table):
            return not_applicable(f"Table {table} does not exist.")
        return result

    # Rule 4: any other check -> NA if the table or field is missing.
    if lookups.table_is_missing(table):
        return not_applicable(f"Table {table} does not exist.")
    if field is not None and lookups.field_is_missing(table, field):
        return not_applicable(f"Field {table}.{field} does not exist.")

    # Rule 5: an error not caused by a missing table or field is not
    # NA -- errors stay errors. Nothing to do here: only PASS/FAIL
    # results reach this function in the first place (see
    # _apply_not_applicable_rules).

    # Rule 6: table empty -> NA.
    if lookups.table_is_empty(table):
        return not_applicable(f"Table {table} is empty.")

    # Rule 7: measureValueCompleteness never NA from its own field
    # being empty -- it is the check that measures emptiness, so it
    # must keep reporting.
    if check_name == "measureValueCompleteness":
        return result

    # Rule 8: field empty, or concept missing, or concept-and-unit
    # missing -> NA.
    if field is not None and lookups.field_is_empty(table, field):
        return not_applicable(f"Field {table}.{field} is not populated.")
    if instance.check_level == "CONCEPT" and result.num_denominator_rows == 0:
        concept_id = instance.params.get("conceptId", "")
        if check_name == "plausibleUnitConceptIds":
            return not_applicable(
                f"Combination of {field}={concept_id} and the "
                f"configured unit concept ids is missing from the "
                f"{table} table."
            )
        return not_applicable(
            f"{field}={concept_id} is missing from the {table} table."
        )

    # Rule 9: otherwise, not NA.
    return result


def _apply_not_applicable_rules(
    results: List[EvaluatedCheck],
) -> List[EvaluatedCheck]:
    """Batch-wide reclassification pass, run once after every check
    in the catalog has a raw PASS/FAIL/ERROR result.
    """
    if not _gate_satisfied(results):
        return results
    lookups = _NotApplicableContext(results)
    updated = []
    for evaluated in results:
        result = evaluated.result
        if result.status not in (CheckStatus.PASS, CheckStatus.FAIL):
            updated.append(evaluated)
            continue
        updated.append(
            EvaluatedCheck(
                evaluated.instance,
                _reclassify(evaluated.instance, result, lookups),
            )
        )
    return updated


def run_checks(
    ctx: CdmContext, catalog: List[CheckInstance]
) -> List[EvaluatedCheck]:
    """Evaluate every instance, in table-grouped order.

    A single check crashing never aborts the run: any exception
    raised by a check function, or an unregistered check name, becomes
    an ERROR result for that instance alone, and the loop continues.

    Once every instance has a raw PASS/FAIL/NOT_APPLICABLE/ERROR
    result, a batch-wide notApplicable pass runs over the full result
    set (see _apply_not_applicable_rules) to reclassify results that
    upstream's calculateNotApplicableStatus.R would also reclassify --
    a missing or empty table, a missing or unpopulated field, or a
    concept-level check with nothing to measure.
    """
    results = []
    for table_name, instances in _group_by_table(catalog).items():
        logger.info("running %d checks on %s", len(instances), table_name)
        for instance in instances:
            results.append(_run_one(ctx, instance))
    return _apply_not_applicable_rules(results)
