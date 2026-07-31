"""Execution of a check catalog against a CDM context.

Ports two layers of upstream's R/executeDqChecks.R pipeline:

  1. .runCheck / .processCheck -- run every check, catching whatever
     goes wrong with any single one so the batch keeps going.
  2. .evaluateThresholds' final step, .calculateNotApplicableStatus
     (R/calculateNotApplicableStatus.R) -- a *second*, batch-wide pass
     over every already-evaluated result that can reclassify some of
     them as notApplicable using facts only visible once the whole
     batch is in hand (e.g. "this field is present but 100% NULL,
     because a sibling measureValueCompleteness check said so").

Most of calculateNotApplicableStatus.R's rules turn out to already be
satisfied elsewhere in this port and are intentionally *not*
reproduced here -- see the module docstring of _apply_field_empty_rule
below and the task report for the full accounting of which rule lives
where and why.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.evaluate import EvaluatedCheck, evaluate
from omop_dqd.registry import get_check, is_registered
from omop_dqd.results import CheckStatus, errored, not_applicable

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


# DQD's .applyNotApplicable (R/calculateNotApplicableStatus.R) special-
# cases these checks out of the generic "fieldIsEmpty" rule:
#   - cdmTable is hardcoded to never be notApplicable, and cdmField is
#     notApplicable only when its *table* is missing, never when a
#     field is merely empty. Both rules are already reproduced at the
#     check level: checks/table_level.py's cdm_table() and
#     checks/field_level.py's cdm_field()/guard() query ctx.has_table
#     / ctx.has_column directly, unconditionally, so they never
#     consult a sibling field-emptiness fact in the first place.
#   - measurePersonCompleteness and measureConditionEraCompleteness
#     have their own dedicated table-existence (and, for the latter,
#     table-emptiness) special cases upstream, both of which are
#     already reproduced at the check level in checks/table_level.py.
#   - measureValueCompleteness is explicitly exempted upstream ("No
#     NA status for measureValueCompleteness if field is empty"),
#     because it is the very check that *measures* field emptiness --
#     without this exemption it would flag itself notApplicable.
_FIELD_EMPTY_EXEMPT_CHECKS = frozenset(
    {
        "cdmTable",
        "cdmField",
        "measurePersonCompleteness",
        "measureConditionEraCompleteness",
        "measureValueCompleteness",
    }
)


def _fully_null_fields(
    results: List[EvaluatedCheck],
) -> Set[Tuple[str, str]]:
    """(table, field) pairs a measureValueCompleteness check found
    100% NULL: every row present, but the column carries no data.

    Mirrors calculateNotApplicableStatus.R's `emptyFields`: one row
    per table+field from the measureValueCompleteness results, with
    ``numDenominatorRows == numViolatedRows``. Only PASS/FAIL results
    are considered -- a measureValueCompleteness check that is itself
    NOT_APPLICABLE or ERROR carries no reliable violated/denominator
    counts to compare.
    """
    empty: Set[Tuple[str, str]] = set()
    for evaluated in results:
        instance = evaluated.instance
        if instance.check_name != "measureValueCompleteness":
            continue
        result = evaluated.result
        if result.status not in (CheckStatus.PASS, CheckStatus.FAIL):
            continue
        if result.num_denominator_rows == result.num_violated_rows:
            empty.add((instance.cdm_table_name, instance.cdm_field_name))
    return empty


def _apply_field_empty_rule(
    results: List[EvaluatedCheck],
) -> List[EvaluatedCheck]:
    """Reclassify checks on a wholly-unpopulated field as NOT_APPLICABLE.

    Ported from DQD's .applyNotApplicable: a field that exists but is
    100% NULL makes every *other* check on that field notApplicable,
    not pass or fail -- upstream's fieldIsEmpty branch, gated on
    ``any(fieldIsEmpty, conceptIsMissing, conceptAndUnitAreMissing)``.

    Most of this port's checks already land on NOT_APPLICABLE on their
    own when a field is 100% NULL, because their denominator *is* the
    field's own non-null count: a 100%-NULL field yields a zero
    denominator, and evaluate() already turns a zero denominator into
    NOT_APPLICABLE (see evaluate.py). That already reproduces
    upstream's conceptIsMissing/conceptAndUnitAreMissing branches too,
    since CONCEPT-level checks denominate the same way.

    But several checks denominate over the *whole table* instead of
    the field's non-null count -- isPrimaryKey, isForeignKey,
    cdmDatatype, isStandardValidConcept, standardConceptRecordCompleteness
    and sourceConceptRecordCompleteness among them -- so for those a
    100%-NULL field does not zero out the denominator, and without
    this rule they can report a real (and misleading) PASS or FAIL on
    a column that holds no data at all. This cross-references every
    check's result against its table+field's sibling
    measureValueCompleteness result to catch exactly that case,
    mirroring the emptyFields join upstream performs across the full
    check-result batch after every check has already run.
    """
    empty_fields = _fully_null_fields(results)
    if not empty_fields:
        return results
    updated = []
    for evaluated in results:
        instance = evaluated.instance
        result = evaluated.result
        if (
            instance.check_name not in _FIELD_EMPTY_EXEMPT_CHECKS
            and instance.cdm_field_name is not None
            and result.status in (CheckStatus.PASS, CheckStatus.FAIL)
            and (instance.cdm_table_name, instance.cdm_field_name)
            in empty_fields
        ):
            updated.append(
                EvaluatedCheck(
                    instance,
                    not_applicable(
                        f"field {instance.qualified_field} " "is not populated"
                    ),
                )
            )
        else:
            updated.append(evaluated)
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
    set (see _apply_field_empty_rule) to reclassify checks on wholly
    -unpopulated fields, mirroring
    calculateNotApplicableStatus.R's cross-check step upstream.
    """
    results = []
    for table_name, instances in _group_by_table(catalog).items():
        logger.info("running %d checks on %s", len(instances), table_name)
        for instance in instances:
            results.append(_run_one(ctx, instance))
    return _apply_field_empty_rule(results)
