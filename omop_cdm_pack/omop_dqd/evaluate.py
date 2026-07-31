"""Threshold application, mirroring the DQD pass/fail rule."""

from dataclasses import dataclass, replace

from omop_dqd.catalog import CheckInstance
from omop_dqd.results import CheckResult, CheckStatus


@dataclass(frozen=True)
class EvaluatedCheck:
    instance: CheckInstance
    result: CheckResult


def evaluate(instance: CheckInstance, result: CheckResult) -> CheckResult:
    """Resolve a measured result into PASS or FAIL.

    Deliberately does not inspect the denominator. Upstream's own
    R/evaluateThresholds.R decides `failed` purely from the threshold
    and `numViolatedRows` -- a zero denominator with zero violations
    is a pass at this layer. Applicability is a separate, later
    concern that needs the *whole* batch of results (whether the
    table or field is missing or empty, elsewhere in the same run),
    which only omop_dqd.runner has in hand; see its notApplicable
    reclassification pass, ported from
    R/calculateNotApplicableStatus.R.
    """
    if result.status in (
        CheckStatus.NOT_APPLICABLE,
        CheckStatus.ERROR,
    ):
        return result

    if instance.threshold <= 0:
        failed = result.num_violated_rows > 0
    else:
        failed = result.pct_violated_rows > instance.threshold

    return replace(
        result,
        status=CheckStatus.FAIL if failed else CheckStatus.PASS,
    )
