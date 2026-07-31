"""Threshold application, mirroring the DQD pass/fail rule."""

from dataclasses import dataclass, replace

from omop_dqd.catalog import CheckInstance
from omop_dqd.results import CheckResult, CheckStatus


@dataclass(frozen=True)
class EvaluatedCheck:
    instance: CheckInstance
    result: CheckResult


def evaluate(instance: CheckInstance, result: CheckResult) -> CheckResult:
    """Resolve a measured result into PASS or FAIL."""
    if result.status in (
        CheckStatus.NOT_APPLICABLE,
        CheckStatus.ERROR,
    ):
        return result

    if result.num_denominator_rows == 0:
        return replace(
            result,
            status=CheckStatus.NOT_APPLICABLE,
            message="no rows to evaluate",
        )

    if instance.threshold <= 0:
        failed = result.num_violated_rows > 0
    else:
        failed = result.pct_violated_rows > instance.threshold

    return replace(
        result,
        status=CheckStatus.FAIL if failed else CheckStatus.PASS,
    )
