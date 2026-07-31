"""Outcome types shared by every check."""

from dataclasses import dataclass


class CheckStatus:
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    ERROR = "ERROR"


@dataclass(frozen=True)
class CheckResult:
    num_violated_rows: int = 0
    num_denominator_rows: int = 0
    status: str = CheckStatus.PASS
    message: str = ""

    @property
    def pct_violated_rows(self) -> float:
        if self.num_denominator_rows == 0:
            return 0.0
        return 100.0 * self.num_violated_rows / self.num_denominator_rows


def counted(violated: int, denominator: int) -> CheckResult:
    """A measured result whose status is resolved later by evaluate()."""
    return CheckResult(
        num_violated_rows=int(violated),
        num_denominator_rows=int(denominator),
    )


def not_applicable(reason: str) -> CheckResult:
    return CheckResult(status=CheckStatus.NOT_APPLICABLE, message=reason)


def errored(reason: str) -> CheckResult:
    return CheckResult(status=CheckStatus.ERROR, message=reason)
