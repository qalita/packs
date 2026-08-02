"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Turning a Great Expectations validation result into publishable metrics.

Separate from the DuckDB glue on purpose: nothing here imports a database
driver, so the rules that decide what a run reports — in particular whether a
check FAILED or merely could not RUN — are testable without an engine.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "MAX_EXAMPLE_LIMIT",
    "raised_exception",
    "result_format",
    "summarize_result",
]

# Hard ceiling on row-level evidence: an example list is the one thing a pack
# emits that grows with the data, so it is capped in code and not only in conf.
MAX_EXAMPLE_LIMIT = 1_000


def _capped(limit: int) -> int:
    return min(max(int(limit), 0), MAX_EXAMPLE_LIMIT)


def raised_exception(result: Any) -> str | None:
    """The error GX hit while validating, or ``None`` if it validated fine.

    GX never raises out of ``Batch.validate``: an expectation whose metric has
    no implementation for the engine comes back as ``success=False`` with the
    failure buried in ``exception_info``. Reading that field is the difference
    between reporting "this check could not run" and reporting "this check
    failed", which are opposite conclusions about the data.

    ``exception_info`` has two shapes — flat for a whole-expectation failure,
    keyed by metric id when a single metric failed — and both are checked here.
    """
    info = getattr(result, "exception_info", None)
    if not isinstance(info, dict):
        return None

    if "raised_exception" in info:
        if not info.get("raised_exception"):
            return None
        return str(info.get("exception_message") or "").strip() or "unknown"

    for entry in info.values():
        if isinstance(entry, dict) and entry.get("raised_exception"):
            message = str(entry.get("exception_message") or "").strip()
            return message or "unknown"
    return None


def result_format(limit: int) -> dict[str, Any] | str:
    """GX result format that caps the example values it brings back.

    GX's COMPLETE format returns every failing value, which on a large source
    is the one unbounded thing a validation pack could produce.
    """
    capped = _capped(limit)
    if capped == 0:
        return "BASIC"
    return {"result_format": "SUMMARY", "partial_unexpected_count": capped}


def summarize_result(result: Any, limit: int) -> dict[str, Any]:
    """The parts of a GX result worth publishing, all of them bounded."""
    payload = getattr(result, "result", None) or {}
    summary: dict[str, Any] = {}

    observed = payload.get("observed_value")
    if observed is not None:
        summary["observed_value"] = observed

    unexpected = payload.get("unexpected_count")
    if unexpected is not None:
        try:
            # The pandas engine reports this as a string and the SQL engine as
            # an int; the platform only charts it when it is a number.
            summary["unexpected_count"] = int(unexpected)
        except (TypeError, ValueError):
            summary["unexpected_count"] = unexpected

    capped = _capped(limit)
    examples = payload.get("partial_unexpected_list") or []
    if examples and capped:
        summary["examples"] = list(examples)[:capped]

    return summary
