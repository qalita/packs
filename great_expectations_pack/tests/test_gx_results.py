"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Reading a GX result correctly is what separates "the data is bad" from "the
check never ran". GX signals the second one inside ``exception_info`` while
still setting ``success=False``, so a pack that ignores that field reports
clean data as failing and a broken engine as a data problem.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gx_results  # noqa: E402


class FakeResult:
    def __init__(self, success=True, result=None, exception_info=None):
        self.success = success
        self.result = result or {}
        self.exception_info = exception_info


def test_no_exception_reads_as_none():
    assert gx_results.raised_exception(FakeResult()) is None
    assert (
        gx_results.raised_exception(
            FakeResult(exception_info={"raised_exception": False})
        )
        is None
    )


def test_flat_exception_info_is_detected():
    # Shape GX uses when the whole expectation could not be evaluated, e.g.
    # "No provider found for column_values.increasing.unexpected_count".
    result = FakeResult(
        success=False,
        exception_info={
            "raised_exception": True,
            "exception_message": "No provider found for x",
            "exception_traceback": "...",
        },
    )
    assert gx_results.raised_exception(result) == "No provider found for x"


def test_metric_keyed_exception_info_is_detected():
    # Shape GX uses when one metric of the expectation failed, e.g. the regex
    # families on DuckDB.
    result = FakeResult(
        success=False,
        exception_info={
            "MetricConfigurationID(...)": {
                "raised_exception": True,
                "exception_message": "'Dialect' object has no attribute",
            }
        },
    )
    assert gx_results.raised_exception(result) == (
        "'Dialect' object has no attribute"
    )


def test_exception_without_message_is_still_an_exception():
    result = FakeResult(
        success=False,
        exception_info={
            "MetricConfigurationID(...)": {
                "raised_exception": True,
                "exception_message": "",
            }
        },
    )
    assert gx_results.raised_exception(result) == "unknown"


@pytest.mark.parametrize(
    "limit,expected",
    [
        (0, "BASIC"),
        (-5, "BASIC"),
        (10, {"result_format": "SUMMARY", "partial_unexpected_count": 10}),
        (
            10_000,
            {
                "result_format": "SUMMARY",
                "partial_unexpected_count": gx_results.MAX_EXAMPLE_LIMIT,
            },
        ),
    ],
)
def test_result_format_is_always_bounded(limit, expected):
    assert gx_results.result_format(limit) == expected


def test_summary_truncates_examples_to_the_limit():
    result = FakeResult(
        success=False,
        result={
            "observed_value": 42,
            "unexpected_count": "17",
            "partial_unexpected_list": list(range(50)),
        },
    )
    summary = gx_results.summarize_result(result, 3)
    assert summary["observed_value"] == 42
    # The pandas engine stringifies this; the platform needs a number.
    assert summary["unexpected_count"] == 17
    assert summary["examples"] == [0, 1, 2]


def test_summary_omits_examples_when_none_are_wanted():
    result = FakeResult(
        result={"partial_unexpected_list": [1, 2, 3], "observed_value": 1}
    )
    assert "examples" not in gx_results.summarize_result(result, 0)
