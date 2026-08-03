"""Tests for the dbt runner pack.

The pack's job is to report what dbt recorded, so the tests drive the two
places it can misreport: the argv handed to dbt, and the reading of
run_results.json. Neither needs dbt installed.
"""

import json

import pytest

import main


def write_run_results(project_dir, results):
    target = project_dir / "target"
    target.mkdir(parents=True, exist_ok=True)
    (target / "run_results.json").write_text(
        json.dumps({"results": results}), encoding="utf-8"
    )


def test_command_is_minimal_when_nothing_is_configured():
    assert main.dbt_command("/p") == [
        "dbt",
        "test",
        "--project-dir",
        "/p",
    ]


def test_command_carries_every_configured_option():
    cmd = main.dbt_command(
        "/p",
        profiles_dir="/prof",
        target="prod",
        models="tag:daily",
        threads=8,
        vars_dict={"day": "2026-08-03"},
    )
    assert cmd[:4] == ["dbt", "test", "--project-dir", "/p"]
    assert (
        "--profiles-dir" in cmd
        and cmd[cmd.index("--profiles-dir") + 1] == "/prof"
    )
    assert cmd[cmd.index("--target") + 1] == "prod"
    assert cmd[cmd.index("--models") + 1] == "tag:daily"
    # threads reaches subprocess as text, not as an int argv entry
    assert cmd[cmd.index("--threads") + 1] == "8"
    assert json.loads(cmd[cmd.index("--vars") + 1]) == {"day": "2026-08-03"}


@pytest.mark.parametrize("falsy", [None, "", 0])
def test_falsy_options_are_omitted_rather_than_passed_empty(falsy):
    cmd = main.dbt_command("/p", profiles_dir=falsy, threads=falsy)
    assert "--profiles-dir" not in cmd
    assert "--threads" not in cmd


def test_only_test_resources_are_counted():
    total, passed, failed = main.count_test_results(
        {
            "results": [
                {"resource_type": "test", "status": "pass"},
                {"resource_type": "model", "status": "success"},
                {"resource_type": "seed", "status": "success"},
            ]
        }
    )
    assert (total, passed, failed) == (1, 1, 0)


@pytest.mark.parametrize("status", ["fail", "error", "warn", "skipped"])
def test_anything_but_pass_counts_as_failed(status):
    """A test that errored or was skipped proved nothing; counting it as a
    pass is how a broken suite reports itself as clean."""
    total, passed, failed = main.count_test_results(
        {"results": [{"resource_type": "test", "status": status}]}
    )
    assert (total, passed, failed) == (1, 0, 1)


def test_counts_are_zero_on_an_empty_report():
    assert main.count_test_results({}) == (0, 0, 0)
    assert main.count_test_results({"results": []}) == (0, 0, 0)


@pytest.mark.parametrize(
    "total,passed,expected",
    [(0, 0, 1.0), (4, 4, 1.0), (4, 3, 0.75), (4, 0, 0.0)],
)
def test_score_is_the_pass_share(total, passed, expected):
    assert main.score_from_counts(total, passed) == expected


def test_read_run_results_returns_none_when_dbt_wrote_nothing(tmp_path):
    assert main.read_run_results(str(tmp_path)) is None


def test_read_run_results_parses_what_dbt_wrote(tmp_path):
    write_run_results(tmp_path, [{"resource_type": "test", "status": "pass"}])
    data = main.read_run_results(str(tmp_path))
    assert data["results"][0]["status"] == "pass"


def test_metrics_carry_the_expected_keys_and_types():
    metrics = main.build_metrics("/p", 4, 3, 1, 0.75)
    assert [m["key"] for m in metrics] == [
        "tests_total",
        "tests_passed",
        "tests_failed",
        "score",
    ]
    by_key = {m["key"]: m["value"] for m in metrics}
    assert by_key["tests_total"] == 4
    # score crosses the wire as a rounded string, as the platform expects
    assert by_key["score"] == "0.75"
    assert all(
        m["scope"] == {"perimeter": "dataset", "value": "/p"} for m in metrics
    )


def test_each_metric_owns_its_scope_dict():
    metrics = main.build_metrics("/p", 1, 1, 0, 1.0)
    metrics[0]["scope"]["value"] = "mutated"
    assert metrics[1]["scope"]["value"] == "/p"
