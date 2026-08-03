"""Run a project's dbt tests and report what dbt actually recorded.

Everything above ``main()`` is import-safe and free of dbt: the argv wiring and
the run_results.json parsing are the parts that can silently misreport, so they
are plain functions the test suite can drive without a dbt installation.
"""

import json
import os
import subprocess

from qalita_core.pack import Pack

RUN_RESULTS = os.path.join("target", "run_results.json")


def dbt_command(
    project_dir,
    profiles_dir=None,
    target=None,
    models=None,
    threads=None,
    vars_dict=None,
):
    """Build the argv for ``dbt test``.

    Split out of :func:`run_dbt_tests` so the flag wiring is assertable without
    a dbt binary on PATH.
    """
    cmd = ["dbt", "test", "--project-dir", project_dir]
    if profiles_dir:
        cmd += ["--profiles-dir", profiles_dir]
    if target:
        cmd += ["--target", target]
    if models:
        cmd += ["--models", models]
    if threads:
        cmd += ["--threads", str(threads)]
    if vars_dict:
        cmd += ["--vars", json.dumps(vars_dict)]
    return cmd


def run_dbt_tests(
    project_dir,
    profiles_dir=None,
    target=None,
    models=None,
    threads=None,
    vars_dict=None,
):
    cmd = dbt_command(
        project_dir, profiles_dir, target, models, threads, vars_dict
    )
    process = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return process.returncode, process.stdout


def count_test_results(data):
    """Return ``(total, passed, failed)`` over the tests in a run_results.json.

    dbt reports ``pass``, ``fail``, ``error``, ``warn`` and ``skipped``. Only
    ``pass`` counts as passed: a test that errored or was skipped never
    demonstrated the property it exists to demonstrate, and folding it into the
    numerator would make a broken suite look clean.
    """
    total = 0
    passed = 0
    failed = 0
    for result in data.get("results", []):
        if result.get("resource_type") != "test":
            continue
        total += 1
        if result.get("status") == "pass":
            passed += 1
        else:
            failed += 1
    return total, passed, failed


def score_from_counts(total, passed):
    """Share of tests that passed. A project with no tests scores 1.0 — there
    is nothing failing — which is only meaningful because :func:`main` refuses
    to reach this point when dbt produced no results at all."""
    if total == 0:
        return 1.0
    return passed / total


def read_run_results(project_dir):
    """Parsed run_results.json, or None when dbt did not write one."""
    path = os.path.join(project_dir, RUN_RESULTS)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_metrics(project_dir, total, passed, failed, score):
    def scope():
        # A fresh dict per metric: one shared object would let any consumer
        # that annotates a scope silently annotate all four.
        return {"perimeter": "dataset", "value": project_dir}

    return [
        {"key": "tests_total", "value": total, "scope": scope()},
        {"key": "tests_passed", "value": passed, "scope": scope()},
        {"key": "tests_failed", "value": failed, "scope": scope()},
        {"key": "score", "value": str(round(score, 2)), "scope": scope()},
    ]


def main():
    with Pack() as pack:
        # dbt runs outside the data loading: the project is on disk, not in the
        # source the platform staged.
        config = pack.pack_config.get("job", {})
        project_dir = config.get("project_dir", ".")

        code, output = run_dbt_tests(
            project_dir,
            config.get("profiles_dir"),
            config.get("target"),
            config.get("models"),
            config.get("threads"),
            config.get("vars"),
        )
        print(output)

        data = read_run_results(project_dir)
        if data is None:
            # No run_results.json means dbt never got as far as running tests.
            # Scoring that 1.0 — what this pack used to do — reports a broken
            # dbt invocation as a flawless test suite. The CLI now honours the
            # pack's exit code, so failing here surfaces as a failed job.
            raise RuntimeError(
                f"dbt exited {code} without writing "
                f"{os.path.join(project_dir, RUN_RESULTS)}: there is no test "
                f"result to report.\ndbt output:\n{output}"
            )

        total, passed, failed = count_test_results(data)
        pack.metrics.data.extend(
            build_metrics(
                project_dir,
                total,
                passed,
                failed,
                score_from_counts(total, passed),
            )
        )
        pack.metrics.save()


if __name__ == "__main__":
    main()
