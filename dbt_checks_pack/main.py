from qalita_core.pack import Pack
import json
import os
import subprocess


def run_dbt_tests(
    project_dir,
    profiles_dir=None,
    target=None,
    models=None,
    threads=None,
    vars_dict=None,
):
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
    env = os.environ.copy()
    process = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    return process.returncode, process.stdout


with Pack() as pack:
    # DBT exécute en dehors du chargement des données
    config = pack.pack_config.get("job", {})
    project_dir = config.get("project_dir", ".")
    profiles_dir = config.get("profiles_dir")
    target = config.get("target")
    models = config.get("models")
    threads = config.get("threads")
    vars_dict = config.get("vars")

    code, output = run_dbt_tests(
        project_dir, profiles_dir, target, models, threads, vars_dict
    )
    print(output)

    # run_results.json est sous target/run_results.json par défaut
    run_results_path = os.path.join(project_dir, "target", "run_results.json")
    tests_total = 0
    tests_passed = 0
    tests_failed = 0

    if os.path.exists(run_results_path):
        with open(run_results_path, "r") as f:
            data = json.load(f)
            for res in data.get("results", []):
                if res.get("resource_type") == "test":
                    tests_total += 1
                    status = res.get("status")
                    if status == "pass":
                        tests_passed += 1
                    else:
                        tests_failed += 1

    score = 1.0 if tests_total == 0 else tests_passed / tests_total

    pack.metrics.data.extend(
        [
            {
                "key": "tests_total",
                "value": tests_total,
                "scope": {"perimeter": "dataset", "value": project_dir},
            },
            {
                "key": "tests_passed",
                "value": tests_passed,
                "scope": {"perimeter": "dataset", "value": project_dir},
            },
            {
                "key": "tests_failed",
                "value": tests_failed,
                "scope": {"perimeter": "dataset", "value": project_dir},
            },
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {"perimeter": "dataset", "value": project_dir},
            },
        ]
    )

    pack.metrics.save()
