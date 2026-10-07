import importlib.util
from pathlib import Path

import polars as pl
import pytest

from codoc_model.spec import load_model

spec = importlib.util.spec_from_file_location(
    "codoc_main", Path(__file__).resolve().parents[1] / "main.py"
)
main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(main)  # the run itself sits under __main__
MODEL = load_model()


def test_defaults_check_every_table_leniency_auto():
    config = main.read_job({}, MODEL)
    assert config["strictness"] == "auto"
    assert config["conventions"] is True
    assert config["cnil_compliant"] is False
    assert main.tables_in_scope(MODEL, config) == list(MODEL.tables)


def test_scope_and_exclusions():
    config = main.read_job(
        {
            "tables": ["dwh_patient", "dwh_data"],
            "excluded_tables": ["dwh_data"],
        },
        MODEL,
    )
    assert main.tables_in_scope(MODEL, config) == ["dwh_patient"]


@pytest.mark.parametrize(
    "job",
    [
        {"tables": ["patients"]},
        {"excluded_tables": "nope"},
        {"table": "patients"},
        {"extensions": ["labs"]},
        {"type_strictness": "loose"},
        {"conventions": "yes"},
        {"cnil_compliant": 1},
    ],
)
def test_bad_config_fails_loudly(job):
    with pytest.raises(ValueError):
        main.read_job(job, MODEL)


def test_run_checks_scores_conventions_and_cross_table_codes(tmp_path):
    paths = {}
    for name, data in {
        "hospital_instance": {"id": [1], "code": ["H1"], "name": ["A"]},
        "dwh_patient": {
            "patient_num": [1, 2],
            "sex": ["F", "X"],
            "instance_id": ["H1", "H9"],
        },
    }.items():
        path = tmp_path / f"{name}.parquet"
        pl.DataFrame(data).write_parquet(path)
        paths[name] = [str(path)]
    config = main.read_job({}, MODEL)
    results = main.run_checks(
        MODEL, ["dwh_patient", "hospital_instance"], paths, config, False
    )
    patient = results[0]
    failed = {(f.check, f.column) for f in patient.findings if not f.passed}
    assert ("accepted_values", "sex") in failed
    assert ("instance_code", "instance_id") in failed
