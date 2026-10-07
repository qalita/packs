import importlib.util
from pathlib import Path

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
    ],
)
def test_bad_config_fails_loudly(job):
    with pytest.raises(ValueError):
        main.read_job(job, MODEL)
