"""Value conventions of the codoc ETL guide."""

import datetime as dt

import polars as pl

from codoc_model.checks import NOT_NULL, check_table
from codoc_model.conventions import (
    ACCEPTED_VALUES,
    CNIL_NULL,
    CONDITIONAL_VALUE,
    DATE_ORDER,
    INSTANCE_CODE,
    MASTER_IDENTIFIER,
    UPLOAD_ID_FORMAT,
    instance_code_findings,
    relaxed_required,
    rule_checks,
    rules_for,
)
from codoc_model.spec import load_model

MODEL = load_model()


def run(table_name, data, cnil=False, enabled=True):
    """check_table with the conventions of ``table_name``, on ``data``."""
    table = MODEL.tables[table_name]
    frame = pl.DataFrame(data).lazy()
    rules = rules_for(table, enabled, cnil)
    deferred, evaluated = rule_checks(table_name, frame, rules)
    result = check_table(
        table,
        frame,
        extra=deferred,
        relaxed_required=relaxed_required(table_name, cnil),
    )
    result.findings.extend(evaluated)
    return result


def violations(result, check):
    return {
        f.column: f.violations
        for f in result.findings
        if f.check == check and not f.passed
    }


def checked(result, check):
    return {f.column for f in result.findings if f.check == check}


def test_accepted_values_allow_null_and_empty():
    result = run(
        "dwh_patient",
        {
            "patient_num": [1, 2, 3, 4, 5],
            "sex": ["F", "M", None, "", "X"],
            "death_code": ["d", None, None, "D", "dead"],
        },
    )
    assert violations(result, ACCEPTED_VALUES) == {"sex": 1, "death_code": 2}


def test_numeric_codes_compare_as_text():
    data = {"activity_form": [30.0, 31.0, None, 33.0]}
    result = run("dwh_data", data)
    assert violations(result, ACCEPTED_VALUES) == {"activity_form": 1}


def test_closed_lists_of_stay_movement_and_thesaurus():
    stay = run(
        "dwh_patient_stay", {"type_dos": ["HDJ", "Hospitalisation", "hdj"]}
    )
    assert violations(stay, ACCEPTED_VALUES) == {"type_dos": 1}
    mvt = run("dwh_patient_mvt", {"type_mvt": ["AM", "AP", "E", "Z"]})
    assert violations(mvt, ACCEPTED_VALUES) == {"type_mvt": 1}
    thesaurus = run("dwh_thesaurus_data", {"value_type": ["numeric", "list"]})
    assert violations(thesaurus, ACCEPTED_VALUES) == {"value_type": 1}


def test_out_date_before_entry_date():
    utc = dt.timezone.utc
    result = run(
        "dwh_patient_stay",
        {
            "entry_date": [
                dt.datetime(2025, 1, 2, tzinfo=utc),
                dt.datetime(2025, 1, 2, tzinfo=utc),
                dt.datetime(2025, 1, 2, tzinfo=utc),
            ],
            "out_date": [
                dt.datetime(2025, 1, 1, tzinfo=utc),
                dt.datetime(2025, 1, 2, tzinfo=utc),
                None,
            ],
        },
    )
    assert violations(result, DATE_ORDER) == {"out_date": 1}


def test_date_order_on_text_timestamps():
    result = run(
        "dwh_patient_mvt",
        {
            "entry_date": ["2025-01-02 10:00:00", "2025-01-02 10:00:00"],
            "out_date": ["2025-01-02 09:00:00", "2025-01-03 10:00:00"],
        },
    )
    assert violations(result, DATE_ORDER) == {"out_date": 1}


def test_list_values_set_if_and_only_if_liste():
    result = run(
        "dwh_thesaurus_data",
        {
            "value_type": ["liste", "liste", "numeric", "numeric", None],
            "list_values": ["a;b", None, None, "a;b", "a"],
        },
    )
    assert violations(result, CONDITIONAL_VALUE) == {"list_values": 2}


def test_one_master_identifier_per_patient():
    result = run(
        "dwh_patient_ipphist",
        {
            "ipphist_num": [1, 2, 3, 4, 5],
            "patient_num": [1, 1, 2, 2, 3],
            "master_patient_id": [True, False, True, True, False],
        },
    )
    [finding] = [f for f in result.findings if f.check == MASTER_IDENTIFIER]
    assert (finding.violations, finding.rows) == (2, 3)  # patients 2 and 3


def test_master_identifier_stored_as_text():
    result = run(
        "dwh_patient_ipphist",
        {"patient_num": [1, 1], "master_patient_id": ["t", "f"]},
    )
    assert violations(result, MASTER_IDENTIFIER) == {}


def test_upload_id_is_a_pipeline_timestamp():
    result = run(
        "hospital_instance",
        {"upload_id": [20250915000000, 20251399000000, 2025, None]},
    )
    assert violations(result, UPLOAD_ID_FORMAT) == {"upload_id": 2}


def test_rule_on_a_missing_column_is_not_applicable():
    result = run("dwh_patient", {"patient_num": [1]})
    assert not checked(result, ACCEPTED_VALUES)


def test_conventions_can_be_disabled():
    result = run("dwh_patient", {"sex": ["X"]}, enabled=False)
    assert not checked(result, ACCEPTED_VALUES)
    assert not checked(result, UPLOAD_ID_FORMAT)


def test_cnil_mode_requires_identifying_columns_null():
    data = {
        "stay_num": [1, 2],
        "encounter_num": [None, "E2"],
        "patient_num": [1, 1],
    }
    assert not checked(run("dwh_patient_stay", data), CNIL_NULL)
    result = run("dwh_patient_stay", data, cnil=True)
    assert violations(result, CNIL_NULL) == {"encounter_num": 1}
    # Required upstream, null by CNIL convention: NOT NULL is dropped.
    assert "encounter_num" not in checked(result, NOT_NULL)
    assert "encounter_num" in checked(run("dwh_patient_stay", data), NOT_NULL)


def test_cnil_patient_columns():
    result = run(
        "dwh_patient",
        {"patient_num": [1], "lastname": ["Martin"], "nss": [None]},
        cnil=True,
    )
    assert violations(result, CNIL_NULL) == {"lastname": 1}
    assert checked(result, CNIL_NULL) == {"lastname", "nss"}


def test_instance_codes_must_exist_in_hospital_instance():
    frames = {
        "hospital_instance": pl.DataFrame({"code": ["H1", "H2"]}).lazy(),
        "dwh_patient": pl.DataFrame(
            {"instance_id": ["H1", "H3", None]}
        ).lazy(),
        "dwh_patient_stay": pl.DataFrame({"instance_stay_id": ["H2"]}).lazy(),
    }
    results = {
        name: check_table(MODEL.tables[name], frame)
        for name, frame in frames.items()
    }
    instance_code_findings(results, frames)
    assert violations(results["dwh_patient"], INSTANCE_CODE) == {
        "instance_id": 1
    }
    assert checked(results["dwh_patient_stay"], INSTANCE_CODE) == {
        "instance_stay_id"
    }
    assert violations(results["dwh_patient_stay"], INSTANCE_CODE) == {}


def test_instance_codes_need_the_referential():
    frames = {"dwh_patient": pl.DataFrame({"instance_id": ["H3"]}).lazy()}
    results = {
        "dwh_patient": check_table(
            MODEL.tables["dwh_patient"], frames["dwh_patient"]
        )
    }
    instance_code_findings(results, frames)
    assert not checked(results["dwh_patient"], INSTANCE_CODE)
