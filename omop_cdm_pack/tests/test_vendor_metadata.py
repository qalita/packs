import csv
import os

from omop_dqd import SUPPORTED_CDM_VERSIONS, VENDOR_CSV_DIR

EXPECTED_CHECK_TYPE_COUNT = 27


def _read(version, kind):
    path = os.path.join(VENDOR_CSV_DIR, f"OMOP_CDMv{version}_{kind}.csv")
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def test_all_vendored_csvs_exist_for_every_supported_version():
    for version in SUPPORTED_CDM_VERSIONS:
        for kind in (
            "Check_Descriptions",
            "Table_Level",
            "Field_Level",
            "Concept_Level",
        ):
            rows = _read(version, kind)
            assert rows, f"OMOP_CDMv{version}_{kind}.csv is empty"


def test_check_descriptions_declare_27_check_types():
    rows = _read("5.4", "Check_Descriptions")
    assert len(rows) == EXPECTED_CHECK_TYPE_COUNT


def test_check_descriptions_expose_the_columns_the_catalog_needs():
    rows = _read("5.4", "Check_Descriptions")
    for column in (
        "checkLevel",
        "checkName",
        "checkDescription",
        "kahnCategory",
        "severity",
    ):
        assert column in rows[0], f"missing column {column}"


def test_field_level_has_the_key_columns_used_for_instantiation():
    rows = _read("5.4", "Field_Level")
    for column in (
        "cdmTableName",
        "cdmFieldName",
        "isRequired",
        "isRequiredThreshold",
        "fkTableName",
        "fkFieldName",
        "standardConceptFieldName",
    ):
        assert column in rows[0], f"missing column {column}"


def test_severities_are_within_the_known_vocabulary():
    rows = _read("5.4", "Check_Descriptions")
    assert {r["severity"] for r in rows} <= {
        "fatal",
        "convention",
        "characterization",
    }
