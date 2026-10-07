"""The vendored model: shape, and agreement with the upstream docs."""

import pytest

from codoc_model.spec import FAMILIES, load_model


@pytest.fixture(scope="module")
def model():
    return load_model()


def test_model_is_pinned_to_an_upstream_commit(model):
    assert model.repository.endswith("codoc-health/codoc-data-model-docs")
    assert len(model.commit) == 40


def test_every_documented_table_is_present(model):
    assert sorted(model.tables) == [
        "dwh_data",
        "dwh_document",
        "dwh_patient",
        "dwh_patient_ipphist",
        "dwh_patient_mvt",
        "dwh_patient_stay",
        "dwh_thesaurus_data",
        "dwh_thesaurus_department",
        "dwh_thesaurus_site",
        "dwh_thesaurus_unit",
        "hospital_instance",
    ]


def test_every_table_has_exactly_one_primary_key(model):
    for table in model.tables.values():
        keys = [c.name for c in table.columns if c.primary_key]
        assert len(keys) == 1, table.name


def test_foreign_keys_point_at_model_tables(model):
    for table in model.tables.values():
        for column in table.columns:
            if column.foreign_key:
                assert column.foreign_key in model.tables, column


def test_datatypes_are_all_mapped(model):
    for table in model.tables.values():
        for column in table.columns + table.extension_columns:
            assert column.datatype in FAMILIES


def test_broken_upstream_quote_does_not_shift_fields(model):
    # Upstream closes the ETL-convention quote of these two rows too early.
    thesaurus = model.tables["dwh_thesaurus_data"].known_columns()
    assert thesaurus["list_values"].declared_type == "varchar(4000)"
    data = model.tables["dwh_data"].known_columns()
    assert data["drug_condition_reason"].declared_type == "varchar(4000)"


def test_extensions_only_expected_when_enabled(model):
    data = model.tables["dwh_data"]
    assert model.extensions == ["drugs", "pmsi"]
    base = {c.name for c in data.expected_columns()}
    with_pmsi = {c.name for c in data.expected_columns(["pmsi"])}
    assert "activity_form" not in base
    assert with_pmsi - base == {
        "activity_form",
        "intersectoriel_number",
        "residency_zip_code",
    }
