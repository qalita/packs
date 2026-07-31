import pytest

from omop_dqd.catalog import (
    CheckInstance,
    load_catalog,
    load_check_descriptions,
)


def test_descriptions_are_keyed_by_check_name():
    descriptions = load_check_descriptions("5.4")
    assert descriptions["isRequired"].severity == "fatal"
    assert descriptions["isRequired"].check_level == "FIELD"
    assert (
        descriptions["measureValueCompleteness"].severity == "characterization"
    )


def test_catalog_instantiates_thousands_of_checks():
    # Exact, measured post-revision counts (revision 1 gates cdmDatatype,
    # fkDomain, fkClass and plausibleUnitConceptIds on their real
    # evaluationFilter, per docs/superpowers/plans/2026-07-31-omop-cdm-pack-revision-1.md).
    # A loose lower bound would not catch a quarter of the catalog
    # silently vanishing, so these are pinned exactly.
    assert len(load_catalog("5.4")) == 2539
    assert len(load_catalog("5.3")) == 2021


def test_every_instance_carries_a_known_severity():
    for check in load_catalog("5.4"):
        assert check.severity in {
            "fatal",
            "convention",
            "characterization",
        }


def test_cdm_field_check_is_instantiated_for_every_field_row():
    catalog = load_catalog("5.4")
    cdm_field_checks = [c for c in catalog if c.check_name == "cdmField"]
    person_fields = [
        c for c in cdm_field_checks if c.cdm_table_name == "PERSON"
    ]
    assert len(person_fields) > 5
    assert all(c.threshold == 0.0 for c in cdm_field_checks)


def test_is_required_is_instantiated_only_when_the_cell_says_yes():
    catalog = load_catalog("5.4")
    required = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isRequired"
    }
    assert ("PERSON", "person_id") in required
    assert ("PERSON", "month_of_birth") not in required


def test_foreign_key_checks_carry_the_referenced_table_and_field():
    catalog = load_catalog("5.4")
    fks = [
        c
        for c in catalog
        if c.check_name == "isForeignKey"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
        and c.cdm_field_name == "condition_concept_id"
    ]
    assert len(fks) == 1
    assert fks[0].params["fkTableName"] == "CONCEPT"
    assert fks[0].params["fkFieldName"] == "CONCEPT_ID"


def test_value_triggered_checks_capture_the_cell_as_a_param():
    catalog = load_catalog("5.4")
    domains = [
        c
        for c in catalog
        if c.check_name == "fkDomain"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
        and c.cdm_field_name == "condition_concept_id"
    ]
    assert len(domains) == 1
    assert domains[0].params["value"] == "Condition"


def test_thresholds_are_parsed_as_floats_defaulting_to_zero():
    catalog = load_catalog("5.4")
    completeness = [
        c
        for c in catalog
        if c.check_name == "standardConceptRecordCompleteness"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
    ]
    assert completeness
    assert all(isinstance(c.threshold, float) for c in completeness)


def test_table_and_concept_level_checks_are_present():
    catalog = load_catalog("5.4")
    levels = {c.check_level for c in catalog}
    assert levels == {"TABLE", "FIELD", "CONCEPT"}


def test_instances_are_hashable_so_they_can_be_deduplicated():
    catalog = load_catalog("5.4")
    assert isinstance(catalog[0], CheckInstance)
    assert len(set(catalog)) == len(catalog)


def test_unsupported_cdm_version_is_rejected():
    with pytest.raises(ValueError, match="5.2"):
        load_catalog("5.2")


def test_both_supported_versions_load():
    assert load_catalog("5.3")
    assert load_catalog("5.4")


def test_cdm_datatype_is_instantiated_only_for_integer_fields():
    catalog = load_catalog("5.4")
    datatypes = {
        c.params["value"].lower()
        for c in catalog
        if c.check_name == "cdmDatatype"
    }
    assert datatypes == {"integer"}


def test_fk_domain_requires_the_field_to_be_a_foreign_key():
    catalog = load_catalog("5.4")
    fk_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isForeignKey"
    }
    domain_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "fkDomain"
    }
    assert domain_fields
    assert domain_fields <= fk_fields


def test_fk_class_requires_the_field_to_be_a_foreign_key():
    catalog = load_catalog("5.4")
    fk_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isForeignKey"
    }
    class_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "fkClass"
    }
    assert class_fields
    assert class_fields <= fk_fields


def test_plausible_unit_concept_ids_is_gated_on_its_threshold():
    catalog = load_catalog("5.4")
    units = [c for c in catalog if c.check_name == "plausibleUnitConceptIds"]
    assert units
    # the gate is the threshold column, but the payload is the id list
    assert all(c.params["value"] for c in units)
    assert all(c.params["conceptId"] for c in units)
    assert all(c.threshold > 0 for c in units)


def test_source_value_completeness_carries_its_companion_field():
    catalog = load_catalog("5.4")
    checks = [c for c in catalog if c.check_name == "sourceValueCompleteness"]
    assert checks
    assert all(c.params.get("standardConceptFieldName") for c in checks)
