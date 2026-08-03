import pytest

from omop_dqd.catalog import (
    FIELD_CHECK_SPECS,
    CheckInstance,
    _instantiate,
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
    #
    # These dropped from 2539/2021 when gating became case-sensitive to
    # match R's `dplyr::filter(cdmDatatype=='integer')`: 5.4 loses the
    # 4 `Integer` rows, 5.3 the 13 `Integer` + 3 `INTEGER` ones.
    assert len(load_catalog("5.4")) == 2535
    assert len(load_catalog("5.3")) == 2005


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


def test_concept_level_checks_capture_the_concept_name():
    # rendered_description needs @conceptId *and* @conceptName to
    # produce readable recommendations for concept-level checks.
    catalog = load_catalog("5.4")
    concept_checks = [
        c
        for c in catalog
        if c.check_name
        in (
            "plausibleGender",
            "plausibleGenderUseDescendants",
            "plausibleUnitConceptIds",
        )
    ]
    assert concept_checks
    assert all(c.params.get("conceptName") for c in concept_checks)


def test_rendered_description_substitutes_known_placeholders():
    instance = CheckInstance(
        check_name="isRequired",
        check_level="FIELD",
        cdm_table_name="PERSON",
        cdm_field_name="person_id",
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description=(
            "NULLs in @cdmFieldName of @cdmTableName are not allowed."
        ),
    )
    assert instance.rendered_description == (
        "NULLs in PERSON_ID of PERSON are not allowed."
    )
    # description itself, the vendored verbatim text, is untouched
    assert instance.description == (
        "NULLs in @cdmFieldName of @cdmTableName are not allowed."
    )


def test_rendered_description_substitutes_concept_placeholders():
    instance = CheckInstance(
        check_name="plausibleGender",
        check_level="CONCEPT",
        cdm_table_name="CONDITION_OCCURRENCE",
        cdm_field_name="condition_concept_id",
        threshold=0.0,
        severity="characterization",
        kahn_category="Plausibility",
        description="Concept @conceptId (@conceptName) looks wrong.",
        param_items=(
            ("conceptId", "26662"),
            ("conceptName", "Pregnant"),
        ),
    )
    assert instance.rendered_description == (
        "Concept 26662 (Pregnant) looks wrong."
    )


def test_rendered_description_leaves_unknown_tokens_and_missing_values_alone():
    # A table-level check has no field, so @cdmFieldName has nothing
    # to substitute with and must be left as a visible, honest token
    # rather than blanked out. Likewise @plausibleGender is not one
    # of the four placeholders this pack renders.
    instance = CheckInstance(
        check_name="cdmTable",
        check_level="TABLE",
        cdm_table_name="PERSON",
        cdm_field_name=None,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="@cdmFieldName of @cdmTableName, gender=@plausibleGender",
    )
    assert instance.rendered_description == (
        "@cdmFieldName of PERSON, gender=@plausibleGender"
    )


def test_no_rendered_recommendation_relevant_description_has_leftover_placeholders():
    # The four placeholders this pack substitutes must be fully
    # resolved across the real catalog wherever the instance actually
    # carries the corresponding value (every instance has a table;
    # concept-level instances have conceptId/conceptName; field-level
    # instances have a field).
    catalog = load_catalog("5.4")
    for instance in catalog:
        rendered = instance.rendered_description
        assert "@cdmTableName" not in rendered
        if instance.cdm_field_name is not None:
            assert "@cdmFieldName" not in rendered
        if instance.check_level == "CONCEPT":
            assert "@conceptId" not in rendered
            assert "@conceptName" not in rendered


def test_source_value_completeness_carries_its_companion_field():
    catalog = load_catalog("5.4")
    checks = [c for c in catalog if c.check_name == "sourceValueCompleteness"]
    assert checks
    assert all(c.params.get("standardConceptFieldName") for c in checks)


# The real vendored CSVs happen to have fkDomain/fkClass set only on
# rows where isForeignKey is already "Yes", so the `requires` gate is
# a no-op against real data: nothing in the tests above would fail if
# the gate were deleted. These drive _instantiate directly with
# synthetic rows so the gate is exercised regardless of what the real
# CSVs contain.
def _spec_for(name):
    return tuple(s for s in FIELD_CHECK_SPECS if s.name == name)


def _field_row(**overrides):
    row = {
        "cdmTableName": "SOME_TABLE",
        "cdmFieldName": "some_concept_id",
    }
    row.update(overrides)
    return row


def test_fk_domain_is_skipped_when_the_field_is_not_a_foreign_key():
    produced = _instantiate(
        [_field_row(fkDomain="Condition", isForeignKey="No")],
        _spec_for("fkDomain"),
        load_check_descriptions("5.4"),
        "cdmFieldName",
    )
    assert not produced


def test_fk_domain_is_instantiated_when_the_field_is_a_foreign_key():
    produced = _instantiate(
        [_field_row(fkDomain="Condition", isForeignKey="Yes")],
        _spec_for("fkDomain"),
        load_check_descriptions("5.4"),
        "cdmFieldName",
    )
    assert len(produced) == 1
    assert produced[0].params["value"] == "Condition"


def test_fk_class_is_skipped_when_the_field_is_not_a_foreign_key():
    produced = _instantiate(
        [_field_row(fkClass="Ingredient", isForeignKey="")],
        _spec_for("fkClass"),
        load_check_descriptions("5.4"),
        "cdmFieldName",
    )
    assert not produced


def test_requires_matching_is_case_sensitive():
    """`isForeignKey=='Yes'` in R does not match a lowercase "yes"."""
    produced = _instantiate(
        [_field_row(fkClass="Ingredient", isForeignKey="yes")],
        _spec_for("fkClass"),
        load_check_descriptions("5.4"),
        "cdmFieldName",
    )
    assert not produced


def test_yes_trigger_matching_is_case_sensitive():
    """`isRequired=='Yes'` in R does not match a lowercase "yes"."""
    descriptions = load_check_descriptions("5.4")
    assert _instantiate(
        [_field_row(isRequired="Yes")],
        _spec_for("isRequired"),
        descriptions,
        "cdmFieldName",
    )
    assert not _instantiate(
        [_field_row(isRequired="yes")],
        _spec_for("isRequired"),
        descriptions,
        "cdmFieldName",
    )


def test_cdm_datatype_trigger_matching_is_case_sensitive():
    """R's `dplyr::filter(cdmDatatype=='integer')` is case-sensitive.

    The vendored CSVs really do carry `Integer`/`INTEGER` rows
    alongside `integer` ones; DQD never runs cdmDatatype on them, so
    neither do we.
    """
    descriptions = load_check_descriptions("5.4")
    assert _instantiate(
        [_field_row(cdmDatatype="integer")],
        _spec_for("cdmDatatype"),
        descriptions,
        "cdmFieldName",
    )
    for casing in ("Integer", "INTEGER"):
        assert not _instantiate(
            [_field_row(cdmDatatype=casing)],
            _spec_for("cdmDatatype"),
            descriptions,
            "cdmFieldName",
        )
