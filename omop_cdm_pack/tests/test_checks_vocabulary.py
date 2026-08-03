"""Field-level checks depending on the OMOP vocabulary (CONCEPT).

Expected values are derived from the upstream SQL templates in
inst/sql/sql_server/, not from the check descriptions' prose, and
several diverge sharply from a literal reading of the plan this task
started from -- see the comment on each test for the deciding SQL
line.

Two of the five names this task implements --
standardConceptRecordCompleteness and sourceConceptRecordCompleteness
-- turn out NOT to touch CONCEPT at all: their shared template,
field_concept_record_completeness.sql, only ever reads the CDM table
itself. They are therefore covered by a dedicated test
(test_completeness_checks_do_not_require_vocabulary) instead of the
parametrised degradation test below.
"""

import polars as pl
import pytest

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus

# fkDomain, fkClass and isStandardValidConcept all LEFT/INNER JOIN
# CONCEPT directly and must degrade to NOT_APPLICABLE without it.
# standardConceptRecordCompleteness and sourceConceptRecordCompleteness
# are deliberately excluded -- see the module docstring and
# test_completeness_checks_do_not_require_vocabulary below.
VOCABULARY_CHECKS = (
    "fkDomain",
    "fkClass",
    "isStandardValidConcept",
)


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="convention",
        kahn_category="Conformance",
        description="d",
        param_items=tuple(sorted(params.items())),
    )
    return get_check(check_name)(ctx, instance)


def _write_tables(tmp_path, **frames):
    """Write {NAME: pl.DataFrame} to parquet, return a CdmContext."""
    table_paths = {}
    for name, frame in frames.items():
        path = tmp_path / f"{name.lower()}.parquet"
        frame.write_parquet(path)
        table_paths[name] = [str(path)]
    return CdmContext.from_paths(table_paths)


@pytest.mark.parametrize("check_name", VOCABULARY_CHECKS)
def test_all_vocabulary_checks_degrade_gracefully(
    mini_cdm_no_vocabulary, check_name
):
    result = _run(
        mini_cdm_no_vocabulary,
        check_name,
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_completeness_checks_do_not_require_vocabulary(
    mini_cdm_no_vocabulary,
):
    # field_concept_record_completeness.sql (the shared template
    # behind both checks) never references CONCEPT: it is a plain
    # `WHERE cdmTable.@cdmFieldName = 0 ...` on the CDM table itself.
    # So, unlike the other three vocabulary checks, these two must
    # NOT become NOT_APPLICABLE just because CONCEPT is missing --
    # they still compute a real result.
    for check_name in (
        "standardConceptRecordCompleteness",
        "sourceConceptRecordCompleteness",
    ):
        result = _run(
            mini_cdm_no_vocabulary,
            check_name,
            "CONDITION_OCCURRENCE",
            "condition_concept_id",
        )
        assert result.status != CheckStatus.NOT_APPLICABLE
        # condition_concept_id has no source-value companion in
        # field_concept_record_completeness.sql's field-name list, so
        # only `= 0` counts, and the mini CDM has zero such rows.
        assert result.num_denominator_rows == 5
        assert result.num_violated_rows == 0


# --- fkDomain ----------------------------------------------------
#
# field_fk_domain.sql LEFT JOINs CONCEPT and filters violated rows on
# `co.concept_id != 0 AND co.domain_id NOT IN ('@fkDomain')`. Because
# it is a LEFT JOIN, a concept id absent from CONCEPT (or literally 0)
# produces a NULL co.concept_id; SQL's `NULL != 0` is unknown, not
# true, so such rows are dropped by the WHERE clause -- they are NOT
# violations. Only a concept that genuinely exists in CONCEPT, with
# the wrong domain, is. The denominator subquery
# (`SELECT COUNT_BIG(*) FROM @schema.@cdmTableName cdmTable`) has no
# WHERE clause at all: the table's full row count, not filtered to
# non-null field values.


def test_fk_domain_orphan_and_null_concepts_are_not_violations(mini_cdm):
    # condition_concept_id: four rows of 201826 (domain "Condition",
    # matches), one NULL, one 99999 (absent from CONCEPT). Under a
    # naive "unmatched or wrong domain" reading (the brief's version)
    # this would report 2 violations (the NULL and the orphan). Per
    # the SQL's LEFT JOIN + `co.concept_id != 0` guard, neither an
    # orphan nor a NULL ever joins to a concept row, so the WHERE
    # clause drops both: 0 violations. The denominator is the whole
    # table (6 rows), not just the non-null ones (5).
    result = _run(
        mini_cdm,
        "fkDomain",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.num_denominator_rows == 6
    assert result.num_violated_rows == 0


def test_fk_domain_passes_when_every_concept_matches(mini_cdm):
    result = _run(
        mini_cdm,
        "fkDomain",
        "VISIT_OCCURRENCE",
        "visit_concept_id",
        value="Visit",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 3


def test_fk_domain_flags_a_concept_of_the_wrong_domain(tmp_path):
    # A concept id that genuinely exists in CONCEPT but belongs to a
    # different domain than expected IS a violation -- unlike an
    # orphan or a NULL. concept 55501 exists with domain "Visit"; the
    # field being checked declares domain "Condition".
    ctx = _write_tables(
        tmp_path,
        CONCEPT=pl.DataFrame(
            {
                "concept_id": [201826, 55501],
                "domain_id": ["Condition", "Visit"],
            }
        ),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {
                "condition_concept_id": [
                    201826,
                    55501,
                    None,
                    99999,
                    0,
                ]
            },
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "fkDomain",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 1


def test_fk_domain_ignores_sentinel_zero_even_if_concept_zero_exists(
    tmp_path,
):
    # Pins the `co.concept_id != 0` guard specifically: if CONCEPT
    # happens to carry an explicit id-0 row of the wrong domain, a
    # field value of 0 must still NOT be flagged. Without this guard,
    # a plain "matched and wrong domain" predicate would count it.
    ctx = _write_tables(
        tmp_path,
        CONCEPT=pl.DataFrame({"concept_id": [0], "domain_id": ["Visit"]}),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_concept_id": [0]},
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "fkDomain",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.num_denominator_rows == 1
    assert result.num_violated_rows == 0


# --- fkClass -------------------------------------------------------
#
# Same shape as fkDomain, over concept_class_id instead of domain_id.


def test_fk_class_checks_the_concept_class(mini_cdm):
    result = _run(
        mini_cdm,
        "fkClass",
        "VISIT_OCCURRENCE",
        "visit_concept_id",
        value="Visit",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 3


def test_fk_class_flags_a_concept_of_the_wrong_class(tmp_path):
    ctx = _write_tables(
        tmp_path,
        CONCEPT=pl.DataFrame(
            {
                "concept_id": [201826, 55501],
                "concept_class_id": ["Clinical Finding", "Visit"],
            }
        ),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_concept_id": [201826, 55501, 99999]},
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "fkClass",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Clinical Finding",
    )
    assert result.num_denominator_rows == 3
    assert result.num_violated_rows == 1


# --- isStandardValidConcept -----------------------------------------
#
# field_is_standard_valid_concept.sql INNER JOINs CONCEPT (not a LEFT
# JOIN): a concept id absent from CONCEPT produces no row at all in
# the join, so -- unlike fkDomain/fkClass with their LEFT JOIN -- it
# is simply never reachable as a violation. The denominator instead
# filters on `cdmTable.@cdmFieldName IS NOT NULL` over the CDM table
# directly, so orphan concept ids ARE counted in the denominator
# while being structurally unable to appear as violated.


def test_is_standard_valid_concept_orphan_and_null_do_not_count(mini_cdm):
    # 99999 is absent from CONCEPT entirely. Under a LEFT-JOIN-style
    # "unmatched counts as invalid" reading (the brief's version) this
    # would be 1 violation. Per the actual INNER JOIN, it cannot
    # appear in the violated-rows subquery at all: 0 violations.
    result = _run(
        mini_cdm,
        "isStandardValidConcept",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 0


def test_is_standard_valid_concept_flags_a_deprecated_concept(tmp_path):
    # concept 4181412: standard_concept NULL, invalid_reason "D" --
    # exists in CONCEPT (so the INNER JOIN reaches it) but fails the
    # standard/valid predicate. concept 8507 is clean. 55555 is an
    # orphan (present in the denominator via its non-null field value,
    # but structurally excluded from ever being counted violated).
    ctx = _write_tables(
        tmp_path,
        CONCEPT=pl.DataFrame(
            {
                "concept_id": [4181412, 8507],
                "standard_concept": [None, "S"],
                "invalid_reason": ["D", None],
            },
            schema_overrides={
                "standard_concept": pl.Utf8,
                "invalid_reason": pl.Utf8,
            },
        ),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_concept_id": [4181412, 8507, None, 55555]},
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "isStandardValidConcept",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 3
    assert result.num_violated_rows == 1


def test_is_standard_valid_concept_ignores_sentinel_zero(tmp_path):
    # Pins the `co.concept_id != 0` guard: even if CONCEPT carries an
    # explicit id-0 row that would otherwise fail the standard/valid
    # predicate, a field value of 0 must not be flagged.
    ctx = _write_tables(
        tmp_path,
        CONCEPT=pl.DataFrame(
            {
                "concept_id": [0],
                "standard_concept": ["C"],
                "invalid_reason": [None],
            },
            schema_overrides={
                "standard_concept": pl.Utf8,
                "invalid_reason": pl.Utf8,
            },
        ),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_concept_id": [0]},
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "isStandardValidConcept",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 1
    assert result.num_violated_rows == 0


# --- standardConceptRecordCompleteness / sourceConceptRecordCompleteness
#
# field_concept_record_completeness.sql never touches CONCEPT. Its
# core rule is `cdmTable.@cdmFieldName = 0`; NULL is only counted (as
# both a violation and part of the denominator) for a fixed list of
# concept id fields that have a companion @xxx_source_value column,
# and then only when that companion is itself non-null. A field with
# no companion in that list -- e.g. condition_concept_id -- is judged
# purely on `= 0`; NULL there is neither a violation nor counted.


def test_standard_concept_record_completeness_counts_only_zero(mini_cdm):
    # condition_concept_id has no source-value companion in the
    # template's field-name list, so the NULL row (id 104, first
    # occurrence) is NOT counted at all, and there are no zero values
    # in the fixture: denominator 5 (non-null), 0 violations. This
    # replaces the brief's expectation of (6, 1), which assumed NULL
    # unconditionally counts as both present and violated -- true only
    # for the field-name-specific extension list this field is not on.
    result = _run(
        mini_cdm,
        "standardConceptRecordCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 0


def test_record_completeness_flags_the_zero_sentinel(tmp_path):
    # Pins the `= 0` rule itself for a plain (non-extension-list)
    # field: 0 is a violation, NULL is invisible to both counts.
    ctx = _write_tables(
        tmp_path,
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_concept_id": [0, 201826, None]},
            schema_overrides={"condition_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "standardConceptRecordCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 2
    assert result.num_violated_rows == 1


def test_record_completeness_source_value_extension(tmp_path):
    # route_concept_id is on the template's field-name extension list,
    # paired with route_source_value:
    #   OR (cdmTable.ROUTE_CONCEPT_ID IS NULL
    #       AND cdmTable.route_source_value IS NOT NULL)
    # A NULL route_concept_id with a populated route_source_value is
    # both counted and violated; a NULL route_concept_id with a NULL
    # source value is invisible to both counts, same as the
    # no-companion case above.
    ctx = _write_tables(
        tmp_path,
        DRUG_EXPOSURE=pl.DataFrame(
            {
                "route_concept_id": [None, 0, 201826, None],
                "route_source_value": ["ORAL", None, "IV", None],
            },
            schema_overrides={"route_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "standardConceptRecordCompleteness",
        "DRUG_EXPOSURE",
        "route_concept_id",
    )
    assert result.num_denominator_rows == 3
    assert result.num_violated_rows == 2


def test_record_completeness_unit_concept_excludes_dose_era(tmp_path):
    # unit_concept_id/unit_source_concept_id get the source-value
    # extension on every table EXCEPT DOSE_ERA:
    #   {@cdmTableName != 'DOSE_ERA' & (@cdmFieldName ==
    #    'UNIT_CONCEPT_ID' | ...)}?{OR (... IS NULL AND
    #    unit_source_value IS NOT NULL)}
    # On DOSE_ERA the extension must not apply, so a NULL
    # unit_concept_id with a populated unit_source_value is neither
    # counted nor violated there, unlike on another table.
    dose_era = _write_tables(
        tmp_path,
        DOSE_ERA=pl.DataFrame(
            {
                "unit_concept_id": [None],
                "unit_source_value": ["mg"],
            },
            schema_overrides={"unit_concept_id": pl.Int64},
        ),
    )
    dose_era_result = _run(
        dose_era,
        "standardConceptRecordCompleteness",
        "DOSE_ERA",
        "unit_concept_id",
    )
    assert dose_era_result.num_denominator_rows == 0
    assert dose_era_result.num_violated_rows == 0

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    drug_exposure = _write_tables(
        other_dir,
        DRUG_EXPOSURE=pl.DataFrame(
            {
                "unit_concept_id": [None],
                "unit_source_value": ["mg"],
            },
            schema_overrides={"unit_concept_id": pl.Int64},
        ),
    )
    drug_exposure_result = _run(
        drug_exposure,
        "standardConceptRecordCompleteness",
        "DRUG_EXPOSURE",
        "unit_concept_id",
    )
    assert drug_exposure_result.num_denominator_rows == 1
    assert drug_exposure_result.num_violated_rows == 1


def test_record_completeness_gender_concept_only_extends_on_provider(
    tmp_path,
):
    # gender_concept_id/gender_source_concept_id get the source-value
    # extension only on PROVIDER:
    #   {@cdmTableName == 'PROVIDER' & (@cdmFieldName ==
    #    'GENDER_CONCEPT_ID' | ...)}?{OR (... IS NULL AND
    #    gender_source_value IS NOT NULL)}
    # On PERSON, the same field name must NOT get the extension.
    provider = _write_tables(
        tmp_path,
        PROVIDER=pl.DataFrame(
            {
                "gender_concept_id": [None],
                "gender_source_value": ["M"],
            },
            schema_overrides={"gender_concept_id": pl.Int64},
        ),
    )
    provider_result = _run(
        provider,
        "standardConceptRecordCompleteness",
        "PROVIDER",
        "gender_concept_id",
    )
    assert provider_result.num_denominator_rows == 1
    assert provider_result.num_violated_rows == 1

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    person = _write_tables(
        other_dir,
        PERSON=pl.DataFrame(
            {
                "gender_concept_id": [None],
                "gender_source_value": ["M"],
            },
            schema_overrides={"gender_concept_id": pl.Int64},
        ),
    )
    person_result = _run(
        person,
        "standardConceptRecordCompleteness",
        "PERSON",
        "gender_concept_id",
    )
    assert person_result.num_denominator_rows == 0
    assert person_result.num_violated_rows == 0


def test_source_concept_record_completeness_is_the_same_rule(tmp_path):
    # sourceConceptRecordCompleteness shares field_concept_record_
    # completeness.sql with standardConceptRecordCompleteness -- the
    # two differ only in which field a given check instance targets,
    # not in behaviour. condition_source_concept_id has no companion
    # in the extension list either.
    ctx = _write_tables(
        tmp_path,
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"condition_source_concept_id": [0, 201826, None]},
            schema_overrides={"condition_source_concept_id": pl.Int64},
        ),
    )
    result = _run(
        ctx,
        "sourceConceptRecordCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_source_concept_id",
    )
    assert result.num_denominator_rows == 2
    assert result.num_violated_rows == 1
