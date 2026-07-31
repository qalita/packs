"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/concept_plausible_gender.sql
  inst/sql/sql_server/concept_plausible_gender_use_descendants.sql
  inst/sql/sql_server/concept_plausible_unit_concept_ids.sql
See the NOTICE file at the pack root.
"""

from typing import List, Optional

import polars as pl

from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable

# concept_plausible_gender.sql and concept_plausible_gender_use_
# descendants.sql both resolve params["value"] through the literal
# ternary ``{@plausibleGender == 'Male'} ? {8507} : {8532}`` -- the
# CSV cell (and therefore params["value"]) is the string "Male" or
# "Female", never a gender concept id. Anything that is not literally
# "Male" takes the Female branch, mirroring the SQL's own lack of a
# validated else-case.
_GENDER_MALE = 8507
_GENDER_FEMALE = 8532

# The sentinel meaning "no unit is plausible for this concept" in
# plausibleUnitConceptIds, checked against the RAW, untouched
# params["value"] string -- ``'@plausibleUnitConceptIds' = '-1'`` in
# the SQL compares the whole parameter, not a parsed list member.
_NO_UNIT_SENTINEL = "-1"

# unit_concept_id == 0 is deliberately excluded from both the
# denominator and the violated count everywhere in
# concept_plausible_unit_concept_ids.sql -- it is reserved for
# standardConceptRecordCompleteness (see that SQL file's own comment).
_UNMAPPED_UNIT = 0


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


def _int_list(raw: str) -> List[int]:
    values = []
    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if token:
            try:
                values.append(int(token))
            except ValueError:
                continue
    return values


def _concept_id(chk) -> Optional[int]:
    """A single concept id, for the two checks that test `= @conceptId`."""
    raw = chk.params.get("conceptId", "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _concept_ids(chk) -> List[int]:
    """A list of concept ids, for plausibleGenderUseDescendants'
    ``ancestor_concept_id IN (@conceptId)``.

    3 of the 4 real catalog instances of this check carry a
    comma-separated conceptId (verified against
    ``load_catalog('5.4')``); parsing it as a single int, as
    plausibleGender legitimately can, would silently degrade most
    real instances to NOT_APPLICABLE.
    """
    return _int_list(chk.params.get("conceptId", ""))


def _expected_gender(chk) -> Optional[int]:
    value = chk.params.get("value", "").strip()
    if not value:
        return None
    return _GENDER_MALE if value == "Male" else _GENDER_FEMALE


def _plausible_gender(ctx, chk, use_descendants: bool) -> CheckResult:
    """Shared body of plausibleGender / plausibleGenderUseDescendants.

    Both upstream templates share one asymmetry that the denominator
    subquery has NO JOIN to person at all -- only the violated-rows
    subquery does, and it is an INNER JOIN. So:

      * a row whose person_id has no match in PERSON inflates the
        denominator but can never be counted as violated;
      * a NULL gender_concept_id is never a violation, because SQL's
        ``p.gender_concept_id <> @expected`` is UNKNOWN (falsy), not
        TRUE, on NULL -- and Polars' ``!=`` propagates NULL the same
        way, so no extra ``is_not_null()`` guard is needed for that.

    The two templates differ in exactly one other respect: plausible-
    Gender matches ``cdmTable.@cdmFieldName = @conceptId`` (a single
    equality), while plausibleGenderUseDescendants matches
    ``ca.ancestor_concept_id IN (@conceptId)`` against
    CONCEPT_ANCESTOR (a real IN list, descendant-expanded).
    """
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, chk.cdm_field_name):
        return not_applicable(
            f"column {chk.qualified_field} is absent from the source"
        )
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")

    expected_gender = _expected_gender(chk)
    if expected_gender is None:
        return not_applicable("no plausible gender on the check instance")

    column = pl.col(chk.cdm_field_name)
    frame = ctx.table(chk.cdm_table_name)

    if use_descendants:
        concept_ids = _concept_ids(chk)
        if not concept_ids:
            return not_applicable("no concept id on the check instance")
        if not ctx.has_table("CONCEPT_ANCESTOR"):
            return not_applicable("CONCEPT_ANCESTOR is absent from the source")
        descendants = (
            ctx.table("CONCEPT_ANCESTOR")
            .filter(pl.col("ancestor_concept_id").is_in(concept_ids))
            .select(pl.col("descendant_concept_id").alias(chk.cdm_field_name))
            .unique()
        )
        base = frame.join(descendants, on=chk.cdm_field_name, how="semi")
    else:
        concept_id = _concept_id(chk)
        if concept_id is None:
            return not_applicable("no concept id on the check instance")
        base = frame.filter(column == concept_id)

    # Denominator: the base rows alone, unfiltered by PERSON.
    total = _row_count(base)

    people = ctx.table("PERSON").select(
        pl.col("person_id"),
        pl.col("gender_concept_id").alias("_gender"),
    )
    violated = _row_count(
        base.join(people, on="person_id", how="inner").filter(
            pl.col("_gender") != expected_gender
        )
    )
    return counted(violated, total)


@register("plausibleGender")
def plausible_gender(ctx, chk) -> CheckResult:
    return _plausible_gender(ctx, chk, use_descendants=False)


@register("plausibleGenderUseDescendants")
def plausible_gender_use_descendants(ctx, chk) -> CheckResult:
    return _plausible_gender(ctx, chk, use_descendants=True)


@register("plausibleUnitConceptIds")
def plausible_unit_concept_ids(ctx, chk) -> CheckResult:
    """Rows for a concept carrying an implausible unit.

    concept_plausible_unit_concept_ids.sql's denominator predicate is
    ``unit_concept_id != 0 OR unit_concept_id IS NULL`` -- NULL is
    explicitly folded INTO the denominator (that ``OR IS NULL`` exists
    for exactly that reason: a bare ``!= 0`` already excludes NULL,
    same as Polars). The violated-rows subquery instead guards with
    ``unit_concept_id IS NOT NULL``, so a NULL unit is counted in the
    denominator but can never be a violation. unit_concept_id = 0 is
    excluded from both buckets entirely, deferred to
    standardConceptRecordCompleteness.

    When params["value"] is the literal string "-1" (not merely
    containing -1), a different branch applies: ANY non-null, non-zero
    unit is a violation, including a literal unit_concept_id of -1 --
    unlike the general ``NOT IN (list, 0)`` branch, where -1 could
    appear as an allowed list member.
    """
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, chk.cdm_field_name):
        return not_applicable(
            f"column {chk.qualified_field} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, "unit_concept_id"):
        return not_applicable(
            f"{chk.cdm_table_name} has no unit_concept_id column"
        )
    concept_id = _concept_id(chk)
    if concept_id is None:
        return not_applicable("no concept id on the check instance")
    raw_value = chk.params.get("value", "").strip()
    if not raw_value:
        return not_applicable("no plausible unit concept ids")

    unit = pl.col("unit_concept_id")
    base = ctx.table(chk.cdm_table_name).filter(
        pl.col(chk.cdm_field_name) == concept_id
    )

    denominator_frame = base.filter((unit != _UNMAPPED_UNIT) | unit.is_null())
    total = _row_count(denominator_frame)

    if raw_value == _NO_UNIT_SENTINEL:
        violated_pred = unit.is_not_null() & (unit != _UNMAPPED_UNIT)
    else:
        allowed = _int_list(raw_value)
        violated_pred = unit.is_not_null() & ~unit.is_in(
            allowed + [_UNMAPPED_UNIT]
        )
    violated = _row_count(base.filter(violated_pred))
    return counted(violated, total)
