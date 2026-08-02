"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/field_cdm_field.sql
  inst/sql/sql_server/field_is_not_nullable.sql
  inst/sql/sql_server/field_cdm_datatype.sql
  inst/sql/sql_server/field_is_primary_key.sql
  inst/sql/sql_server/field_measure_value_completeness.sql
  inst/sql/sql_server/field_source_value_completeness.sql
  inst/sql/sql_server/field_plausible_value_low.sql
  inst/sql/sql_server/field_plausible_value_high.sql
  inst/sql/sql_server/field_plausible_start_before_end.sql
  inst/sql/sql_server/is_foreign_key.sql
  inst/sql/sql_server/field_plausible_after_birth.sql
  inst/sql/sql_server/field_plausible_before_death.sql
  inst/sql/sql_server/field_plausible_during_life.sql
  inst/sql/sql_server/field_plausible_temporal_after.sql
  inst/sql/sql_server/field_within_visit_dates.sql
  inst/sql/sql_server/field_fk_domain.sql
  inst/sql/sql_server/field_fk_class.sql
  inst/sql/sql_server/field_is_standard_valid_concept.sql
  inst/sql/sql_server/field_concept_record_completeness.sql
See the NOTICE file at the pack root.
"""

from typing import Optional

import polars as pl

from omop_dqd.checks._common import (
    _row_count,
    as_date,
    guard,
    require_columns,
)
from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable

# Concrete Polars dtypes making up "integer" and "float", used by
# cdmDatatype. pl.INTEGER_DTYPES / pl.FLOAT_DTYPES are deprecated on
# the installed Polars version (1.43.1) and emit a DeprecationWarning
# on access, so the explicit sets are spelled out here instead.
_INTEGER_DTYPES = frozenset(
    {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    }
)
_FLOAT_DTYPES = frozenset({pl.Float32, pl.Float64})


def _count_where(
    ctx, chk, predicate: pl.Expr, denominator: Optional[pl.Expr] = None
) -> CheckResult:
    """Count rows matching predicate, over an optional denominator."""
    frame = ctx.table(chk.cdm_table_name)
    if denominator is not None:
        frame = frame.filter(denominator)
    total = _row_count(frame)
    violated = _row_count(frame.filter(predicate))
    return counted(violated, total)


@register("cdmField")
def cdm_field(ctx, chk) -> CheckResult:
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    present = ctx.has_column(chk.cdm_table_name, chk.cdm_field_name)
    return counted(0 if present else 1, 1)


@register("cdmDatatype")
def cdm_datatype(ctx, chk) -> CheckResult:
    """Non-null values that are not whole numbers.

    Upstream only ever runs this for fields declared "integer" (see
    the evaluationFilter in Check_Descriptions.csv), and counts
    non-null rows that are non-numeric or numeric-with-a-decimal-
    point, over a denominator of every row in the table.

    In parquet the declared type is enforced by the file, so an
    integer dtype can hold no violation; float and string columns
    can.

    KNOWN DIVERGENCE. On a string column, upstream's test is SQL
    Server's ``ISNUMERIC(@cdmFieldName) = 0 OR @cdmFieldName LIKE
    '%.%'``; ours is a strict ``cast(pl.Int64)`` that fails. The two
    disagree on values ISNUMERIC accepts but Int64 does not:

      * a leading ``+`` (``"+12"``) -- ISNUMERIC says numeric, our
        cast fails, so we count a violation where upstream does not;
      * scientific notation (``"1e5"``) -- likewise;
      * currency and thousands notation (``"$12"``, ``"1,234"``) --
        ISNUMERIC accepts these as *money*, our cast fails, so again
        we flag what upstream does not.

    Every divergence is in the same direction: we are stricter, never
    laxer. A value in any of these forms is not a plausible OMOP
    integer id in the first place, so flagging it is the more useful
    behaviour -- but it is a divergence from the SQL, not a
    reproduction of it, and it is the only one in this module.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    dtype = ctx.dtypes(chk.cdm_table_name)[field]
    column = pl.col(field)

    if dtype in _INTEGER_DTYPES:
        return counted(0, total)
    if dtype in _FLOAT_DTYPES:
        violated = column.is_not_null() & (column != column.floor())
    elif dtype == pl.Utf8:
        parsed = column.str.strip_chars().cast(pl.Int64, strict=False)
        violated = column.is_not_null() & parsed.is_null()
    else:
        return not_applicable(f"dtype {dtype} cannot be checked as an integer")
    return counted(_row_count(frame.filter(violated)), total)


@register("isRequired")
def is_required(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    return _count_where(ctx, chk, pl.col(chk.cdm_field_name).is_null())


@register("measureValueCompleteness")
def measure_value_completeness(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    return _count_where(ctx, chk, pl.col(chk.cdm_field_name).is_null())


@register("sourceValueCompleteness")
def source_value_completeness(ctx, chk) -> CheckResult:
    """Distinct source values that are unmapped.

    Upstream counts DISTINCT source values whose companion standard
    concept field is 0, over a denominator of distinct non-null
    source values plus one bucket for NULL if any row has one. Both
    numbers are counts of values, not of rows.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    standard_field = chk.params.get("standardConceptFieldName", "").lower()
    if not standard_field or not ctx.has_column(
        chk.cdm_table_name, standard_field
    ):
        return not_applicable(
            f"companion field {standard_field!r} unavailable"
        )
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)

    violated = frame.filter(pl.col(standard_field) == 0).select(field).unique()
    distinct_non_null = _row_count(frame.select(field).drop_nulls().unique())
    has_null = _row_count(frame.filter(pl.col(field).is_null())) > 0
    denominator = distinct_non_null + (1 if has_null else 0)
    return counted(_row_count(violated), denominator)


@register("isPrimaryKey")
def is_primary_key(ctx, chk) -> CheckResult:
    """Rows whose key value is not unique.

    Upstream counts EVERY row of a duplicated group, not the excess
    beyond the first: a value appearing twice contributes 2, not 1.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    duplicated = (
        frame.group_by(field)
        .agg(pl.len().alias("_n"))
        .filter(pl.col("_n") > 1)
        .select(field)
    )
    violated = frame.join(duplicated, on=field, how="semi")
    return counted(_row_count(violated), total)


def _bound(chk) -> Optional[float]:
    raw = chk.params.get("value", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


@register("plausibleValueLow")
def plausible_value_low(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    bound = _bound(chk)
    if bound is None:
        return not_applicable("non-numeric plausible bound")
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column < bound, denominator=column.is_not_null()
    )


@register("plausibleValueHigh")
def plausible_value_high(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    bound = _bound(chk)
    if bound is None:
        return not_applicable("non-numeric plausible bound")
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column > bound, denominator=column.is_not_null()
    )


@register("plausibleStartBeforeEnd")
def plausible_start_before_end(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    end_field = chk.params.get("plausibleStartBeforeEndFieldName", "").lower()
    if not end_field or not ctx.has_column(chk.cdm_table_name, end_field):
        return not_applicable(f"end field {end_field!r} unavailable")
    start = pl.col(chk.cdm_field_name)
    end = pl.col(end_field)
    return _count_where(
        ctx,
        chk,
        start > end,
        denominator=start.is_not_null() & end.is_not_null(),
    )


# --- Join family --------------------------------------------------
#
# Each check below joins a second CDM table. Where the upstream SQL's
# denominator subquery differs from its violated-rows subquery (e.g.
# no non-null filter, or an extra join), that asymmetry is preserved
# here rather than reusing a single generic "filter then join" helper
# for every check.


def person_birth_date(ctx) -> pl.LazyFrame:
    """PERSON birth dates, falling back to year/month/day parts.

    Mirrors field_plausible_after_birth.sql's
    COALESCE(birth_datetime, CONCAT(year, month, day)), defaulting
    month and day to 1 (1 January) when absent.
    """
    person = ctx.table("PERSON")
    columns = ctx.columns("PERSON")
    composed = pl.date(
        pl.col("year_of_birth").cast(pl.Int32),
        (
            pl.col("month_of_birth").cast(pl.Int32).fill_null(1)
            if "month_of_birth" in columns
            else pl.lit(1, dtype=pl.Int32)
        ),
        (
            pl.col("day_of_birth").cast(pl.Int32).fill_null(1)
            if "day_of_birth" in columns
            else pl.lit(1, dtype=pl.Int32)
        ),
    )
    if "birth_datetime" in columns:
        birth = as_date(ctx, "PERSON", "birth_datetime").fill_null(composed)
    else:
        birth = composed
    return person.select(pl.col("person_id"), birth.alias("birth_date"))


def _join_violation_count(
    ctx, chk, other: pl.LazyFrame, on: str, predicate: pl.Expr
) -> CheckResult:
    """Count rows of the CDM table violating a cross-table predicate.

    The denominator is every row with a non-null checked field, with
    no join applied -- this matches field_plausible_after_birth.sql's
    denominator subquery, `WHERE cdmTable.@cdmFieldName IS NOT NULL`,
    which has no JOIN at all. Only use this helper for checks whose
    upstream denominator subquery is shaped that way; several of the
    checks below have a differently-shaped denominator and compute it
    directly instead.
    """
    column = pl.col(chk.cdm_field_name)
    base = ctx.table(chk.cdm_table_name).filter(column.is_not_null())
    total = _row_count(base)
    violated = _row_count(
        base.join(other, on=on, how="inner").filter(predicate)
    )
    return counted(violated, total)


@register("isForeignKey")
def is_foreign_key(ctx, chk) -> CheckResult:
    """Rows whose value has no match in a parent table's key column.

    Upstream's denominator subquery
    (``SELECT COUNT_BIG(*) FROM @schema.@cdmTableName cdmTable``) has
    no WHERE clause at all -- it is the CDM table's full row count,
    not restricted to rows with a non-null checked field.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    parent_table = chk.params.get("fkTableName", "").upper()
    parent_field = chk.params.get("fkFieldName", "").lower()
    if not ctx.has_table(parent_table):
        return not_applicable(f"referenced table {parent_table} is absent")
    if not ctx.has_column(parent_table, parent_field):
        return not_applicable(
            f"referenced column {parent_table}.{parent_field} is absent"
        )
    child = ctx.table(chk.cdm_table_name)
    total = _row_count(child)
    column = pl.col(chk.cdm_field_name)
    parent_keys = (
        ctx.table(parent_table)
        .select(pl.col(parent_field).alias(chk.cdm_field_name))
        .drop_nulls()
        .unique()
    )
    orphans = child.filter(column.is_not_null()).join(
        parent_keys, on=chk.cdm_field_name, how="anti"
    )
    return counted(_row_count(orphans), total)


@register("plausibleAfterBirth")
def plausible_after_birth(ctx, chk) -> CheckResult:
    """Events dated before the person's birth.

    Matches field_plausible_after_birth.sql exactly: an INNER JOIN to
    PERSON in the violated-rows subquery, and a denominator that only
    filters on the checked field being non-null (no join).
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")
    missing = require_columns(
        ctx, "PERSON", "person_id", "year_of_birth"
    ) or require_columns(ctx, chk.cdm_table_name, "person_id")
    if missing:
        return missing
    return _join_violation_count(
        ctx,
        chk,
        person_birth_date(ctx),
        on="person_id",
        predicate=as_date(ctx, chk.cdm_table_name, chk.cdm_field_name)
        < pl.col("birth_date"),
    )


def _death_dates(ctx) -> pl.LazyFrame:
    """DEATH rows keyed by person_id, death_date left possibly null.

    Only person_id is required to be non-null (a null person_id can
    never join to anything, in SQL or here). death_date is
    deliberately NOT dropped: neither upstream denominator subquery
    requires it -- field_plausible_before_death.sql's denominator
    only filters on the *checked field*, and field_plausible_during_
    life.sql's denominator only tests person_id membership. A DEATH
    row with a null death_date still means that person is dead and
    still belongs in both denominators; only the *violated-rows*
    comparison against death_date naturally excludes it (NULL
    comparisons are falsy in both SQL and Polars).
    """
    return (
        ctx.table("DEATH")
        .select(
            pl.col("person_id"),
            as_date(ctx, "DEATH", "death_date").alias("death_date"),
        )
        .drop_nulls(subset=["person_id"])
    )


@register("plausibleBeforeDeath")
def plausible_before_death(ctx, chk) -> CheckResult:
    """Events dated more than 60 days after the person's death.

    field_plausible_before_death.sql's violated-rows predicate is
    ``CAST(cdmTable.@cdmFieldName AS DATE) >
    DATEADD(day, 60, de.death_date)`` -- a 60-day grace period, not a
    bare ``> death_date``. Its denominator subquery re-joins DEATH and
    filters ``cdmTable.@cdmFieldName IS NOT NULL``, so only rows
    belonging to a person who died, with a *non-null checked field*,
    count -- a null death_date does not exclude the row.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("DEATH"):
        return not_applicable("DEATH is absent from the source")
    missing = require_columns(
        ctx, "DEATH", "person_id", "death_date"
    ) or require_columns(ctx, chk.cdm_table_name, "person_id")
    if missing:
        return missing
    column = pl.col(chk.cdm_field_name)
    joined = ctx.table(chk.cdm_table_name).join(
        _death_dates(ctx), on="person_id", how="inner"
    )
    denominator_frame = joined.filter(column.is_not_null())
    total = _row_count(denominator_frame)
    grace_end = pl.col("death_date").dt.offset_by("60d")
    violated = _row_count(
        denominator_frame.filter(
            as_date(ctx, chk.cdm_table_name, chk.cdm_field_name) > grace_end
        )
    )
    return counted(violated, total)


@register("plausibleDuringLife")
def plausible_during_life(ctx, chk) -> CheckResult:
    """Events dated more than 60 days after the person's death.

    Not a delegation to plausibleBeforeDeath, despite sharing the
    same violation rule: field_plausible_during_life.sql's
    denominator is
    ``WHERE person_id IN (SELECT person_id FROM death)`` over the
    *unfiltered* CDM table -- it does not require the checked field
    to be non-null, unlike field_plausible_before_death.sql's
    ``JOIN death ... WHERE cdmTable.@cdmFieldName IS NOT NULL``. The
    violated-rows subquery also has no explicit non-null filter, but
    NULL comparisons are falsy in both SQL and Polars, so nulls are
    excluded from violations either way.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("DEATH"):
        return not_applicable("DEATH is absent from the source")
    missing = require_columns(
        ctx, "DEATH", "person_id", "death_date"
    ) or require_columns(ctx, chk.cdm_table_name, "person_id")
    if missing:
        return missing
    death = _death_dates(ctx)
    frame = ctx.table(chk.cdm_table_name)
    dead_person_ids = death.select("person_id").unique()
    denominator_frame = frame.join(dead_person_ids, on="person_id", how="semi")
    total = _row_count(denominator_frame)
    grace_end = pl.col("death_date").dt.offset_by("60d")
    violated = _row_count(
        frame.join(death, on="person_id", how="inner").filter(
            as_date(ctx, chk.cdm_table_name, chk.cdm_field_name) > grace_end
        )
    )
    return counted(violated, total)


@register("plausibleTemporalAfter")
def plausible_temporal_after(ctx, chk) -> CheckResult:
    """Rows whose date does not occur on/after a reference date.

    field_plausible_temporal_after.sql's denominator subquery has no
    WHERE clause at all: it is the whole CDM table, unconditionally.

    The vendored catalog only ever pairs this check with two
    reference-table shapes (checked against
    OMOP_CDMv5.4_Field_Level.csv):

    * ``plausibleTemporalAfterTableName == 'PERSON'``: the reference
      date is PERSON's birth_datetime, defaulting to *1 June* of
      year_of_birth when null --
      ``CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)``
      -- unlike person_birth_date()'s 1 January default used by
      plausibleAfterBirth.
    * ``plausibleTemporalAfterTableName == cdmTableName``: the
      reference is another column of the same row, no join.

    Any other combination is not something the vendored catalog ever
    produces, and upstream's own WHERE clause would not evaluate it
    correctly either: its non-PERSON branch reads
    ``CAST(cdmTable.@plausibleTemporalAfterFieldName AS DATE)`` --
    the *CDM table's own* column of that name, not the joined table's
    -- so it only resolves when that column happens to exist on the
    CDM table itself.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    other_table = chk.params.get("plausibleTemporalAfterTableName", "").upper()
    other_field = chk.params.get("plausibleTemporalAfterFieldName", "").lower()
    if not other_table or not ctx.has_table(other_table):
        return not_applicable(f"table {other_table} is absent")

    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    cdm_field = as_date(ctx, chk.cdm_table_name, chk.cdm_field_name)

    if other_table == "PERSON":
        if not ctx.has_column(chk.cdm_table_name, "person_id"):
            return not_applicable(f"{chk.cdm_table_name} has no person_id")
        missing = require_columns(ctx, "PERSON", "person_id", "year_of_birth")
        if missing:
            return missing
        person_columns = ctx.columns("PERSON")
        birth_field = (
            as_date(ctx, "PERSON", other_field)
            if other_field in person_columns
            else pl.lit(None, dtype=pl.Date)
        )
        fallback = pl.date(pl.col("year_of_birth").cast(pl.Int32), 6, 1)
        reference = ctx.table("PERSON").select(
            pl.col("person_id"),
            birth_field.fill_null(fallback).alias("_reference"),
        )
        violated = _row_count(
            frame.join(reference, on="person_id", how="inner").filter(
                pl.col("_reference") > cdm_field
            )
        )
        return counted(violated, total)

    if not ctx.has_column(chk.cdm_table_name, other_field):
        return not_applicable(
            f"{chk.cdm_table_name} has no column {other_field}; "
            "upstream's non-PERSON branch reads it from cdmTable, "
            "not the joined table"
        )
    candidate = frame
    if other_table != chk.cdm_table_name:
        if not ctx.has_column(other_table, "person_id"):
            return not_applicable(
                f"cannot restrict to {other_table} on person_id"
            )
        missing = require_columns(ctx, chk.cdm_table_name, "person_id")
        if missing:
            return missing
        other_ids = ctx.table(other_table).select("person_id").unique()
        candidate = frame.join(other_ids, on="person_id", how="inner")
    other_col = as_date(ctx, chk.cdm_table_name, other_field)
    violated = _row_count(candidate.filter(other_col > cdm_field))
    return counted(violated, total)


@register("withinVisitDates")
def within_visit_dates(ctx, chk) -> CheckResult:
    """Events outside their visit's window, with a 7-day grace period.

    field_within_visit_dates.sql's predicate is
    ``cdmTable.@cdmFieldName < DATEADD(DAY, -7, vo.visit_start_date)
    OR cdmTable.@cdmFieldName > DATEADD(DAY, 7, vo.visit_end_date)``
    -- a +/-7 day grace window, not a bare
    ``[visit_start_date, visit_end_date]`` bound. The denominator
    subquery is an unfiltered INNER JOIN to VISIT_OCCURRENCE (no
    non-null filter on the checked field).
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("VISIT_OCCURRENCE"):
        return not_applicable("VISIT_OCCURRENCE is absent")
    if not ctx.has_column(chk.cdm_table_name, "visit_occurrence_id"):
        return not_applicable(
            f"{chk.cdm_table_name} has no visit_occurrence_id"
        )
    missing = require_columns(
        ctx,
        "VISIT_OCCURRENCE",
        "visit_occurrence_id",
        "visit_start_date",
        "visit_end_date",
    )
    if missing:
        return missing
    visits = ctx.table("VISIT_OCCURRENCE").select(
        pl.col("visit_occurrence_id"),
        as_date(ctx, "VISIT_OCCURRENCE", "visit_start_date").alias(
            "_visit_start"
        ),
        as_date(ctx, "VISIT_OCCURRENCE", "visit_end_date").alias("_visit_end"),
    )
    joined = ctx.table(chk.cdm_table_name).join(
        visits, on="visit_occurrence_id", how="inner"
    )
    total = _row_count(joined)
    event = as_date(ctx, chk.cdm_table_name, chk.cdm_field_name)
    early_bound = pl.col("_visit_start").dt.offset_by("-7d")
    late_bound = pl.col("_visit_end").dt.offset_by("7d")
    violated = _row_count(
        joined.filter((event < early_bound) | (event > late_bound))
    )
    return counted(violated, total)


# --- Vocabulary family -----------------------------------------------
#
# fkDomain, fkClass and isStandardValidConcept join CONCEPT directly
# and must degrade to NOT_APPLICABLE when it is absent.
# standardConceptRecordCompleteness and sourceConceptRecordCompleteness
# are the exception: their shared template, field_concept_record_
# completeness.sql, never references CONCEPT at all -- see
# _concept_record_completeness below -- so they use guard(), not
# _vocabulary_guard().


def _vocabulary_guard(ctx, chk, *concept_columns) -> Optional[CheckResult]:
    """guard(), plus degrading to NOT_APPLICABLE without CONCEPT.

    `concept_columns` are the CONCEPT attribute columns the calling
    check selects, on top of the concept_id it always joins on. A
    CONCEPT table present but missing one of them is as unrunnable as
    no CONCEPT at all, and must degrade the same way rather than
    raising into an ERROR.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("CONCEPT"):
        return not_applicable(
            "the OMOP vocabulary (CONCEPT) is absent from the source"
        )
    return require_columns(ctx, "CONCEPT", "concept_id", *concept_columns)


def _concept_attribute(ctx, attribute: str) -> pl.LazyFrame:
    """CONCEPT reduced to a join key plus one renamed attribute column."""
    return ctx.table("CONCEPT").select(
        pl.col("concept_id").alias("_concept_id"),
        pl.col(attribute).alias("_attribute"),
    )


def _domain_or_class_violation(
    ctx, chk, attribute: str, expected: str
) -> CheckResult:
    """Rows whose *matched* concept carries the wrong domain/class.

    field_fk_domain.sql and field_fk_class.sql share this shape: a
    LEFT JOIN to CONCEPT, with violated rows filtered by
    ``co.concept_id != 0 AND co.domain_id NOT IN ('@fkDomain')``
    (field_fk_class.sql: ``co.concept_class_id != '@fkClass'``).
    Because the join is a LEFT JOIN, a concept id absent from CONCEPT
    -- or literally 0 -- produces a NULL co.concept_id on that row;
    SQL's ``NULL != 0`` is unknown, not true, so the WHERE clause
    drops it. An orphan or sentinel-0 concept id is therefore NOT a
    violation of this check -- only a concept that genuinely exists in
    CONCEPT with the wrong domain/class is.

    The denominator subquery has no WHERE clause at all
    (``SELECT COUNT_BIG(*) FROM @schema.@cdmTableName cdmTable``): it
    is the table's full row count, not restricted to non-null field
    values.

    The join uses ``coalesce=False`` so the matched concept id survives
    as its own column even when NULL (unmatched) -- Polars' default
    join behaviour merges same-role join key columns away, which would
    make an orphan (non-null field, no match) indistinguishable from a
    genuine match.
    """
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    joined = frame.join(
        _concept_attribute(ctx, attribute),
        left_on=field,
        right_on="_concept_id",
        how="left",
        coalesce=False,
    )
    violated_pred = (
        pl.col("_concept_id").is_not_null()
        & (pl.col("_concept_id") != 0)
        & (pl.col("_attribute") != expected)
    )
    return counted(_row_count(joined.filter(violated_pred)), total)


@register("fkDomain")
def fk_domain(ctx, chk) -> CheckResult:
    skip = _vocabulary_guard(ctx, chk, "domain_id")
    if skip:
        return skip
    expected = chk.params.get("value", "")
    return _domain_or_class_violation(ctx, chk, "domain_id", expected)


@register("fkClass")
def fk_class(ctx, chk) -> CheckResult:
    skip = _vocabulary_guard(ctx, chk, "concept_class_id")
    if skip:
        return skip
    expected = chk.params.get("value", "")
    return _domain_or_class_violation(ctx, chk, "concept_class_id", expected)


@register("isStandardValidConcept")
def is_standard_valid_concept(ctx, chk) -> CheckResult:
    """Non-null concept ids that fail to resolve to a standard, valid concept.

    field_is_standard_valid_concept.sql INNER JOINs CONCEPT -- unlike
    fkDomain/fkClass's LEFT JOIN. A concept id absent from CONCEPT
    therefore produces no row at all in the join, so it can never be
    counted as a violation: this check only flags concept ids that ARE
    found in CONCEPT but are non-standard, invalid, or (defensively,
    matching the SQL's ``co.concept_id != 0``) the literal sentinel 0.

    Its denominator, unlike fkDomain/fkClass, filters on
    ``cdmTable.@cdmFieldName IS NOT NULL`` over the CDM table directly
    -- so an orphan concept id IS counted in the denominator, while
    being structurally unable to ever appear as violated.
    """
    skip = _vocabulary_guard(ctx, chk, "standard_concept", "invalid_reason")
    if skip:
        return skip
    field = chk.cdm_field_name
    column = pl.col(field)
    denominator_frame = ctx.table(chk.cdm_table_name).filter(
        column.is_not_null()
    )
    total = _row_count(denominator_frame)
    concept = ctx.table("CONCEPT").select(
        pl.col("concept_id"),
        pl.col("standard_concept").alias("_standard"),
        pl.col("invalid_reason").alias("_invalid"),
    )
    # The same filtered frame feeds the join. Polars joins do not
    # match NULL keys (nulls_equal defaults to False), exactly as
    # SQL's INNER JOIN does not, so pre-filtering the null field
    # values away cannot change which rows the join produces -- it
    # just saves a second full scan of what is, in a real CDM, one of
    # the largest tables there is.
    joined = denominator_frame.join(
        concept, left_on=field, right_on="concept_id", how="inner"
    )
    violated_pred = (column != 0) & (
        (pl.col("_standard") != "S")
        | pl.col("_invalid").is_not_null()
        | pl.col("_standard").is_null()
    )
    violated = _row_count(joined.filter(violated_pred))
    return counted(violated, total)


# field name (lower) -> companion @xxx_source_value column consulted
# by field_concept_record_completeness.sql's non-required-field
# extension clauses, e.g.
#   {@cdmFieldName == 'ROUTE_CONCEPT_ID'}?{OR (cdmTable.@cdmFieldName
#   IS NULL AND cdmTable.route_source_value IS NOT NULL)}
# A concept id field absent from this mapping (and from the two
# table-conditional cases handled separately in
# _record_completeness_source_field) only has the bare ``= 0`` rule
# applied to it.
_RECORD_COMPLETENESS_SOURCE_VALUE_FIELDS = {
    "admitted_from_concept_id": "admitted_from_source_value",
    "admitting_source_concept_id": "admitting_source_value",
    "discharged_to_concept_id": "discharged_to_source_value",
    "discharge_to_concept_id": "discharge_to_source_value",
    "condition_status_concept_id": "condition_status_source_value",
    "modifier_concept_id": "modifier_source_value",
    "route_concept_id": "route_source_value",
    "qualifier_concept_id": "qualifier_source_value",
    "cause_concept_id": "cause_source_value",
    "cause_source_concept_id": "cause_source_value",
    "anatomic_site_concept_id": "anatomic_site_source_value",
    "disease_status_concept_id": "disease_status_source_value",
    "country_concept_id": "country_source_value",
    "place_of_service_concept_id": "place_of_service_source_value",
    "specialty_concept_id": "specialty_source_value",
    "specialty_source_concept_id": "specialty_source_value",
    "payer_concept_id": "payer_source_value",
    "payer_source_concept_id": "payer_source_value",
    "plan_concept_id": "plan_source_value",
    "plan_source_concept_id": "plan_source_value",
    "sponsor_concept_id": "sponsor_source_value",
    "sponsor_source_concept_id": "sponsor_source_value",
    "stop_reason_concept_id": "stop_reason_source_value",
    "stop_reason_source_concept_id": "stop_reason_source_value",
}


def _record_completeness_source_field(chk) -> Optional[str]:
    """The companion source-value column for one field, if any.

    unit_concept_id / unit_source_concept_id get the extension on
    every table except DOSE_ERA; gender_concept_id / gender_source_
    concept_id only get it on PROVIDER. Both are literal
    ``@cdmTableName`` conditions in field_concept_record_
    completeness.sql (
    ``{@cdmTableName != 'DOSE_ERA' & (@cdmFieldName == 'UNIT_CONCEPT_ID'
    | ...)}?{...}`` and
    ``{@cdmTableName == 'PROVIDER' & (@cdmFieldName ==
    'GENDER_CONCEPT_ID' | ...)}?{...}``), so they are handled here
    rather than in the flat mapping above.
    """
    field = chk.cdm_field_name
    table = chk.cdm_table_name
    if field in ("unit_concept_id", "unit_source_concept_id"):
        return None if table == "DOSE_ERA" else "unit_source_value"
    if field in ("gender_concept_id", "gender_source_concept_id"):
        return "gender_source_value" if table == "PROVIDER" else None
    return _RECORD_COMPLETENESS_SOURCE_VALUE_FIELDS.get(field)


def _concept_record_completeness(ctx, chk) -> CheckResult:
    """Non-required-field completeness: zero, or null-with-source-present.

    field_concept_record_completeness.sql never references CONCEPT --
    it is a query over the CDM table alone. Its core predicate is
    ``cdmTable.@cdmFieldName = 0``; for a fixed list of concept id
    fields with a companion source-value column (see
    _record_completeness_source_field above), it additionally flags --
    and additionally counts into the denominator -- a NULL concept id
    whose companion source value is populated. A field with no
    companion (e.g. condition_concept_id) is judged purely on ``= 0``;
    a NULL concept id there is neither counted nor a violation.

    standardConceptRecordCompleteness and sourceConceptRecordCompleteness
    both resolve to this same function: they share this one upstream
    template and differ only in which field a given check instance
    targets, never in behaviour.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    column = pl.col(chk.cdm_field_name)
    source_field = _record_completeness_source_field(chk)
    if source_field is not None and ctx.has_column(
        chk.cdm_table_name, source_field
    ):
        source = pl.col(source_field)
        violated_pred = (column == 0) | (
            column.is_null() & source.is_not_null()
        )
        denominator_pred = column.is_not_null() | source.is_not_null()
    else:
        violated_pred = column == 0
        denominator_pred = column.is_not_null()
    return _count_where(ctx, chk, violated_pred, denominator=denominator_pred)


@register("standardConceptRecordCompleteness")
@register("sourceConceptRecordCompleteness")
def concept_record_completeness(ctx, chk) -> CheckResult:
    return _concept_record_completeness(ctx, chk)
