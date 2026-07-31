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
See the NOTICE file at the pack root.
"""

from typing import Optional

import polars as pl

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


def guard(ctx, chk) -> Optional[CheckResult]:
    """Return a NOT_APPLICABLE result when the check cannot run.

    Used by every field-level check before touching data.
    """
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if chk.cdm_field_name and not ctx.has_column(
        chk.cdm_table_name, chk.cdm_field_name
    ):
        return not_applicable(
            f"column {chk.qualified_field} is absent from the source"
        )
    return None


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


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
        birth = pl.col("birth_datetime").cast(pl.Date).fill_null(composed)
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
    return _join_violation_count(
        ctx,
        chk,
        person_birth_date(ctx),
        on="person_id",
        predicate=pl.col(chk.cdm_field_name).cast(pl.Date)
        < pl.col("birth_date"),
    )


def _death_dates(ctx) -> pl.LazyFrame:
    return (
        ctx.table("DEATH")
        .select(
            pl.col("person_id"),
            pl.col("death_date").cast(pl.Date),
        )
        .drop_nulls()
    )


@register("plausibleBeforeDeath")
def plausible_before_death(ctx, chk) -> CheckResult:
    """Events dated more than 60 days after the person's death.

    field_plausible_before_death.sql's violated-rows predicate is
    ``CAST(cdmTable.@cdmFieldName AS DATE) >
    DATEADD(day, 60, de.death_date)`` -- a 60-day grace period, not a
    bare ``> death_date``. Its denominator subquery re-joins DEATH and
    filters ``cdmTable.@cdmFieldName IS NOT NULL``, so only rows
    belonging to a person who died, with a populated date, count.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("DEATH"):
        return not_applicable("DEATH is absent from the source")
    column = pl.col(chk.cdm_field_name)
    joined = ctx.table(chk.cdm_table_name).join(
        _death_dates(ctx), on="person_id", how="inner"
    )
    denominator_frame = joined.filter(column.is_not_null())
    total = _row_count(denominator_frame)
    grace_end = pl.col("death_date").dt.offset_by("60d")
    violated = _row_count(
        denominator_frame.filter(column.cast(pl.Date) > grace_end)
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
    death = _death_dates(ctx)
    frame = ctx.table(chk.cdm_table_name)
    dead_person_ids = death.select("person_id").unique()
    denominator_frame = frame.join(dead_person_ids, on="person_id", how="semi")
    total = _row_count(denominator_frame)
    column = pl.col(chk.cdm_field_name)
    grace_end = pl.col("death_date").dt.offset_by("60d")
    violated = _row_count(
        frame.join(death, on="person_id", how="inner").filter(
            column.cast(pl.Date) > grace_end
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
    cdm_field = pl.col(chk.cdm_field_name).cast(pl.Date)

    if other_table == "PERSON":
        if not ctx.has_column(chk.cdm_table_name, "person_id"):
            return not_applicable(f"{chk.cdm_table_name} has no person_id")
        person_columns = ctx.columns("PERSON")
        birth_field = (
            pl.col(other_field).cast(pl.Date)
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
        other_ids = ctx.table(other_table).select("person_id").unique()
        candidate = frame.join(other_ids, on="person_id", how="inner")
    other_col = pl.col(other_field).cast(pl.Date)
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
    visits = ctx.table("VISIT_OCCURRENCE").select(
        pl.col("visit_occurrence_id"),
        pl.col("visit_start_date").cast(pl.Date).alias("_visit_start"),
        pl.col("visit_end_date").cast(pl.Date).alias("_visit_end"),
    )
    joined = ctx.table(chk.cdm_table_name).join(
        visits, on="visit_occurrence_id", how="inner"
    )
    total = _row_count(joined)
    event = pl.col(chk.cdm_field_name).cast(pl.Date)
    early_bound = pl.col("_visit_start").dt.offset_by("-7d")
    late_bound = pl.col("_visit_end").dt.offset_by("7d")
    violated = _row_count(
        joined.filter((event < early_bound) | (event > late_bound))
    )
    return counted(violated, total)
