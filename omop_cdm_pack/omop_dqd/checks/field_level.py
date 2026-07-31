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
