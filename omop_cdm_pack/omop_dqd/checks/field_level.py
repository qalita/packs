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

# OMOP datatype names mapped onto the Polars dtype groups that satisfy
# them. A source is free to widen a type (int32 where int64 is
# specified) so membership, not equality, is what matters.
#
# pl.INTEGER_DTYPES / pl.FLOAT_DTYPES are deprecated on the installed
# Polars version (1.43.1) and emit a DeprecationWarning on access
# rather than raising AttributeError, so the explicit replacement
# sets from the task brief's Step 4 are used unconditionally here to
# keep test output warning-free.
#
# "varchar" is included (absent from the brief's sample) because the
# vendored Field_Level CSVs declare cdmDatatype as "varchar(N)" for
# the majority of CDM columns -- without it, cdmDatatype would be
# NOT_APPLICABLE for nearly every real field, and the brief's own
# test (a varchar(50) declaration checked against an integer column)
# would never observe a mismatch.
_DATATYPE_GROUPS = {
    "integer": frozenset(
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
    ),
    "bigint": frozenset(
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
    ),
    "float": frozenset({pl.Float32, pl.Float64}),
    "date": frozenset({pl.Date}),
    "datetime": frozenset({pl.Datetime}),
    "varchar": frozenset({pl.Utf8}),
}


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
    skip = guard(ctx, chk)
    if skip:
        return skip
    declared = chk.params.get("value", "").strip().lower()
    group = None
    for prefix, dtypes in _DATATYPE_GROUPS.items():
        if declared.startswith(prefix):
            group = dtypes
            break
    if group is None:
        # Datatype labels outside the known groups (integer, bigint,
        # float, date, datetime, varchar) are not checked.
        return not_applicable(f"datatype {declared!r} is not checked")
    actual = ctx.dtypes(chk.cdm_table_name)[chk.cdm_field_name]
    return counted(0 if actual in group else 1, 1)


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
    skip = guard(ctx, chk)
    if skip:
        return skip
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column.is_null() | (column.cast(pl.Utf8) == "")
    )


@register("isPrimaryKey")
def is_primary_key(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    frame = ctx.table(chk.cdm_table_name)
    counts = (
        frame.select(
            pl.len().alias("total"),
            pl.col(chk.cdm_field_name).n_unique().alias("distinct"),
        )
        .collect(engine="streaming")
        .row(0)
    )
    total, distinct = counts
    return counted(total - distinct, total)


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
