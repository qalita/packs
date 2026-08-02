"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Helpers shared by the field-, table- and concept-level check modules.
Nothing here reimplements a specific inst/sql/sql_server/*.sql
template; these are the guards and primitives every port of one needs.
See the NOTICE file at the pack root.
"""

from typing import Optional

import polars as pl

from omop_dqd.results import CheckResult, not_applicable


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


def guard(ctx, chk) -> Optional[CheckResult]:
    """Return a NOT_APPLICABLE result when the check cannot run.

    Used by every check before touching data: this pack never FAILs
    on an absent table or column, because upstream has no SQL to run
    at all in that situation.
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


def require_columns(ctx, table: str, *columns: str) -> Optional[CheckResult]:
    """NOT_APPLICABLE naming the first column `table` is missing.

    `guard` only covers a check's *own* table and field. Every check
    that reads a *secondary* table -- PERSON, DEATH, VISIT_OCCURRENCE,
    CONCEPT, CONDITION_OCCURRENCE... -- must also declare which of its
    columns it selects, otherwise a source missing one of them makes
    the check raise, and runner.py turns that into ERROR. The same
    class of defect would then report as NOT_APPLICABLE in one check
    and ERROR in another; this exists so it reports NOT_APPLICABLE
    everywhere.

    The table itself must already be known present -- call
    `ctx.has_table` (or `guard`) first; `ctx.has_column` returns False
    for every column of an absent table, which would produce a
    misleading "column X is absent" message.
    """
    for column in columns:
        if not ctx.has_column(table, column):
            return not_applicable(
                f"column {table}.{column} is absent from the source"
            )
    return None


def as_date(ctx, table: str, column: str) -> pl.Expr:
    """`column` of `table` as a Date expression, parsing strings.

    Every date-comparing check needs its operand as a Date. A plain
    `.cast(pl.Date)` is right for a column parquet already types as
    Date/Datetime, but on a *string* column it is both deprecated
    ("Casting from String to Date is deprecated and will be removed in
    Polars 2.0") and strict -- one unparseable value raises, and
    runner.py turns that into ERROR for the whole check. Since
    properties.yaml advertises `file` and `folder` sources, where a
    date plausibly arrives as text, string columns go through
    `str.to_date(strict=False)` instead: an unparseable value becomes
    NULL, and a NULL date is simply never a violation (SQL and Polars
    both make NULL comparisons falsy), which is the same outcome the
    upstream SQL reaches on a column its own server cannot compare.
    """
    expression = pl.col(column)
    if ctx.dtypes(table).get(column) == pl.Utf8:
        return expression.str.to_date(strict=False)
    return expression.cast(pl.Date)
