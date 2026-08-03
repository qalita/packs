"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/table_cdm_table.sql
  inst/sql/sql_server/table_person_completeness.sql
  inst/sql/sql_server/table_condition_era_completeness.sql
  inst/sql/sql_server/table_observation_period_overlap.sql
See the NOTICE file at the pack root.
"""

import polars as pl

from omop_dqd.checks._common import _row_count, as_date, require_columns
from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable


@register("cdmTable")
def cdm_table(ctx, chk) -> CheckResult:
    """A table either exists or it doesn't.

    table_cdm_table.sql's own violated-rows subquery is a decoy: its
    ``CASE WHEN COUNT_BIG(*) = 0 THEN 0 ELSE 0 END`` is *always* zero
    whenever the SQL runs at all. Upstream actually detects a missing
    table by the query failing to execute against the database:
    R/evaluateThresholds.R pattern-matches "does not exist"-style
    driver errors specifically for cdmTable (and cdmField) and forces
    ``failed <- 1``, while R/calculateNotApplicableStatus.R hardcodes
    that cdmTable "should never be marked as NA, no matter what". We
    have no SQL execution to fail, so ctx.has_table stands in
    directly for that error path: present -> 0 violated, absent -> 1
    violated, over a denominator that is always 1 either way (the
    literal ``SELECT 1 AS num_rows`` denominator subquery).
    """
    present = ctx.has_table(chk.cdm_table_name)
    return counted(0 if present else 1, 1)


@register("measurePersonCompleteness")
def measure_person_completeness(ctx, chk) -> CheckResult:
    """People with no record at all in the checked table.

    table_person_completeness.sql LEFT JOINs PERSON to the checked
    table on person_id and counts the PERSON rows left unmatched; the
    denominator is PERSON's full row count, unfiltered. A missing
    *checked* table matches upstream's dedicated special case in
    calculateNotApplicableStatus.R ("Special rule for
    measurePersonCompleteness: tableIsMissing -> 1"). Upstream has no
    real equivalent for PERSON itself being absent (the query would
    just error), so -- per this pack's convention of never FAILing on
    an absent table -- that degrades to NOT_APPLICABLE too.
    """
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, "person_id"):
        return not_applicable(f"{chk.cdm_table_name} has no person_id column")
    missing = require_columns(ctx, "PERSON", "person_id")
    if missing:
        return missing
    people = ctx.table("PERSON").select("person_id")
    total = _row_count(people)
    referenced = (
        ctx.table(chk.cdm_table_name).select("person_id").drop_nulls().unique()
    )
    missing = people.join(referenced, on="person_id", how="anti")
    return counted(_row_count(missing), total)


@register("measureConditionEraCompleteness")
def measure_condition_era_completeness(ctx, chk) -> CheckResult:
    """People with real conditions but no matching condition era.

    table_condition_era_completeness.sql restricts BOTH its
    violated-rows and its denominator subqueries to
    ``co.condition_concept_id != 0`` -- the sentinel-0 "unmapped"
    concept id is excluded from the denominator entirely, not merely
    exempted from being counted as a violation. SQL's three-valued
    logic means a NULL condition_concept_id is *also* excluded
    (``NULL != 0`` is unknown, not true), which matches Polars' own
    null-propagating ``!=`` -- no extra drop_nulls needed for that
    column.
    """
    if not ctx.has_table("CONDITION_ERA"):
        return not_applicable("CONDITION_ERA is absent from the source")
    if not ctx.has_table("CONDITION_OCCURRENCE"):
        return not_applicable("CONDITION_OCCURRENCE is absent from the source")
    missing = require_columns(
        ctx, "CONDITION_OCCURRENCE", "condition_concept_id", "person_id"
    ) or require_columns(ctx, "CONDITION_ERA", "person_id")
    if missing:
        return missing
    occurrences = (
        ctx.table("CONDITION_OCCURRENCE")
        .filter(pl.col("condition_concept_id") != 0)
        .select("person_id")
        .drop_nulls()
        .unique()
    )
    total = _row_count(occurrences)
    eras = ctx.table("CONDITION_ERA").select("person_id").drop_nulls().unique()
    missing = occurrences.join(eras, on="person_id", how="anti")
    return counted(_row_count(missing), total)


@register("measureObservationPeriodOverlap")
def measure_observation_period_overlap(ctx, chk) -> CheckResult:
    """People with any two observation periods that overlap or touch.

    table_observation_period_overlap.sql self-joins OBSERVATION_PERIOD
    to itself on person_id for every pair of *distinct*
    observation_period_id values (both directions -- it is not
    restricted to id1 < id2), then flags the pair when EITHER:

      * they overlap, inclusive of sharing exactly one boundary day
        (``start <= other.end AND end >= other.start`` -- <=/>=, not
        </>), or
      * they are exactly one calendar day apart with no gap at all
        (``DATEADD(day, 1, end) = other.start``, checked in both
        directions) -- true back-to-back periods, which do not
        satisfy the first test but are still flagged.

    This is evaluated over every pair, not just neighbours in
    start-date order, so a person with three periods where one long
    period covers two short, mutually-disjoint ones is still caught.
    The denominator is every distinct person_id in OBSERVATION_PERIOD,
    unfiltered -- it does not require an overlap to exist.
    """
    table = "OBSERVATION_PERIOD"
    if not ctx.has_table(table):
        return not_applicable(f"{table} is absent from the source")
    missing = require_columns(
        ctx,
        table,
        "observation_period_id",
        "person_id",
        "observation_period_start_date",
        "observation_period_end_date",
    )
    if missing:
        return missing
    periods = ctx.table(table).select(
        pl.col("observation_period_id"),
        pl.col("person_id"),
        as_date(ctx, table, "observation_period_start_date").alias(
            "start_date"
        ),
        as_date(ctx, table, "observation_period_end_date").alias("end_date"),
    )
    total = _row_count(periods.select("person_id").unique())

    other = periods.select(
        pl.col("observation_period_id").alias("_other_id"),
        pl.col("person_id"),
        pl.col("start_date").alias("_other_start"),
        pl.col("end_date").alias("_other_end"),
    )
    paired = periods.join(other, on="person_id", how="inner").filter(
        pl.col("observation_period_id") != pl.col("_other_id")
    )
    overlaps = (pl.col("start_date") <= pl.col("_other_end")) & (
        pl.col("end_date") >= pl.col("_other_start")
    )
    back_to_back = (
        pl.col("end_date").dt.offset_by("1d") == pl.col("_other_start")
    ) | (pl.col("_other_end").dt.offset_by("1d") == pl.col("start_date"))
    violated = (
        paired.filter(overlaps | back_to_back).select("person_id").unique()
    )
    return counted(_row_count(violated), total)
