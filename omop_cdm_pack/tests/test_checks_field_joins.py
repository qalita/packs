"""Field-level checks that join a second CDM table.

Expected values are derived from the upstream SQL templates in
inst/sql/sql_server/, not from the check descriptions' prose, and
several diverge from a literal reading of the plan this task started
from -- see the comments on each test for the deciding SQL line.
"""

import polars as pl

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="fatal",
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


def test_foreign_key_detects_the_orphan_concept(mini_cdm):
    # condition_concept_id 99999 is absent from CONCEPT.
    # is_foreign_key.sql's denominator subquery is
    # `SELECT COUNT_BIG(*) FROM @schema.@cdmTableName cdmTable` --
    # no WHERE clause at all, so it is the whole table (6 rows), not
    # just the rows with a non-null fk value (5).
    result = _run(
        mini_cdm,
        "isForeignKey",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        fkTableName="CONCEPT",
        fkFieldName="CONCEPT_ID",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_foreign_key_is_not_applicable_without_the_parent_table(
    mini_cdm_no_vocabulary,
):
    result = _run(
        mini_cdm_no_vocabulary,
        "isForeignKey",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        fkTableName="CONCEPT",
        fkFieldName="CONCEPT_ID",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_plausible_after_birth_detects_the_pre_birth_event(mini_cdm):
    # condition_occurrence_id 102 starts 1970-01-01, person 1 born
    # 1980. field_plausible_after_birth.sql's denominator is
    # `WHERE cdmTable.@cdmFieldName IS NOT NULL` (no join), which is
    # 6 here since every condition row has a start date.
    result = _run(
        mini_cdm,
        "plausibleAfterBirth",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_plausible_before_death_detects_the_post_mortem_event(
    mini_cdm,
):
    # condition_occurrence_id 103 is 2021-01-01, person 3 died
    # 2020-01-01 -- more than 60 days later either way.
    # field_plausible_before_death.sql's denominator subquery
    # re-joins DEATH and filters on a non-null date:
    #   JOIN @cdmDatabaseSchema.death ON death.person_id = cdmTable.person_id
    #   WHERE cdmTable.@cdmFieldName IS NOT NULL
    # Only person 3 died, and only row 103 belongs to them, so the
    # denominator is 1, not the whole table.
    result = _run(
        mini_cdm,
        "plausibleBeforeDeath",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_plausible_during_life_matches_before_death_in_this_fixture(
    mini_cdm,
):
    # In this fixture person 3 (the only death) has exactly one
    # condition row and it is non-null, so plausibleDuringLife's
    # "every row belonging to a dead person" denominator happens to
    # coincide with plausibleBeforeDeath's "non-null rows of a dead
    # person" denominator. See the next test for a fixture where they
    # diverge: delegating one to the other is not generally correct.
    result = _run(
        mini_cdm,
        "plausibleDuringLife",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_plausible_during_life_denominator_differs_from_before_death(
    tmp_path,
):
    # field_plausible_during_life.sql's denominator is
    #   WHERE person_id IN (SELECT person_id FROM death)
    # over the *unfiltered* CDM table -- it does not require the
    # checked field to be non-null. field_plausible_before_death.sql
    # instead re-joins DEATH and filters
    # `cdmTable.@cdmFieldName IS NOT NULL`. A dead person with one
    # populated and one NULL event date must therefore produce
    # different denominators (2 vs 1) under the two checks: this is
    # the proof that plausibleDuringLife is not simply a delegation
    # to plausibleBeforeDeath, despite the two sharing the same
    # ">60 days after death" violation rule.
    ctx = _write_tables(
        tmp_path,
        PERSON=pl.DataFrame({"person_id": [5], "year_of_birth": [1950]}),
        DEATH=pl.DataFrame(
            {"person_id": [5], "death_date": ["2010-01-01"]}
        ).with_columns(pl.col("death_date").str.to_date()),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {
                "person_id": [5, 5],
                "condition_start_date": ["2010-05-01", None],
            }
        ).with_columns(pl.col("condition_start_date").str.to_date()),
    )

    during_life = _run(
        ctx,
        "plausibleDuringLife",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    before_death = _run(
        ctx,
        "plausibleBeforeDeath",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert during_life.num_denominator_rows == 2
    assert during_life.num_violated_rows == 1
    assert before_death.num_denominator_rows == 1
    assert before_death.num_violated_rows == 1


def test_within_visit_dates_counts_the_out_of_window_events(mini_cdm):
    # Rows 101 and 102 (visit 10, window 2015-01-01..2015-01-05) and
    # row 103 (visit 12, window 2017-01-01..2017-01-05) all fall
    # outside their visit's window by wide margins, well past the
    # +/-7 day grace period field_within_visit_dates.sql allows
    # (`cdmTable.@cdmFieldName < DATEADD(DAY, -7, vo.visit_start_date)
    # OR ... > DATEADD(DAY, 7, vo.visit_end_date)`). Every row joins
    # to a visit, so the denominator is the whole table, 6.
    result = _run(
        mini_cdm,
        "withinVisitDates",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 3
    assert result.num_denominator_rows == 6


def test_within_visit_dates_honours_the_seven_day_grace_period(
    tmp_path,
):
    # An event 3 days before visit_start_date is inside upstream's
    # +/-7 day grace window and must NOT be flagged -- a naive
    # "date must be inside [start, end]" implementation would wrongly
    # flag it.
    ctx = _write_tables(
        tmp_path,
        VISIT_OCCURRENCE=pl.DataFrame(
            {
                "visit_occurrence_id": [1],
                "person_id": [1],
                "visit_start_date": ["2020-01-10"],
                "visit_end_date": ["2020-01-20"],
            }
        ).with_columns(
            pl.col("visit_start_date").str.to_date(),
            pl.col("visit_end_date").str.to_date(),
        ),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {
                "person_id": [1],
                "condition_start_date": ["2020-01-07"],
                "visit_occurrence_id": [1],
            }
        ).with_columns(pl.col("condition_start_date").str.to_date()),
    )

    result = _run(
        ctx,
        "withinVisitDates",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 1


def test_plausible_temporal_after_same_table(mini_cdm):
    # condition_end_date must occur on/after condition_start_date;
    # row 101 has a start date (2015-06-01) after its end date
    # (2015-05-01). field_plausible_temporal_after.sql's denominator
    # subquery has no WHERE clause at all, so it is the whole table,
    # 6, not filtered to non-null dates.
    result = _run(
        mini_cdm,
        "plausibleTemporalAfter",
        "CONDITION_OCCURRENCE",
        "condition_end_date",
        plausibleTemporalAfterTableName="CONDITION_OCCURRENCE",
        plausibleTemporalAfterFieldName="condition_start_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_plausible_temporal_after_person_birth(mini_cdm):
    # condition_start_date must occur on/after PERSON.birth_datetime;
    # row 102 (person 1, born 1980-01-15) starts 1970-01-01. This is
    # the dominant real usage of plausibleTemporalAfter in the
    # vendored catalog (cdmField vs PERSON.birth_datetime).
    result = _run(
        mini_cdm,
        "plausibleTemporalAfter",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleTemporalAfterTableName="PERSON",
        plausibleTemporalAfterFieldName="birth_datetime",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_plausible_temporal_after_person_defaults_to_june_first(
    tmp_path,
):
    # field_plausible_temporal_after.sql defaults a missing
    # birth_datetime to *1 June* of year_of_birth:
    #   CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
    # -- unlike plausibleAfterBirth / person_birth_date(), which
    # default to 1 January. An event on 2000-03-01, with no
    # birth_datetime and year_of_birth 2000, is after the (wrong)
    # 1 Jan default but before the (correct) 1 Jun default, so this
    # only flags a violation if the June default is implemented.
    ctx = _write_tables(
        tmp_path,
        PERSON=pl.DataFrame({"person_id": [7], "year_of_birth": [2000]}),
        CONDITION_OCCURRENCE=pl.DataFrame(
            {"person_id": [7], "condition_start_date": ["2000-03-01"]}
        ).with_columns(pl.col("condition_start_date").str.to_date()),
    )

    result = _run(
        ctx,
        "plausibleTemporalAfter",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleTemporalAfterTableName="PERSON",
        plausibleTemporalAfterFieldName="birth_datetime",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_plausible_temporal_after_unreachable_cross_table_combo(
    mini_cdm,
):
    # The vendored catalog never pairs plausibleTemporalAfter with a
    # table that is neither PERSON nor the CDM table itself (checked
    # against OMOP_CDMv5.4_Field_Level.csv). Upstream's own WHERE
    # clause would silently misresolve this combination anyway -- its
    # non-PERSON branch reads `CAST(cdmTable.@plausibleTemporalAfter
    # FieldName AS DATE)`, i.e. a column on the CDM table itself, not
    # on the joined table:
    #   }:{
    #       CAST(cdmTable.@plausibleTemporalAfterFieldName AS DATE)
    #   } > CAST(cdmTable.@cdmFieldName AS DATE)
    # CONDITION_OCCURRENCE has no visit_start_date column, so we
    # report NOT_APPLICABLE rather than inventing a join the SQL
    # itself doesn't perform.
    result = _run(
        mini_cdm,
        "plausibleTemporalAfter",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleTemporalAfterTableName="VISIT_OCCURRENCE",
        plausibleTemporalAfterFieldName="visit_start_date",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE
