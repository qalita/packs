import polars as pl

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus


def _run(ctx, check_name, table):
    instance = CheckInstance(
        check_name=check_name,
        check_level="TABLE",
        cdm_table_name=table,
        cdm_field_name=None,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
    )
    return get_check(check_name)(ctx, instance)


def _write(tmp_path, name, frame):
    path = tmp_path / f"{name.lower()}_part_0.parquet"
    frame.write_parquet(path)
    return str(path)


def _dates(frame, *columns):
    return frame.with_columns([pl.col(c).str.to_date() for c in columns])


# --- baseline coverage -----------------------------------------------


def test_cdm_table_passes_for_a_present_table(mini_cdm):
    result = _run(mini_cdm, "cdmTable", "PERSON")
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 1


def test_cdm_table_flags_a_missing_table(mini_cdm):
    result = _run(mini_cdm, "cdmTable", "DRUG_EXPOSURE")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_person_completeness_counts_people_without_records(mini_cdm):
    # people 1, 2, 3 have conditions; person 4 has none
    result = _run(
        mini_cdm, "measurePersonCompleteness", "CONDITION_OCCURRENCE"
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 4


def test_person_completeness_is_not_applicable_without_person(
    mini_cdm,
):
    result = _run(mini_cdm, "measurePersonCompleteness", "NO_SUCH_TABLE")
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_person_completeness_is_not_applicable_without_person_table(
    tmp_path,
):
    occurrences = pl.DataFrame({"person_id": [1, 2]})
    ctx = CdmContext.from_paths(
        {
            "CONDITION_OCCURRENCE": [
                _write(tmp_path, "condition_occurrence", occurrences)
            ]
        }
    )
    result = _run(ctx, "measurePersonCompleteness", "CONDITION_OCCURRENCE")
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_observation_period_overlap_is_not_applicable_when_absent(
    mini_cdm,
):
    result = _run(
        mini_cdm,
        "measureObservationPeriodOverlap",
        "OBSERVATION_PERIOD",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_observation_period_overlap_detects_overlapping_periods(
    tmp_path,
):
    # person 1 has two overlapping periods, person 2 has two disjoint
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2, 3, 4],
                "person_id": [1, 1, 2, 2],
                "observation_period_start_date": [
                    "2010-01-01",
                    "2010-06-01",
                    "2010-01-01",
                    "2012-01-01",
                ],
                "observation_period_end_date": [
                    "2010-12-31",
                    "2011-12-31",
                    "2010-12-31",
                    "2012-12-31",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})

    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 2


def test_condition_era_completeness_is_not_applicable_when_absent(
    mini_cdm,
):
    result = _run(mini_cdm, "measureConditionEraCompleteness", "CONDITION_ERA")
    assert result.status == CheckStatus.NOT_APPLICABLE


# --- upstream-verified additions --------------------------------------
#
# The four tests below each pin a semantic constant that the brief's
# draft got wrong or omitted entirely, confirmed against the upstream
# SQL templates at
# github.com/OHDSI/DataQualityDashboard/blob/main/inst/sql/sql_server/.


def test_condition_era_completeness_na_without_condition_occurrence(
    tmp_path,
):
    era = pl.DataFrame({"person_id": [1]})
    ctx = CdmContext.from_paths(
        {"CONDITION_ERA": [_write(tmp_path, "condition_era", era)]}
    )
    result = _run(ctx, "measureConditionEraCompleteness", "CONDITION_ERA")
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_condition_era_completeness_excludes_sentinel_zero_concept(
    tmp_path,
):
    """table_condition_era_completeness.sql filters
    ``co.condition_concept_id != 0`` in BOTH its denominator and its
    violated-rows subqueries: a person whose only condition rows
    carry the sentinel concept id 0 must be excluded from the
    denominator entirely, not merely spared from being a violation.
    """
    occurrences = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "condition_concept_id": [201826, 0, 201826],
        }
    )
    eras = pl.DataFrame({"person_id": [3]})
    ctx = CdmContext.from_paths(
        {
            "CONDITION_OCCURRENCE": [
                _write(tmp_path, "condition_occurrence", occurrences)
            ],
            "CONDITION_ERA": [_write(tmp_path, "condition_era", eras)],
        }
    )
    result = _run(ctx, "measureConditionEraCompleteness", "CONDITION_ERA")
    # person 2's only row is the sentinel 0 -> excluded from the
    # denominator (2, not 3); person 1 has a real condition and no
    # matching era -> violated; person 3 has a matching era -> clean.
    assert result.num_denominator_rows == 2
    assert result.num_violated_rows == 1


def test_observation_period_overlap_touching_boundary_is_a_violation(
    tmp_path,
):
    """A shared boundary day counts as overlap: the SQL's comparison
    is ``start <= other.end AND end >= other.start`` (<=/>=, not
    </>).
    """
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2],
                "person_id": [1, 1],
                "observation_period_start_date": [
                    "2010-01-01",
                    "2010-06-01",
                ],
                "observation_period_end_date": [
                    "2010-06-01",
                    "2010-12-31",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})
    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_observation_period_overlap_one_day_gap_is_a_violation(
    tmp_path,
):
    """Back-to-back periods with an exact one-day gap are flagged via
    the SQL's ``DATEADD(day, 1, end) = other.start`` clause, even
    though they never overlap under the <=/>= test.
    """
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2],
                "person_id": [1, 1],
                "observation_period_start_date": [
                    "2010-01-01",
                    "2010-06-02",
                ],
                "observation_period_end_date": [
                    "2010-06-01",
                    "2010-12-31",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})
    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_observation_period_overlap_shared_single_boundary_day(
    tmp_path,
):
    """Two one-day periods on the SAME day overlap, inclusively.

    table_observation_period_overlap.sql's first clause is
    ``cdmTable.start <= cdmTable2.end AND cdmTable.end >=
    cdmTable2.start`` -- <=/>=, not </>. Both dates of both periods
    coincide here, so the pair overlaps only by virtue of those two
    equalities: every comparison in the clause is an equality, in
    both directions of the self-join.

    That makes this the one shape that pins the inclusivity of the
    `<=` side. The neighbouring
    test_observation_period_overlap_touching_boundary_is_a_violation
    does not: overlap is a symmetric relation, so with p1 ending the
    day p2 begins, the (p2, p1) direction of the self-join still
    fires through the `>=` clause even if the `<=` were narrowed to
    `<`, and the person is still counted. Here, narrowing `<=` to `<`
    makes BOTH directions false and the count drops to 0.

    The one-day gap clause cannot rescue it either: end + 1 day is
    the day AFTER the other period's start, not equal to it.
    """
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2],
                "person_id": [1, 1],
                "observation_period_start_date": [
                    "2010-06-01",
                    "2010-06-01",
                ],
                "observation_period_end_date": [
                    "2010-06-01",
                    "2010-06-01",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})
    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_observation_period_overlap_two_day_gap_is_not_a_violation(
    tmp_path,
):
    """A two-day gap is neither an overlap nor a one-day back-to-back
    join, so it must NOT be flagged -- the negative control proving
    the two boundary tests above aren't vacuously true.
    """
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2],
                "person_id": [1, 1],
                "observation_period_start_date": [
                    "2010-01-01",
                    "2010-06-03",
                ],
                "observation_period_end_date": [
                    "2010-06-01",
                    "2010-12-31",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})
    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 1


def test_observation_period_overlap_non_adjacent_pair_is_caught(
    tmp_path,
):
    """A person with three periods where a long period covers two
    short, mutually-disjoint ones must still be flagged: upstream
    self-joins every pair of that person's periods (not just
    neighbours in start-date order), so the long/short-2 pair (which
    a naive "compare to the immediately preceding period only"
    algorithm would evaluate against the wrong neighbour) is still
    caught.
    """
    periods = _dates(
        pl.DataFrame(
            {
                "observation_period_id": [1, 2, 3],
                "person_id": [1, 1, 1],
                "observation_period_start_date": [
                    "2000-01-01",
                    "2005-01-01",
                    "2010-01-01",
                ],
                "observation_period_end_date": [
                    "2020-01-01",
                    "2005-01-05",
                    "2010-01-05",
                ],
            }
        ),
        "observation_period_start_date",
        "observation_period_end_date",
    )
    path = _write(tmp_path, "observation_period", periods)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [path]})
    result = _run(ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1
