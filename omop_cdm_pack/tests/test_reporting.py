import omop_dqd.checks  # noqa: F401  (registers implementations)
from omop_dqd.catalog import CheckInstance, load_catalog
from omop_dqd.evaluate import EvaluatedCheck
from omop_dqd.reporting import (
    SEVERITY_WEIGHTS,
    build_metrics,
    build_recommendations,
)
from omop_dqd.results import CheckResult, CheckStatus
from omop_dqd.runner import run_checks


def _evaluated(
    check_name,
    status,
    severity="fatal",
    kahn="Conformance",
    table="PERSON",
    field="person_id",
    violated=0,
    denominator=10,
):
    instance = CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity=severity,
        kahn_category=kahn,
        description=f"{check_name} description",
    )
    return EvaluatedCheck(
        instance,
        CheckResult(
            num_violated_rows=violated,
            num_denominator_rows=denominator,
            status=status,
        ),
    )


def _by_key(metrics, key, perimeter=None):
    return [
        m
        for m in metrics
        if m["key"] == key
        and (perimeter is None or m["scope"]["perimeter"] == perimeter)
    ]


# --- score computation -------------------------------------------------


def test_score_is_one_when_everything_passes():
    metrics = build_metrics([_evaluated("isRequired", CheckStatus.PASS)], "ds")
    score = _by_key(metrics, "score", "dataset")[0]
    assert float(score["value"]) == 1.0


def test_score_is_zero_when_everything_fails():
    metrics = build_metrics(
        [_evaluated("isRequired", CheckStatus.FAIL, violated=5)], "ds"
    )
    score = _by_key(metrics, "score", "dataset")[0]
    assert float(score["value"]) == 0.0


def test_score_is_severity_weighted():
    # one fatal failure (weight 3) and one characterization pass
    # (weight 1) -> passed 1 / total 4 = 0.25. Pins fatal=3 and
    # characterization=1 together: change either weight and this
    # ratio moves off 0.25.
    results = [
        _evaluated("isRequired", CheckStatus.FAIL, severity="fatal"),
        _evaluated(
            "measureValueCompleteness",
            CheckStatus.PASS,
            severity="characterization",
        ),
    ]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 0.25


def test_score_pins_convention_weight():
    # one fatal pass (weight 3) and one convention failure (weight 2)
    # -> passed 3 / total 5 = 0.6. This value only holds if
    # convention's weight is exactly 2; weight 1 would give 0.75 and
    # weight 3 would give 0.5.
    results = [
        _evaluated("isRequired", CheckStatus.PASS, severity="fatal"),
        _evaluated("cdmDatatype", CheckStatus.FAIL, severity="convention"),
    ]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 0.6


def test_severity_weights_are_exactly_3_2_1():
    assert SEVERITY_WEIGHTS == {
        "fatal": 3.0,
        "convention": 2.0,
        "characterization": 1.0,
    }


def test_not_applicable_checks_are_excluded_from_the_score():
    results = [
        _evaluated("isRequired", CheckStatus.PASS),
        _evaluated("fkDomain", CheckStatus.NOT_APPLICABLE),
        _evaluated("fkClass", CheckStatus.ERROR),
    ]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 1.0


def test_score_is_zero_when_nothing_is_applicable():
    # A scope where nothing was decidable (everything NOT_APPLICABLE
    # or ERROR) must not silently read as perfect quality.
    results = [_evaluated("fkDomain", CheckStatus.NOT_APPLICABLE)]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 0.0


def test_score_is_zero_when_everything_errors():
    results = [_evaluated("fkDomain", CheckStatus.ERROR)]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 0.0


# --- kahn category scores -----------------------------------------------


def test_kahn_category_scores_are_emitted():
    results = [
        _evaluated("isRequired", CheckStatus.PASS, kahn="Conformance"),
        _evaluated(
            "measureValueCompleteness",
            CheckStatus.FAIL,
            kahn="Completeness",
        ),
        _evaluated(
            "plausibleValueLow",
            CheckStatus.PASS,
            kahn="Plausibility",
        ),
    ]
    metrics = build_metrics(results, "ds")
    keys = {m["key"] for m in metrics}
    assert "conformance_score" in keys
    assert "completeness_score" in keys
    assert "plausibility_score" in keys
    completeness = _by_key(metrics, "completeness_score")[0]
    assert float(completeness["value"]) == 0.0


def test_kahn_category_matching_is_case_insensitive():
    # The vendored catalog uses exactly "Conformance", "Plausibility"
    # and "Completeness" (capitalized) -- KAHN_METRIC_KEYS itself is
    # keyed in lowercase, so matching must normalize case rather than
    # depend on the catalog's exact capitalization. Deliberately use
    # a casing that differs from *both* the catalog's ("Conformance")
    # and the internal dict's ("conformance"), so this only passes if
    # the implementation actually normalizes rather than happening to
    # match one side already.
    results = [
        _evaluated("isRequired", CheckStatus.PASS, kahn="CONFORMANCE"),
    ]
    metrics = build_metrics(results, "ds")
    keys = {m["key"] for m in metrics}
    assert "conformance_score" in keys


def test_every_real_catalog_kahn_category_is_classified():
    catalog = load_catalog("5.4")
    seen = {instance.kahn_category.strip().lower() for instance in catalog}
    assert seen == {"conformance", "plausibility", "completeness"}


# --- per-table scores -----------------------------------------------------


def test_per_table_scores_are_emitted():
    results = [
        _evaluated("isRequired", CheckStatus.PASS, table="PERSON"),
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            table="CONDITION_OCCURRENCE",
        ),
    ]
    table_scores = _by_key(build_metrics(results, "ds"), "score", "table")
    values = {m["scope"]["value"]: float(m["value"]) for m in table_scores}
    assert values["PERSON"] == 1.0
    assert values["CONDITION_OCCURRENCE"] == 0.0


# --- pct_violated_rows ------------------------------------------------


def test_pct_violated_is_emitted_only_for_failures():
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            violated=3,
            denominator=10,
            field="a",
        ),
        _evaluated("isRequired", CheckStatus.PASS, field="b", violated=0),
    ]
    metrics = _by_key(build_metrics(results, "ds"), "pct_violated_rows")
    assert len(metrics) == 1
    assert float(metrics[0]["value"]) == 30.0
    assert metrics[0]["scope"]["value"] == "PERSON.a"


def test_metric_values_are_strings():
    metrics = build_metrics([_evaluated("isRequired", CheckStatus.PASS)], "ds")
    assert all(isinstance(m["value"], str) for m in metrics)


# --- recommendations -----------------------------------------------------


def test_recommendations_are_emitted_for_fatal_failures_only():
    results = [
        _evaluated("isRequired", CheckStatus.FAIL, severity="fatal"),
        _evaluated(
            "plausibleValueLow",
            CheckStatus.FAIL,
            severity="characterization",
        ),
        _evaluated("cdmField", CheckStatus.PASS, severity="fatal"),
    ]
    recommendations = build_recommendations(results, "ds")
    assert len(recommendations) == 1
    assert "isRequired" in recommendations[0]["content"]


def test_recommendations_carry_the_expected_shape():
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            severity="fatal",
            violated=2,
            denominator=10,
        )
    ]
    recommendation = build_recommendations(results, "ds")[0]
    assert recommendation["type"] == "OMOP CDM"
    assert recommendation["level"] in {"high", "warning", "info"}
    assert recommendation["scope"]["perimeter"] == "column"
    assert recommendation["scope"]["value"] == "PERSON.person_id"
    assert recommendation["scope"]["parent_scope"]["value"] == "ds"
    assert recommendation["scope"]["parent_scope"]["perimeter"] == "dataset"


def test_recommendation_level_is_high_at_20_percent_violated():
    # Boundary is >= 20%. Below this test's violated/denominator it
    # would read "warning" instead.
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            severity="fatal",
            violated=4,
            denominator=20,
        )
    ]
    recommendation = build_recommendations(results, "ds")[0]
    assert recommendation["level"] == "high"


def test_recommendation_level_is_warning_just_below_20_percent():
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            severity="fatal",
            violated=199,
            denominator=1000,
        )
    ]
    recommendation = build_recommendations(results, "ds")[0]
    assert recommendation["level"] == "warning"


def test_recommendation_level_is_info_when_nothing_violated():
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            severity="fatal",
            violated=0,
            denominator=10,
        )
    ]
    recommendation = build_recommendations(results, "ds")[0]
    assert recommendation["level"] == "info"


# --- full-catalog sanity check --------------------------------------------


def test_full_catalog_run_produces_a_sane_report(mini_cdm):
    catalog = load_catalog("5.4")
    results = run_checks(mini_cdm, catalog)

    fail_count = sum(1 for r in results if r.result.status == CheckStatus.FAIL)
    fatal_fail_count = sum(
        1
        for r in results
        if r.result.status == CheckStatus.FAIL
        and r.instance.severity == "fatal"
    )

    metrics = build_metrics(results, "mini_cdm")
    recommendations = build_recommendations(results, "mini_cdm")

    pct_metrics = _by_key(metrics, "pct_violated_rows")
    assert len(pct_metrics) == fail_count

    assert len(recommendations) == fatal_fail_count

    score = _by_key(metrics, "score", "dataset")[0]
    assert 0.0 <= float(score["value"]) <= 1.0

    # Metric volume must stay far below one metric per check result:
    # ~2539 results collapse into a small, table/category/dataset
    # sized set plus one row per failure.
    assert len(metrics) < 500
