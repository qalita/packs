"""Aggregation of check results into QALITA metrics and recommendations.

Tasks 5-10 produce one EvaluatedCheck per catalog instance (~2539 of
them against the vendored 5.4 catalog). Nobody consumes that many
metrics: this module collapses them into the handful of numbers the
QALITA platform actually displays (an overall dataset score, one score
per Kahn framework category, one score per CDM table, and a
per-failure detail row) plus a short list of recommendations for the
failures serious enough to act on.

There is no upstream to match here -- DQD renders its own dashboard,
and this aggregation is QALITA's own design, not a port.
"""

from collections import defaultdict
from typing import Dict, List

from omop_dqd.evaluate import EvaluatedCheck
from omop_dqd.results import CheckStatus

# Severity weighting is this pack's own design choice, not something
# DQD computes: a failing "fatal" check should move a score more than
# a failing "characterization" one. Pinned by
# tests/test_reporting.py::test_severity_weights_are_exactly_3_2_1
# and by the weighted-score tests -- changing any of these numbers
# must break a test.
SEVERITY_WEIGHTS: Dict[str, float] = {
    "fatal": 3.0,
    "convention": 2.0,
    "characterization": 1.0,
}

# The vendored Check_Descriptions.csv carries exactly these three
# Kahn framework categories, capitalized ("Conformance",
# "Plausibility", "Completeness" -- verified against the real 5.4
# catalog, see test_every_real_catalog_kahn_category_is_classified).
# Matching is case-insensitive so a stray casing difference doesn't
# silently drop a category out of the per-category scores.
KAHN_METRIC_KEYS = {
    "conformance": "conformance_score",
    "completeness": "completeness_score",
    "plausibility": "plausibility_score",
}

# Only PASS/FAIL checks were actually decided. NOT_APPLICABLE means
# the check could not run at all (missing table/field, empty data...)
# and ERROR means it crashed -- neither says anything about the data,
# so both are excluded from every score below.
_DECIDED = (CheckStatus.PASS, CheckStatus.FAIL)

# Recommendation urgency bucket boundaries, expressed as a percentage
# of violated rows. Also this pack's own design, pinned by
# test_recommendation_level_is_high_at_20_percent_violated,
# test_recommendation_level_is_warning_just_below_20_percent and
# test_recommendation_level_is_info_when_nothing_violated.
_HIGH_LEVEL_THRESHOLD_PCT = 20.0


def _weighted_score(results: List[EvaluatedCheck]) -> float:
    """Share of passing checks among decided ones, weighted by severity.

    NOT_APPLICABLE and ERROR checks are excluded: a check that could
    not run says nothing about quality. A scope with nothing decidable
    (e.g. an entirely missing table, where every check on it comes
    back NOT_APPLICABLE) scores 0.0 rather than the misleading 1.0 an
    empty-numerator-empty-denominator average would otherwise produce.
    """
    total = 0.0
    passed = 0.0
    for evaluated in results:
        if evaluated.result.status not in _DECIDED:
            continue
        weight = SEVERITY_WEIGHTS.get(evaluated.instance.severity, 1.0)
        total += weight
        if evaluated.result.status == CheckStatus.PASS:
            passed += weight
    if total == 0.0:
        return 0.0
    return passed / total


def _metric(key: str, value: float, perimeter: str, scope_value: str) -> dict:
    return {
        "key": key,
        "value": str(round(value, 4)),
        "scope": {"perimeter": perimeter, "value": scope_value},
    }


def build_metrics(
    results: List[EvaluatedCheck], dataset_label: str
) -> List[dict]:
    """Collapse evaluated checks into dataset/category/table/detail metrics.

    Emits, in order:
      - one dataset-level "score" (severity-weighted pass rate over
        every decided check);
      - one "<category>_score" per Kahn category actually present in
        `results` (conformance/completeness/plausibility);
      - one table-scoped "score" per CDM table present in `results`;
      - one "pct_violated_rows" per *failing* check, scoped to the
        column (or table, for table-level checks) it failed on.

    The last bucket is deliberately the only one that scales with the
    number of results: everything else is bounded by the number of
    Kahn categories (<=3) and CDM tables (<=39), so a run of ~2539
    checks still turns into a small, dashboard-sized metric list.
    """
    metrics = [
        _metric("score", _weighted_score(results), "dataset", dataset_label)
    ]

    by_category = defaultdict(list)
    for evaluated in results:
        category = evaluated.instance.kahn_category.strip().lower()
        if category in KAHN_METRIC_KEYS:
            by_category[category].append(evaluated)
    for category, key in KAHN_METRIC_KEYS.items():
        category_results = by_category.get(category)
        if not category_results:
            continue
        metrics.append(
            _metric(
                key,
                _weighted_score(category_results),
                "dataset",
                dataset_label,
            )
        )

    by_table = defaultdict(list)
    for evaluated in results:
        by_table[evaluated.instance.cdm_table_name].append(evaluated)
    for table_name, table_results in sorted(by_table.items()):
        metrics.append(
            _metric(
                "score",
                _weighted_score(table_results),
                "table",
                table_name,
            )
        )

    for evaluated in results:
        if evaluated.result.status != CheckStatus.FAIL:
            continue
        metrics.append(
            _metric(
                "pct_violated_rows",
                evaluated.result.pct_violated_rows,
                "column",
                evaluated.instance.qualified_field,
            )
        )

    return metrics


def _level(pct_violated: float) -> str:
    if pct_violated >= _HIGH_LEVEL_THRESHOLD_PCT:
        return "high"
    if pct_violated > 0.0:
        return "warning"
    return "info"


def build_recommendations(
    results: List[EvaluatedCheck], dataset_label: str
) -> List[dict]:
    """One recommendation per failing check of "fatal" severity.

    Restricted to "fatal" so the list stays short and actionable --
    "convention" and "characterization" failures are visible through
    pct_violated_rows metrics instead, without generating a
    recommendation of their own.

    The content reuses the upstream checkDescription text as
    remediation guidance, plus the concrete violation counts so each
    recommendation is self-contained.
    """
    recommendations = []
    for evaluated in results:
        instance = evaluated.instance
        if evaluated.result.status != CheckStatus.FAIL:
            continue
        if instance.severity != "fatal":
            continue
        detail = instance.description or instance.check_name
        recommendations.append(
            {
                "content": (
                    f"[{instance.check_name}] {instance.qualified_field}: "
                    f"{evaluated.result.num_violated_rows} of "
                    f"{evaluated.result.num_denominator_rows} rows violate "
                    f"this check. {detail}"
                ),
                "type": "OMOP CDM",
                "scope": {
                    "perimeter": "column",
                    "value": instance.qualified_field,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_label,
                    },
                },
                "level": _level(evaluated.result.pct_violated_rows),
            }
        )
    return recommendations
