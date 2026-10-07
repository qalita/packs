"""Findings -> QALITA metrics, recommendations, schemas and figures.

Scopes follow omop_cdm_pack, the other multi-table model pack: the source is
the dataset, each codoc table a ``table`` under it, each column a ``column``
named ``<table>.<column>`` under the dataset.

The headline ``score`` is the mean of the per-table scores, so every table in
scope weighs the same: a missing table counts as a 0, instead of being one
failed check drowned among the hundreds a present table produces.
"""

from collections import Counter, defaultdict
from typing import Dict, List

from codoc_model.checks import (
    COLUMN_PRESENT,
    COLUMN_TYPE,
    MAX_LENGTH,
    NOT_NULL,
    PRIMARY_KEY,
    TABLE_PRESENT,
    UNEXPECTED_COLUMN,
    Finding,
    TableResult,
)

RECOMMENDATION_TYPE = "codoc data model"

# metric key emitted per failing column check
COLUMN_METRIC = {
    COLUMN_PRESENT: "column_missing",
    COLUMN_TYPE: "type_violations",
    NOT_NULL: "null_violations",
    PRIMARY_KEY: "primary_key_violations",
    MAX_LENGTH: "length_violations",
    UNEXPECTED_COLUMN: "column_unexpected",
}


def overall_score(results: List[TableResult]) -> float:
    if not results:
        return 0.0
    return sum(r.score for r in results) / len(results)


def _ratio(value: float) -> str:
    return str(round(value, 2))


def _dataset(name: str) -> dict:
    return {"perimeter": "dataset", "value": name}


def _table(name: str, dataset: str) -> dict:
    return {
        "perimeter": "table",
        "value": name,
        "parent_scope": _dataset(dataset),
    }


def _column(table: str, column: str, dataset: str) -> dict:
    return {
        "perimeter": "column",
        "value": f"{table}.{column}",
        "parent_scope": _dataset(dataset),
    }


def _failed(results: List[TableResult], check: str) -> List[Finding]:
    return [
        f
        for r in results
        for f in r.findings
        if f.check == check and not f.passed
    ]


def build_metrics(results: List[TableResult], dataset: str) -> List[dict]:
    scope = _dataset(dataset)
    present = [r for r in results if r.present]
    metrics = [
        {
            "key": "score",
            "value": _ratio(overall_score(results)),
            "scope": scope,
        },
        {"key": "tables_expected", "value": len(results), "scope": scope},
        {"key": "tables_present", "value": len(present), "scope": scope},
        {
            "key": "tables_missing",
            "value": len(results) - len(present),
            "scope": scope,
        },
        {
            "key": "columns_checked",
            "value": sum(
                1
                for r in present
                for f in r.findings
                if f.check == COLUMN_PRESENT
            ),
            "scope": scope,
        },
    ]
    for check, key in COLUMN_METRIC.items():
        metrics.append(
            {
                "key": f"{key}_count",
                "value": len(_failed(results, check)),
                "scope": scope,
            }
        )

    for result in results:
        table_scope = _table(result.table, dataset)
        metrics.append(
            {
                "key": "score",
                "value": _ratio(result.score),
                "scope": table_scope,
            }
        )
        if result.present:
            failed = [f for f in result.findings if f.scored and not f.passed]
            metrics.extend(
                [
                    {
                        "key": "row_count",
                        "value": result.rows,
                        "scope": table_scope,
                    },
                    {
                        "key": "checks_failed",
                        "value": len(failed),
                        "scope": table_scope,
                    },
                ]
            )
        for finding in result.findings:
            if finding.column is None:
                continue
            column_scope = _column(result.table, finding.column, dataset)
            if finding.check == COLUMN_TYPE:
                metrics.append(
                    {
                        "key": "declared_type",
                        "value": finding.expected,
                        "scope": column_scope,
                    }
                )
                metrics.append(
                    {
                        "key": "stored_type",
                        "value": finding.actual,
                        "scope": column_scope,
                    }
                )
            if not finding.passed:
                metrics.append(
                    {
                        "key": COLUMN_METRIC[finding.check],
                        "value": finding.violations or 1,
                        "scope": column_scope,
                    }
                )
    return metrics


def _level(finding: Finding) -> str:
    if finding.check in (TABLE_PRESENT, NOT_NULL, PRIMARY_KEY):
        return "high"
    if finding.check == UNEXPECTED_COLUMN:
        return "info"
    if finding.check in (COLUMN_PRESENT, COLUMN_TYPE):
        return "high" if finding.required else "warning"
    return "warning"


def _count(finding: Finding) -> str:
    return f"{finding.violations} of {finding.rows} rows"


def build_recommendations(
    results: List[TableResult], dataset: str, unmatched_objects=()
) -> List[dict]:
    """One recommendation per failing check.

    Missing and unexpected columns are grouped per table: a table built on an
    older model version can miss a dozen columns, which is one fix, not twelve.
    """
    recommendations = []

    def add(content: str, scope: dict, level: str) -> None:
        recommendations.append(
            {
                "content": content,
                "type": RECOMMENDATION_TYPE,
                "scope": scope,
                "level": level,
            }
        )

    for result in results:
        table_scope = _table(result.table, dataset)
        grouped: Dict[str, List[Finding]] = defaultdict(list)
        for finding in result.findings:
            if finding.passed:
                continue
            if finding.check == TABLE_PRESENT:
                add(
                    f"Table {result.table} is missing from the source.",
                    table_scope,
                    "high",
                )
            elif finding.check in (COLUMN_PRESENT, UNEXPECTED_COLUMN):
                grouped[finding.check].append(finding)
            else:
                add(
                    _finding_text(finding),
                    _column(result.table, finding.column, dataset),
                    _level(finding),
                )

        missing = grouped.get(COLUMN_PRESENT, [])
        if missing:
            required = [f.column for f in missing if f.required]
            optional = [f.column for f in missing if not f.required]
            parts = []
            if required:
                parts.append(f"required: {', '.join(required)}")
            if optional:
                parts.append(f"optional: {', '.join(optional)}")
            add(
                f"Table {result.table} lacks {len(missing)} column(s) of the "
                f"codoc data model ({'; '.join(parts)}).",
                table_scope,
                "high" if required else "warning",
            )
        unexpected = grouped.get(UNEXPECTED_COLUMN, [])
        if unexpected:
            add(
                f"Table {result.table} carries {len(unexpected)} column(s) "
                "the codoc data model does not define: "
                f"{', '.join(f.column for f in unexpected)}.",
                table_scope,
                "info",
            )

    for key in unmatched_objects:
        add(
            f"Source object {key} matches no codoc table in scope and was not checked. "
            "Rename it after its codoc table, or set job.table.",
            _dataset(dataset),
            "info",
        )
    return recommendations


def _finding_text(finding: Finding) -> str:
    where = f"{finding.table}.{finding.column}"
    if finding.check == COLUMN_TYPE:
        if finding.violations:
            return (
                f"{where} is stored as {finding.actual} instead of "
                f"{finding.expected}: {_count(finding)} do not convert."
            )
        return f"{where} is stored as {finding.actual} instead of {finding.expected}."
    if finding.check == NOT_NULL:
        return f"{where} is required but null in {_count(finding)}."
    if finding.check == PRIMARY_KEY:
        return (
            f"{where} is the primary key but has null or duplicate values "
            f"in {_count(finding)}."
        )
    if finding.check == MAX_LENGTH:
        return f"{where} exceeds {finding.expected} in {_count(finding)}."
    return f"{where}: {finding.detail}"


def build_schemas(results: List[TableResult], dataset: str) -> List[dict]:
    """Tree of present tables and their columns, model columns included.

    A model column missing from the source still enters the tree, so the
    ``column_missing`` metric scoped to it has a node to hang off.
    """
    schemas = []
    for result in results:
        if not result.present:
            continue
        schemas.append(
            {
                "key": "table",
                "value": result.table,
                "scope": _table(result.table, dataset),
            }
        )
        columns = []
        for finding in result.findings:
            if finding.column is not None and finding.column not in columns:
                columns.append(finding.column)
        schemas.extend(
            {
                "key": "column",
                "value": f"{result.table}.{column}",
                "scope": _column(result.table, column, dataset),
            }
            for column in columns
        )
    return schemas


def declare_figures(figures, results: List[TableResult], dataset: str) -> None:
    figures.declare_measure(
        "score",
        unit="score",
        direction="higher_is_better",
        target=1.0,
        warn=0.8,
        label="Conformité au modèle codoc",
    )
    figures.declare_measure(
        "n_checks", unit="count", direction="neutral", label="Contrôles"
    )
    figures.declare_measure(
        "n_failed",
        unit="count",
        direction="lower_is_better",
        target=0,
        label="Contrôles en échec",
    )
    scope = _dataset(dataset)

    figures.add(
        "score_by_table",
        intent="breakdown",
        of="score",
        frame=[
            {"table": r.table, "score": round(r.score, 4)} for r in results
        ],
        dims=["table"],
        measures=["score"],
        scope=scope,
        title="Conformité par table",
    )

    outcome = Counter(
        "pass" if f.passed else "fail"
        for r in results
        for f in r.findings
        if f.scored
    )
    figures.add(
        "checks_outcome",
        intent="composition",
        frame=[
            {"status": k, "n_checks": outcome.get(k, 0)}
            for k in ("pass", "fail")
        ],
        dims=["status"],
        measures=["n_checks"],
        scope=scope,
        title="Résultat des contrôles",
    )

    failed = Counter(
        f.check
        for r in results
        for f in r.findings
        if f.scored and not f.passed
    )
    if failed:
        figures.add(
            "failures_by_check",
            intent="breakdown",
            of="score",
            frame=[
                {"check": k, "n_failed": v} for k, v in sorted(failed.items())
            ],
            dims=["check"],
            measures=["n_failed"],
            scope=scope,
            title="Échecs par type de contrôle",
        )
