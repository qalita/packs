"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

HL7 FHIR conformance checks over a tabular dataset.

The previous implementation looped ``for idx in range(row_count)`` and, for each
mapped field, did ``series.iloc[idx]``, a ``re.match`` and a
``datetime.fromisoformat`` inside a ``try/except``. That is O(rows x fields) of
Python interpreter: at 1e9 rows it does not terminate, whatever the machine's
memory. It also read only the first parquet part, so on a chunked source it
scored the first chunk and reported it as the dataset.

Every rule is now a Polars expression over the mapped column — required,
enum, pattern, ISO date, boolean — reduced horizontally into one "this record is
invalid" flag. Validity, per-field violation counts and completeness all come
out of a single streaming aggregation per table.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack

logger = logging.getLogger("fhir_compliance_pack")

DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000

BOOLEAN_LITERALS = ["true", "false", "1", "0", "yes", "no"]

# ISO-8601 shapes accepted by datetime.fromisoformat, which is what the pandas
# version used. The shape check alone would accept 2023-02-30, so it is paired
# with an explicit-format parse below.
ISO_DATE_SHAPE = (
    r"^\d{4}-\d{2}-\d{2}"
    r"([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _job(pack: Pack) -> Dict[str, Any]:
    return pack.pack_config.get("job", {}) or {}


def _example_limit(job: Dict[str, Any]) -> int:
    raw = job.get("examples", DEFAULT_EXAMPLE_ROWS)
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        limit = DEFAULT_EXAMPLE_ROWS
    return max(0, min(limit, MAX_EXAMPLE_ROWS))


def _examples_value(frame: "pl.DataFrame") -> str:
    """Serialize bounded example rows.

    The platform stores a metric value as a string, so the rows travel as JSON
    rather than as a list that would be rejected on ingestion.
    """
    return json.dumps(frame.to_dicts(), ensure_ascii=False, default=str)


def _text(column: Optional[str]) -> "pl.Expr":
    """The mapped column as trimmed text, or a null literal when unmapped.

    A FHIR field mapped to a column the dataset does not have has to behave like
    a column full of nulls, which is what the pandas version did with
    ``mapped[field] = None``. It has to be repeated to the frame height rather
    than left as a literal: a scalar would aggregate to one row, so a required
    field on an absent column would be counted as one violation instead of one
    per record.
    """
    if column is None:
        return pl.repeat(None, pl.len(), dtype=pl.Utf8)
    return pl.col(column).cast(pl.Utf8).str.strip_chars()


def _present(value: "pl.Expr") -> "pl.Expr":
    return value.is_not_null() & (value.str.len_chars() > 0)


def _anchored(regex: str) -> str:
    """Emulate ``re.match``, which anchors at the start of the string."""
    return regex if regex.startswith("^") else f"^{regex}"


def _field_violation(
    field: str,
    value: "pl.Expr",
    present: "pl.Expr",
    job: Dict[str, Any],
) -> "pl.Expr":
    """Every FHIR rule for one field, as one boolean expression.

    Rules other than ``required`` only apply to a populated value, exactly as
    the row-by-row version did: an absent optional field is not a violation.
    """
    required = set(job.get("required_fields", []) or [])
    enums = job.get("enums", {}) or {}
    patterns = job.get("patterns", {}) or {}
    date_fields = set(job.get("date_fields", []) or [])
    boolean_fields = set(job.get("boolean_fields", []) or [])

    checks: List[pl.Expr] = []

    if field in required:
        checks.append(~present)

    if field in enums:
        allowed = [str(item) for item in enums[field]]
        checks.append(present & ~value.is_in(allowed))

    if field in patterns:
        checks.append(
            present & ~value.str.contains(_anchored(str(patterns[field])))
        )

    if field in date_fields:
        # Explicit format on purpose: format inference is decided per batch, so
        # on a multi-part source it could accept a value in one part and reject
        # the same value in another.
        parsed = value.str.slice(0, 10).str.to_date(
            format="%Y-%m-%d", strict=False
        )
        checks.append(
            present
            & ~(value.str.contains(ISO_DATE_SHAPE) & parsed.is_not_null())
        )

    if field in boolean_fields:
        checks.append(
            present & ~value.str.to_lowercase().is_in(BOOLEAN_LITERALS)
        )

    if not checks:
        return pl.lit(False)
    return pl.any_horizontal(checks).fill_null(False)


def _rules(
    schema: Dict[str, Any], job: Dict[str, Any]
) -> Tuple[Dict[str, "pl.Expr"], Dict[str, "pl.Expr"], List[str]]:
    """Per-field violation and presence expressions, plus the unmapped fields."""
    mappings = job.get("field_mappings", {}) or {}
    violations: Dict[str, pl.Expr] = {}
    presence: Dict[str, pl.Expr] = {}
    unmapped: List[str] = []

    for field, column in mappings.items():
        if column not in schema:
            unmapped.append(field)
            column = None
        value = _text(column)
        present = _present(value)
        presence[field] = present
        violations[field] = _field_violation(field, value, present, job)

    return violations, presence, unmapped


def _evaluate(
    lf: "pl.LazyFrame",
    violations: Dict[str, "pl.Expr"],
    presence: Dict[str, "pl.Expr"],
) -> Dict[str, Any]:
    """Row total, invalid records and per-field counts, in ONE streaming pass."""
    exprs: Dict[str, pl.Expr] = {"__rows": pl.len()}
    for field, expr in violations.items():
        exprs[f"violated|{field}"] = expr.sum()
    for field, expr in presence.items():
        exprs[f"present|{field}"] = expr.fill_null(False).sum()
    if violations:
        exprs["__invalid"] = pl.any_horizontal(list(violations.values())).sum()
    return analytics.agg(lf, exprs)


def _invalid_predicate(violations: Dict[str, "pl.Expr"]) -> "pl.Expr":
    if not violations:
        return pl.lit(False)
    return pl.any_horizontal(list(violations.values()))


def _metric(key: str, value: Any, scope: Dict[str, Any]) -> Dict[str, Any]:
    return {"key": key, "value": value, "scope": scope}


def run(pack: Pack) -> None:
    if pack.source_config.get("type") == "database":
        table_or_query = pack.source_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type source, you must specify "
                "'table_or_query' in the config."
            )
        pack.load_data("source", table_or_query=table_or_query)
    else:
        pack.load_data("source")

    job = _job(pack)
    mappings = job.get("field_mappings", {}) or {}
    example_limit = _example_limit(job)

    dataset_name = pack.source_config["name"]
    tables = pack.tables("source")
    single_table = len(tables) == 1

    total_records = 0
    valid_records = 0
    completeness_numerator = 0.0

    for table in tables:
        dataset_label = dataset_name if single_table else table
        dataset_scope = {"perimeter": "dataset", "value": dataset_label}

        lf = pack.scan("source", table)
        schema = pack.schema("source", table)
        violations, presence, unmapped = _rules(schema, job)
        for field in unmapped:
            logger.warning(
                "FHIR field '%s' maps to a column absent from '%s'",
                field,
                table,
            )

        stats = _evaluate(lf, violations, presence)
        rows = int(stats.get("__rows") or 0)
        invalid = int(stats.get("__invalid") or 0)
        field_count = len(mappings)

        present_total = sum(
            int(stats.get(f"present|{field}") or 0) for field in presence
        )
        completeness = (
            0.0
            if not rows or not field_count
            else present_total / (rows * field_count)
        )

        total_records += rows
        valid_records += rows - invalid
        completeness_numerator += completeness * rows

        dataset_validity = 0.0 if rows == 0 else (rows - invalid) / rows
        pack.metrics.data.extend(
            [
                _metric(
                    "completeness", str(round(completeness, 4)), dataset_scope
                ),
                _metric("records", rows, dataset_scope),
                _metric("invalid_records", invalid, dataset_scope),
                _metric(
                    "validity_ratio",
                    str(round(dataset_validity, 4)),
                    dataset_scope,
                ),
            ]
        )

        for field in violations:
            violated = int(stats.get(f"violated|{field}") or 0)
            present_count = int(stats.get(f"present|{field}") or 0)
            field_scope = {
                "perimeter": "column",
                "value": mappings.get(field, field),
                "parent_scope": dataset_scope,
            }
            pack.metrics.data.extend(
                [
                    _metric("field_violations", violated, field_scope),
                    _metric(
                        "field_completeness",
                        str(round(present_count / rows, 4)) if rows else "0.0",
                        field_scope,
                    ),
                ]
            )
            if violated:
                pack.recommendations.data.append(
                    {
                        "content": (
                            f"FHIR field '{field}' is invalid on {violated} "
                            f"record(s) of '{dataset_label}'."
                        ),
                        "type": "FHIR Conformance Violation",
                        "scope": dict(field_scope),
                        "level": (
                            "high"
                            if rows and violated / rows > 0.05
                            else "warning"
                        ),
                    }
                )

        if invalid and example_limit:
            mapped_columns = [
                column for column in mappings.values() if column in schema
            ]
            _, rows_frame = analytics.failures(
                lf,
                _invalid_predicate(violations),
                limit=example_limit,
                columns=mapped_columns or None,
            )
            if rows_frame.height:
                pack.metrics.data.append(
                    _metric(
                        "invalid_record_examples",
                        _examples_value(rows_frame),
                        dataset_scope,
                    )
                )

    validity_ratio = (
        0.0 if total_records == 0 else valid_records / total_records
    )
    overall_completeness = (
        0.0 if total_records == 0 else completeness_numerator / total_records
    )
    root_scope = {"perimeter": "dataset", "value": dataset_name}
    pack.metrics.data.extend(
        [
            _metric("score", str(round(validity_ratio, 2)), root_scope),
            _metric("valid_records", valid_records, root_scope),
        ]
    )
    if not single_table:
        # With one table the loop above already emitted these at exactly this
        # scope; repeating them would put two rows behind one (key, scope).
        pack.metrics.data.extend(
            [
                _metric(
                    "validity_ratio",
                    str(round(validity_ratio, 4)),
                    root_scope,
                ),
                _metric(
                    "completeness",
                    str(round(overall_completeness, 4)),
                    root_scope,
                ),
                _metric("records", total_records, root_scope),
            ]
        )

    pack.metrics.save()
    pack.recommendations.save()


if __name__ == "__main__":
    with Pack() as _pack:
        run(_pack)
