"""Conformance of one source table to its codoc model definition.

Two kinds of checks run here:

* **structural**, answered from the parquet footers alone: is every model
  column present, and does its stored type belong to the declared family;
* **constraints**, answered by a single streaming aggregation per table:
  NOT NULL on required columns, uniqueness of the primary key, the
  ``varchar(n)`` length limit, and -- for a column stored under another type --
  whether every value would convert to the declared one.

Every check yields a :class:`Finding`. Only aggregate counts ever leave this
module: no source value is copied into a finding.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import polars as pl

from codoc_model.spec import Column, Table

TABLE_PRESENT = "table_present"
COLUMN_PRESENT = "column_present"
COLUMN_TYPE = "column_type"
NOT_NULL = "not_null"
PRIMARY_KEY = "primary_key_unique"
MAX_LENGTH = "max_length"
UNEXPECTED_COLUMN = "unexpected_column"

# How a stored type relates to the declared one.
MATCH = "match"
CONVERTIBLE = "convertible"  # every value converts, the type does not match
MISMATCH = "mismatch"

BOOLEAN_TEXT = ["true", "false", "t", "f", "1", "0", "yes", "no", "y", "n"]


@dataclass
class Finding:
    table: str
    check: str
    passed: bool
    column: Optional[str] = None
    scored: bool = True
    expected: Optional[str] = None
    actual: Optional[str] = None
    violations: int = 0
    rows: int = 0
    required: bool = False
    detail: str = ""

    @property
    def violation_rate(self) -> float:
        return self.violations / self.rows if self.rows else 0.0


@dataclass
class TableResult:
    table: str
    present: bool
    rows: int = 0
    findings: List[Finding] = field(default_factory=list)

    @property
    def score(self) -> float:
        """Share of scored checks that passed; a missing table scores 0."""
        if not self.present:
            return 0.0
        scored = [f for f in self.findings if f.scored]
        if not scored:
            return 1.0
        return sum(f.passed for f in scored) / len(scored)


def family_of(dtype: pl.DataType) -> str:
    """The codoc type family a polars dtype stores, or ``other``."""
    if dtype == pl.Null:
        return "null"
    if dtype.is_integer():
        return "integer"
    if isinstance(dtype, pl.Decimal):
        return "integer" if (dtype.scale or 0) == 0 else "float"
    if dtype.is_float():
        return "float"
    if dtype in (pl.String, pl.Categorical) or isinstance(dtype, pl.Enum):
        return "string"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    if dtype == pl.Date:
        return "date"
    if dtype == pl.Boolean:
        return "boolean"
    return "other"


def type_name(dtype: pl.DataType) -> str:
    try:
        return dtype.base_type().__name__
    except AttributeError:
        return str(dtype)


def classify_type(expected: str, actual: str) -> str:
    """Relation of a stored family to a declared one, before reading data.

    ``CONVERTIBLE`` here means "convertible by construction" (an integer
    always reads as text, a date always widens to a timestamp). Text that may
    or may not parse as the declared type is :func:`needs_data_check`.
    """
    if actual in (expected, "null"):
        return MATCH
    if expected == "float" and actual == "integer":
        return MATCH
    if expected == "string" and actual != "other":
        return CONVERTIBLE
    if (expected, actual) in (("datetime", "date"), ("date", "datetime")):
        return CONVERTIBLE
    if needs_data_check(expected, actual):
        return CONVERTIBLE
    return MISMATCH


def needs_data_check(expected: str, actual: str) -> bool:
    """Whether convertibility depends on the values themselves."""
    if actual == "string":
        return expected in ("integer", "float", "datetime", "date", "boolean")
    return (expected, actual) in (("integer", "float"), ("boolean", "integer"))


def unconvertible_expr(name: str, expected: str, actual: str) -> pl.Expr:
    """Count of non-null values that would not convert to ``expected``."""
    col = pl.col(name)
    if actual == "string":
        text = col.cast(pl.String).str.strip_chars()
        if expected == "integer":
            converted = text.cast(pl.Int64, strict=False)
        elif expected == "float":
            converted = text.cast(pl.Float64, strict=False)
        elif expected == "datetime":
            # A time zone is required as soon as one value carries an offset;
            # naive values are read as UTC, which changes nothing to whether
            # they parse.
            converted = text.str.to_datetime(strict=False, time_zone="UTC")
        elif expected == "date":
            converted = text.str.to_date(strict=False)
        else:  # boolean
            return (
                col.is_not_null()
                & ~text.str.to_lowercase().is_in(BOOLEAN_TEXT)
            ).sum()
        return (col.is_not_null() & converted.is_null()).sum()
    if expected == "integer":  # stored as float or decimal
        number = col.cast(pl.Float64)
        return (number.is_not_null() & (number != number.round(0))).sum()
    return (col.is_not_null() & ~col.is_in([0, 1])).sum()  # boolean as int


def resolve_columns(schema: Dict[str, pl.DataType]) -> Dict[str, str]:
    """Lower-cased column name -> name as stored (Oracle upper-cases)."""
    resolved = {}
    for name in schema:
        resolved.setdefault(name.lower(), name)
    return resolved


def check_table(
    table: Table,
    frame: pl.LazyFrame,
    extensions=(),
    strict_types: bool = False,
    extra=(),
    relaxed_required=(),
) -> TableResult:
    """Every check of one present table, in model column order.

    ``extra`` holds further ``(finding, violation count expression)`` pairs
    -- the value conventions -- folded into the same single aggregation.
    ``relaxed_required`` names required columns whose NOT NULL check is
    dropped.
    """
    schema = dict(frame.collect_schema())
    stored = resolve_columns(schema)
    known = table.known_columns()
    result = TableResult(table=table.name, present=True)
    result.findings.append(Finding(table.name, TABLE_PRESENT, passed=True))

    aggregations: List[pl.Expr] = [pl.len().alias("__rows")]
    # Findings whose violation count comes back from the aggregation, keyed
    # by the alias the count is collected under.
    pending: Dict[str, Finding] = {}

    def defer(finding: Finding, expr: pl.Expr) -> None:
        alias = f"__{len(pending)}"
        aggregations.append(expr.alias(alias))
        pending[alias] = finding
        result.findings.append(finding)

    for column in _present_and_expected(table, stored, extensions):
        name = stored.get(column.name)
        if name is None:
            result.findings.append(
                Finding(
                    table.name,
                    COLUMN_PRESENT,
                    passed=False,
                    column=column.name,
                    expected=column.declared_type,
                    required=column.required,
                    detail="column missing from the source table",
                )
            )
            continue
        result.findings.append(
            Finding(
                table.name,
                COLUMN_PRESENT,
                passed=True,
                column=column.name,
                required=column.required,
            )
        )
        _column_checks(
            column,
            name,
            schema[name],
            strict_types,
            defer,
            result,
            check_not_null=column.name not in relaxed_required,
        )

    for lowered, name in stored.items():
        if lowered not in known:
            result.findings.append(
                Finding(
                    table.name,
                    UNEXPECTED_COLUMN,
                    passed=False,
                    column=name,
                    scored=False,
                    actual=type_name(schema[name]),
                    detail="column not defined by the codoc data model",
                )
            )

    for finding, expr in extra:
        defer(finding, expr)

    counts = (
        frame.select(aggregations)
        .collect(engine="streaming")
        .row(0, named=True)
    )
    result.rows = int(counts["__rows"])
    for alias, finding in pending.items():
        finding.violations = int(counts[alias] or 0)
        finding.rows = result.rows
        finding.passed = finding.violations == 0
        if finding.check == COLUMN_TYPE:
            # A column stored as text that fully converts is still not the
            # declared type: it passes only when types are not strict.
            finding.passed = finding.passed and not strict_types
            if finding.violations:
                finding.detail = (
                    f"{finding.violations} value(s) do not convert to "
                    f"{finding.expected}"
                )
    return result


def _present_and_expected(
    table: Table, stored: Dict[str, str], extensions
) -> List[Column]:
    """Expected columns, plus extension columns the source carries anyway."""
    expected = table.expected_columns(extensions)
    names = {column.name for column in expected}
    extra = [
        column
        for column in table.extension_columns
        if column.name not in names and column.name in stored
    ]
    return expected + extra


def _column_checks(  # pylint: disable=too-many-arguments
    column,
    name,
    dtype,
    strict_types,
    defer,
    result,
    *,
    check_not_null=True,
) -> None:
    table = result.table
    actual = family_of(dtype)
    relation = classify_type(column.family, actual)
    type_finding = Finding(
        table,
        COLUMN_TYPE,
        passed=relation == MATCH
        or (relation == CONVERTIBLE and not strict_types),
        column=column.name,
        expected=column.declared_type,
        actual=type_name(dtype),
        required=column.required,
    )
    if relation == MISMATCH:
        type_finding.detail = (
            f"stored as {type_name(dtype)}, declared {column.declared_type}"
        )
    elif relation == CONVERTIBLE:
        type_finding.detail = (
            f"stored as {type_name(dtype)}, declared {column.declared_type}; "
            "values convert"
        )
    if relation == CONVERTIBLE and needs_data_check(column.family, actual):
        defer(type_finding, unconvertible_expr(name, column.family, actual))
    else:
        result.findings.append(type_finding)

    if column.required and check_not_null:
        defer(
            Finding(
                table,
                NOT_NULL,
                passed=True,
                column=column.name,
                required=True,
                detail="null value(s) in a required column",
            ),
            pl.col(name).null_count(),
        )
    if column.primary_key:
        defer(
            Finding(
                table,
                PRIMARY_KEY,
                passed=True,
                column=column.name,
                required=True,
                detail="duplicate or null primary key value(s)",
            ),
            pl.len() - pl.col(name).drop_nulls().n_unique(),
        )
    if column.max_length and actual == "string":
        defer(
            Finding(
                table,
                MAX_LENGTH,
                passed=True,
                column=column.name,
                expected=column.declared_type,
                required=column.required,
                detail=f"value(s) longer than {column.max_length} characters",
            ),
            (
                pl.col(name).cast(pl.String).str.len_chars()
                > column.max_length
            ).sum(),
        )


def missing_table(table: Table) -> TableResult:
    result = TableResult(table=table.name, present=False)
    result.findings.append(
        Finding(
            table.name,
            TABLE_PRESENT,
            passed=False,
            required=True,
            detail="table missing from the source",
        )
    )
    return result
