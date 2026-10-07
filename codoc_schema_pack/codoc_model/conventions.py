"""Value conventions of the codoc ETL guide.

``model.json`` holds what the upstream CSVs state as structure (type, required,
keys). The *ETL Conventions* column states rules on values, in prose; the
checkable ones are transcribed here by hand, against the upstream commit that
``model.json`` is pinned to. Conventions that cannot be verified from the data
are left out on purpose: values the guide calls "not standardized yet"
(``entry_mode``, ``mvt_exit_mode``...), how a ``*_pid`` hash was computed, and
the January-1st default of partial dates.

Three shapes of rule:

* **row rules** reduce to one count over the table, so they join the single
  streaming aggregation of :func:`codoc_model.checks.check_table`;
* **grouped rules** need a ``group_by`` first (one master identifier per
  patient) and run as a query of their own;
* **cross-table rules** (instance codes known to ``hospital_instance``) run
  once every table is loaded, see :func:`instance_code_findings`.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import polars as pl

from codoc_model.checks import (
    Finding,
    TableResult,
    family_of,
    resolve_columns,
)

ACCEPTED_VALUES = "accepted_values"
DATE_ORDER = "date_order"
CONDITIONAL_VALUE = "conditional_value"
UPLOAD_ID_FORMAT = "upload_id_format"
CNIL_NULL = "cnil_null"
MASTER_IDENTIFIER = "master_identifier"
INSTANCE_CODE = "instance_code"

TRUE_TEXT = ["true", "t", "1", "yes", "y"]
UPLOAD_ID_PATTERN = "%Y%m%d%H%M%S"

# "*null* in CNIL compliant warehouse". encounter_num is also declared
# Required upstream: in a CNIL warehouse its NOT NULL check is dropped, since
# the two statements cannot both hold.
CNIL_COLUMNS = {
    "dwh_patient": (
        "lastname",
        "maiden_name",
        "firstname",
        "nss",
        "phone_number",
        "email",
        "residence_address",
    ),
    "dwh_patient_stay": ("encounter_num",),
}

# "Code of the healthcare center, see hospital_instance"; hospital_instance.code
# "Should match instance_xxx_id in clinical tables". The bigint instance_id of
# dwh_thesaurus_site/department references hospital_instance.id instead: a
# foreign key, not a code, so it is not checked here.
INSTANCE_COLUMNS = {
    "dwh_patient": "instance_id",
    "dwh_patient_stay": "instance_stay_id",
    "dwh_patient_mvt": "instance_mvt_id",
    "dwh_patient_ipphist": "instance_ipp_id",
    "dwh_document": "instance_document_id",
    "dwh_data": "instance_data_id",
}


@dataclass(frozen=True)
class Rule:
    kind: str
    table: str
    column: str  # the column the finding is reported on
    other: Optional[str] = None  # second column of a two-column rule
    values: Tuple[str, ...] = ()
    expected: str = ""  # the convention, as reported to the user

    @property
    def columns(self) -> Tuple[str, ...]:
        return (self.column,) + ((self.other,) if self.other else ())

    @property
    def grouped(self) -> bool:
        return self.kind == MASTER_IDENTIFIER


def _accepted(table, column, values) -> Rule:
    return Rule(
        ACCEPTED_VALUES,
        table,
        column,
        values=tuple(values),
        expected=", ".join(values),
    )


VALUE_RULES = [
    # "F (female), M (male), O (other), empty if unknown"
    _accepted("dwh_patient", "sex", ["F", "M", "O"]),
    # "null if alive, d if dead"
    _accepted("dwh_patient", "death_code", ["d"]),
    _accepted(
        "dwh_patient_stay",
        "type_dos",
        [
            "Consultation",
            "HDJ",
            "HAD",
            "Urgence",
            "Hospitalisation",
            "Ambulatoire",
            "Externes",
        ],
    ),
    _accepted(
        "dwh_patient_mvt",
        "type_mvt",
        ["C", "J", "U", "H", "A", "S", "AM", "AP", "E"],
    ),
    _accepted(
        "dwh_thesaurus_data",
        "value_type",
        ["numeric", "text", "present", "liste"],
    ),
    # PMSI extension: "30: CMP, 31: ..., 32: CATTP"
    _accepted("dwh_data", "activity_form", ["30", "31", "32"]),
    # "out_date should be greater or equal than entry_date". Stated on the
    # stay; a movement carries the same admission/discharge pair.
    Rule(
        DATE_ORDER,
        "dwh_patient_stay",
        "out_date",
        other="entry_date",
        expected="out_date >= entry_date",
    ),
    Rule(
        DATE_ORDER,
        "dwh_patient_mvt",
        "out_date",
        other="entry_date",
        expected="out_date >= entry_date",
    ),
    # "List of values when type is liste else null"
    Rule(
        CONDITIONAL_VALUE,
        "dwh_thesaurus_data",
        "list_values",
        other="value_type",
        expected="set if and only if value_type = liste",
    ),
    # "Each patient should have one and only one master identifier."
    Rule(
        MASTER_IDENTIFIER,
        "dwh_patient_ipphist",
        "master_patient_id",
        other="patient_num",
        expected="exactly one master identifier per patient",
    ),
]


def rules_for(table, enabled: bool = True, cnil: bool = False) -> List[Rule]:
    """Conventions applying to one model table."""
    rules = []
    if enabled:
        rules = [r for r in VALUE_RULES if r.table == table.name]
        if "upload_id" in table.known_columns():
            # datetime.now().strftime("%Y%m%d%H%M%S") at pipeline start
            rules.append(
                Rule(
                    UPLOAD_ID_FORMAT,
                    table.name,
                    "upload_id",
                    expected="YYYYMMDDHHMMSS",
                )
            )
    if cnil:
        rules.extend(
            Rule(CNIL_NULL, table.name, column, expected="null")
            for column in CNIL_COLUMNS.get(table.name, ())
        )
    return rules


def relaxed_required(table_name: str, cnil: bool) -> Tuple[str, ...]:
    """Required columns whose NOT NULL check a CNIL warehouse drops."""
    return CNIL_COLUMNS.get(table_name, ()) if cnil else ()


def _text(name: str, dtype: pl.DataType) -> pl.Expr:
    """Values as stripped text, empty text as null.

    Integers stored as floats (``30.0``) are read as integers first, so a
    numeric code compares equal to its text form.
    """
    col = pl.col(name)
    if family_of(dtype) == "float":
        col = col.cast(pl.Int64, strict=False)
    text = col.cast(pl.String).str.strip_chars()
    return pl.when(text == "").then(None).otherwise(text)


def _timestamp(name: str, dtype: pl.DataType) -> pl.Expr:
    """Values as UTC timestamps; whatever does not parse becomes null."""
    col = pl.col(name)
    family = family_of(dtype)
    if family == "datetime":
        if dtype.time_zone:
            return col.dt.convert_time_zone("UTC")
        return col.dt.replace_time_zone("UTC")
    if family == "date":
        return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
    return _text(name, dtype).str.to_datetime(strict=False, time_zone="UTC")


def _truthy(name: str, dtype: pl.DataType) -> pl.Expr:
    family = family_of(dtype)
    if family == "boolean":
        return pl.col(name).fill_null(False)
    if family in ("integer", "float"):
        return pl.col(name).fill_null(0) == 1
    return (
        _text(name, dtype).str.to_lowercase().is_in(TRUE_TEXT).fill_null(False)
    )


def violation_expr(
    rule: Rule, names: Dict[str, str], schema: Dict[str, pl.DataType]
) -> pl.Expr:
    """Number of rows breaking a row rule.

    ``names`` maps model column names to the names the source stores them
    under. A null value never breaks a value rule: whether it may be null is
    the NOT NULL check's business.
    """
    name = names[rule.column]
    dtype = schema[name]
    if rule.kind == ACCEPTED_VALUES:
        value = _text(name, dtype)
        return (value.is_not_null() & ~value.is_in(rule.values)).sum()
    if rule.kind == DATE_ORDER:
        end = _timestamp(name, dtype)
        start = _timestamp(names[rule.other], schema[names[rule.other]])
        return (end < start).fill_null(False).sum()
    if rule.kind == CONDITIONAL_VALUE:
        kind = _text(names[rule.other], schema[names[rule.other]])
        has_list = _text(name, dtype).is_not_null()
        return (kind.is_not_null() & ((kind == "liste") != has_list)).sum()
    if rule.kind == UPLOAD_ID_FORMAT:
        value = _text(name, dtype)
        parsed = value.str.to_datetime(UPLOAD_ID_PATTERN, strict=False)
        return (
            value.is_not_null()
            & (parsed.is_null() | (value.str.len_chars() != 14))
        ).sum()
    if rule.kind == CNIL_NULL:
        return _text(name, dtype).is_not_null().sum()
    raise ValueError(f"{rule.kind} is not a row rule")


def rule_checks(
    table_name: str, frame: pl.LazyFrame, rules: List[Rule]
) -> Tuple[List[Tuple[Finding, pl.Expr]], List[Finding]]:
    """``(row checks to aggregate, grouped checks already evaluated)``.

    A rule whose columns are not all in the source is not applicable and
    yields nothing: the missing column is already a ``column_present``
    failure.
    """
    schema = dict(frame.collect_schema())
    names = resolve_columns(schema)
    deferred, evaluated = [], []
    for rule in rules:
        if not all(column in names for column in rule.columns):
            continue
        finding = Finding(
            table_name,
            rule.kind,
            passed=True,
            column=rule.column,
            expected=rule.expected,
        )
        if rule.grouped:
            violations, groups = grouped_violations(rule, frame, names, schema)
            finding.violations, finding.rows = violations, groups
            finding.passed = violations == 0
            evaluated.append(finding)
        else:
            deferred.append((finding, violation_expr(rule, names, schema)))
    return deferred, evaluated


def grouped_violations(
    rule: Rule,
    frame: pl.LazyFrame,
    names: Dict[str, str],
    schema: Dict[str, pl.DataType],
) -> Tuple[int, int]:
    """``(patients without exactly one master identifier, patients)``."""
    name = names[rule.column]
    patient = names[rule.other]
    counts = (
        frame.filter(pl.col(patient).is_not_null())
        .group_by(patient)
        .agg(_truthy(name, schema[name]).sum().alias("masters"))
        .select(
            (pl.col("masters") != 1).sum().alias("violations"),
            pl.len().alias("patients"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return int(counts["violations"] or 0), int(counts["patients"] or 0)


def instance_code_findings(
    results: Dict[str, TableResult], frames: Dict[str, pl.LazyFrame]
) -> None:
    """Append, to each clinical table, whether its instance codes are known.

    Applicable only when ``hospital_instance`` was loaded with its ``code``
    column: without the referential there is nothing to match against.
    """
    referential = frames.get("hospital_instance")
    if referential is None:
        return
    ref_schema = dict(referential.collect_schema())
    ref_names = resolve_columns(ref_schema)
    if "code" not in ref_names:
        return
    codes = (
        referential.select(
            _text(ref_names["code"], ref_schema[ref_names["code"]])
            .drop_nulls()
            .unique()
        )
        .collect(engine="streaming")
        .to_series()
        .to_list()
    )

    for table, column in INSTANCE_COLUMNS.items():
        frame = frames.get(table)
        if frame is None or table not in results:
            continue
        schema = dict(frame.collect_schema())
        names = resolve_columns(schema)
        if column not in names:
            continue
        value = _text(names[column], schema[names[column]])
        counts = (
            frame.select(
                (value.is_not_null() & ~value.is_in(codes))
                .sum()
                .alias("violations"),
                pl.len().alias("rows"),
            )
            .collect(engine="streaming")
            .row(0, named=True)
        )
        violations = int(counts["violations"] or 0)
        results[table].findings.append(
            Finding(
                table,
                INSTANCE_CODE,
                passed=violations == 0,
                column=column,
                expected="a hospital_instance.code",
                violations=violations,
                rows=int(counts["rows"]),
            )
        )
