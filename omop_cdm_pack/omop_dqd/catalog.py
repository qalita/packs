"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements the check instantiation performed upstream in
R/RunCheck.R and R/Execution.R, driven by inst/csv/ metadata.
See the NOTICE file at the pack root.
"""

import csv
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from omop_dqd import SUPPORTED_CDM_VERSIONS, VENDOR_CSV_DIR

# Trigger modes decide when a CSV cell instantiates a check.
#   ALWAYS - one instance per CSV row, unconditionally
#   YES    - instantiate when the cell equals "Yes"
#   VALUE  - instantiate when the cell is non-empty; the cell content
#            becomes params["value"] (a bound, a domain, a datatype...)
TRIGGER_ALWAYS = "always"
TRIGGER_YES = "yes"
TRIGGER_VALUE = "value"


@dataclass(frozen=True)
class CheckSpec:
    """How one check column of a metadata CSV instantiates checks.

    trigger_column  column that gates instantiation; defaults to `name`
    value_column    column supplying params["value"]; defaults to `name`
    trigger_equals  for TRIGGER_VALUE, the cell must equal this
                    (case-insensitive) rather than merely be non-empty
    requires        prerequisite (column, value) pairs, all of which
                    must hold for the check to instantiate

    These exist to express Check_Descriptions.csv's `evaluationFilter`
    column declaratively for the handful of checks whose filter is
    not equivalent to the plain trigger mode. There is no general
    expression interpreter: the four exceptions are hardcoded per
    spec below, auditable against the CSV column.
    """

    name: str
    trigger: str
    param_columns: Tuple[str, ...] = ()
    trigger_column: Optional[str] = None
    value_column: Optional[str] = None
    trigger_equals: Optional[str] = None
    requires: Tuple[Tuple[str, str], ...] = ()


# Field-level checks, in the order they appear in Check_Descriptions.csv.
FIELD_CHECK_SPECS: Tuple[CheckSpec, ...] = (
    CheckSpec("cdmField", TRIGGER_ALWAYS),
    CheckSpec("isRequired", TRIGGER_YES),
    CheckSpec("cdmDatatype", TRIGGER_VALUE, trigger_equals="integer"),
    CheckSpec("isPrimaryKey", TRIGGER_YES),
    CheckSpec("isForeignKey", TRIGGER_YES, ("fkTableName", "fkFieldName")),
    CheckSpec("fkDomain", TRIGGER_VALUE, requires=(("isForeignKey", "Yes"),)),
    CheckSpec("fkClass", TRIGGER_VALUE, requires=(("isForeignKey", "Yes"),)),
    CheckSpec("isStandardValidConcept", TRIGGER_YES),
    CheckSpec("measureValueCompleteness", TRIGGER_YES),
    CheckSpec("standardConceptRecordCompleteness", TRIGGER_YES),
    CheckSpec(
        "sourceConceptRecordCompleteness",
        TRIGGER_YES,
        ("standardConceptFieldName",),
    ),
    CheckSpec(
        "sourceValueCompleteness",
        TRIGGER_YES,
        param_columns=("standardConceptFieldName",),
    ),
    CheckSpec("plausibleValueLow", TRIGGER_VALUE),
    CheckSpec("plausibleValueHigh", TRIGGER_VALUE),
    CheckSpec(
        "plausibleTemporalAfter",
        TRIGGER_YES,
        (
            "plausibleTemporalAfterTableName",
            "plausibleTemporalAfterFieldName",
        ),
    ),
    CheckSpec("plausibleDuringLife", TRIGGER_YES),
    CheckSpec(
        "plausibleStartBeforeEnd",
        TRIGGER_YES,
        ("plausibleStartBeforeEndFieldName",),
    ),
    CheckSpec("plausibleAfterBirth", TRIGGER_YES),
    CheckSpec("plausibleBeforeDeath", TRIGGER_YES),
    CheckSpec("withinVisitDates", TRIGGER_YES),
)

TABLE_CHECK_SPECS: Tuple[CheckSpec, ...] = (
    CheckSpec("cdmTable", TRIGGER_ALWAYS),
    CheckSpec("measurePersonCompleteness", TRIGGER_YES),
    CheckSpec("measureConditionEraCompleteness", TRIGGER_YES),
    CheckSpec("measureObservationPeriodOverlap", TRIGGER_YES),
)

# Concept_Level.csv carries one row per concept, with cdmTableName,
# cdmFieldName and conceptId identifying what the row is about, and
# one column per concept-level check. Each check instance is scoped
# to a specific concept, so conceptId is threaded through as a param
# alongside the triggering cell value.
CONCEPT_CHECK_SPECS: Tuple[CheckSpec, ...] = (
    CheckSpec("plausibleGender", TRIGGER_VALUE, ("conceptId",)),
    CheckSpec("plausibleGenderUseDescendants", TRIGGER_VALUE, ("conceptId",)),
    # The evaluationFilter for this check is
    # `plausibleUnitConceptIdsThreshold!=''`: the gate is the
    # threshold column, but the payload consumed by Task 9 is the
    # unit concept id list in plausibleUnitConceptIds itself.
    CheckSpec(
        "plausibleUnitConceptIds",
        TRIGGER_VALUE,
        param_columns=("conceptId",),
        trigger_column="plausibleUnitConceptIdsThreshold",
        value_column="plausibleUnitConceptIds",
    ),
)


@dataclass(frozen=True)
class CheckDescription:
    check_name: str
    check_level: str
    severity: str
    kahn_category: str
    description: str


@dataclass(frozen=True)
class CheckInstance:
    check_name: str
    check_level: str
    cdm_table_name: str
    cdm_field_name: Optional[str]
    threshold: float
    severity: str
    kahn_category: str
    description: str
    param_items: Tuple[Tuple[str, str], ...] = field(default=())

    @property
    def params(self) -> Dict[str, str]:
        return dict(self.param_items)

    @property
    def qualified_field(self) -> str:
        if self.cdm_field_name:
            return f"{self.cdm_table_name}.{self.cdm_field_name}"
        return self.cdm_table_name


def _assert_supported(cdm_version: str) -> None:
    if cdm_version not in SUPPORTED_CDM_VERSIONS:
        raise ValueError(
            f"Unsupported CDM version {cdm_version!r}. "
            f"Supported: {', '.join(SUPPORTED_CDM_VERSIONS)}"
        )


def _read_csv(cdm_version: str, kind: str) -> List[Dict[str, str]]:
    path = os.path.join(VENDOR_CSV_DIR, f"OMOP_CDMv{cdm_version}_{kind}.csv")
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_check_descriptions(
    cdm_version: str,
) -> Dict[str, CheckDescription]:
    _assert_supported(cdm_version)
    descriptions = {}
    for row in _read_csv(cdm_version, "Check_Descriptions"):
        descriptions[row["checkName"]] = CheckDescription(
            check_name=row["checkName"],
            check_level=row["checkLevel"],
            severity=row["severity"],
            kahn_category=row.get("kahnCategory", ""),
            description=row.get("checkDescription", ""),
        )
    return descriptions


def _parse_threshold(row: Dict[str, str], check_name: str) -> float:
    raw = (row.get(f"{check_name}Threshold") or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _collect_params(
    row: Dict[str, str], spec: CheckSpec, value_cell: str
) -> Tuple[Tuple[str, str], ...]:
    params = {}
    if spec.trigger == TRIGGER_VALUE:
        params["value"] = value_cell
    for column in spec.param_columns:
        value = (row.get(column) or "").strip()
        if value:
            params[column] = value
    return tuple(sorted(params.items()))


def _requirements_met(
    row: Dict[str, str], requires: Tuple[Tuple[str, str], ...]
) -> bool:
    for column, expected in requires:
        actual = (row.get(column) or "").strip()
        if actual.lower() != expected.lower():
            return False
    return True


def _instantiate(
    rows: List[Dict[str, str]],
    specs: Tuple[CheckSpec, ...],
    descriptions: Dict[str, CheckDescription],
    field_column: Optional[str],
) -> List[CheckInstance]:
    instances = []
    for row in rows:
        table_name = (row.get("cdmTableName") or "").strip()
        if not table_name:
            continue
        field_name = None
        if field_column:
            field_name = (row.get(field_column) or "").strip() or None
        for spec in specs:
            if not _requirements_met(row, spec.requires):
                continue
            gate_column = spec.trigger_column or spec.name
            gate_cell = (row.get(gate_column) or "").strip()
            if spec.trigger == TRIGGER_YES and gate_cell.lower() != "yes":
                continue
            if spec.trigger == TRIGGER_VALUE:
                if not gate_cell:
                    continue
                if (
                    spec.trigger_equals is not None
                    and gate_cell.lower() != spec.trigger_equals.lower()
                ):
                    continue
            description = descriptions.get(spec.name)
            if description is None:
                continue
            value_column = spec.value_column or spec.name
            value_cell = (row.get(value_column) or "").strip()
            instances.append(
                CheckInstance(
                    check_name=spec.name,
                    check_level=description.check_level,
                    cdm_table_name=table_name.upper(),
                    cdm_field_name=(
                        field_name.lower() if field_name else None
                    ),
                    threshold=_parse_threshold(row, spec.name),
                    severity=description.severity,
                    kahn_category=description.kahn_category,
                    description=description.description,
                    param_items=_collect_params(row, spec, value_cell),
                )
            )
    return instances


def load_catalog(cdm_version: str) -> List[CheckInstance]:
    """Instantiate every applicable check for a CDM version."""
    _assert_supported(cdm_version)
    descriptions = load_check_descriptions(cdm_version)
    instances: List[CheckInstance] = []
    instances.extend(
        _instantiate(
            _read_csv(cdm_version, "Table_Level"),
            TABLE_CHECK_SPECS,
            descriptions,
            field_column=None,
        )
    )
    instances.extend(
        _instantiate(
            _read_csv(cdm_version, "Field_Level"),
            FIELD_CHECK_SPECS,
            descriptions,
            field_column="cdmFieldName",
        )
    )
    instances.extend(
        _instantiate(
            _read_csv(cdm_version, "Concept_Level"),
            CONCEPT_CHECK_SPECS,
            descriptions,
            field_column="cdmFieldName",
        )
    )
    return instances
