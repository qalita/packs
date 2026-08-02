# omop_cdm_pack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a QALITA pack that evaluates an OMOP CDM instance against the 27 data quality check types of the OHDSI DataQualityDashboard, executed entirely in Polars.

**Architecture:** The OHDSI check metadata CSVs are vendored verbatim and drive check instantiation (~550 CSV rows → 2 535 check instances for CDM 5.4). The 30 OHDSI SQL Server templates are reimplemented as 27 Polars functions sharing one signature. A runner groups check instances by CDM table so each table is scanned once, then results are aggregated into QALITA metrics and recommendations.

**Tech Stack:** Python 3.10/3.11, Polars (lazy + streaming), pyarrow, `qalita_core`, pytest.

## Global Constraints

- Python: `requires-python = ">=3.10,<3.12"` (matches every other pack).
- Dependencies: `qalita-core>=1.5.0`, `pyarrow>=23.0.1`, `polars>=1.0.0`.
- Pack directory: `omop_cdm_pack/` at repo root. Pack `name` is `omop_cdm`, initial `version` is `0.1.0`.
- Formatter: Black, line length 79 (repo default — pre-commit enforces it). Linters: Pylint, Flake8, Bandit run via pre-commit on commit.
- Commits: English, conventional commits (`feat:`, `fix:`, `chore:`, `docs:`, `test:`).
- Git tags use bare semver `X.Y.Z` — **never** a `v` prefix. Do not tag in this plan.
- Licensing is dual-regime. `omop_dqd/vendor/` content and every module in `omop_dqd/checks/` derive from Apache-2.0 OHDSI material and must carry the attribution header defined in Task 1. Everything else is proprietary QALITA. Never modify a vendored CSV.
- Never load a CDM table eagerly. Every table access goes through `pl.scan_parquet` and every result is produced by `.collect(engine="streaming")`.
- A failing check must never abort a run. Any exception inside a check function becomes `CheckStatus.ERROR` for that instance only.

---

### Task 1: Pack scaffolding and vendored OHDSI metadata

**Files:**
- Create: `omop_cdm_pack/pyproject.toml`
- Create: `omop_cdm_pack/properties.yaml`
- Create: `omop_cdm_pack/README.md`
- Create: `omop_cdm_pack/NOTICE`
- Create: `omop_cdm_pack/LICENSE` (copied from a sibling pack)
- Create: `omop_cdm_pack/run.sh` (copied from a sibling pack)
- Create: `omop_cdm_pack/icon.png` (placeholder copied from a sibling pack)
- Create: `omop_cdm_pack/omop_dqd/__init__.py`
- Create: `omop_cdm_pack/omop_dqd/vendor/README.md`
- Create: `omop_cdm_pack/omop_dqd/vendor/LICENSE-APACHE-2.0.txt`
- Create: `omop_cdm_pack/omop_dqd/vendor/csv/*.csv` (8 files, downloaded)
- Test: `omop_cdm_pack/tests/test_vendor_metadata.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the package `omop_dqd` importable from the pack root; vendored CSVs at `omop_dqd/vendor/csv/`; the constant `VENDOR_CSV_DIR`.

- [ ] **Step 1: Create the pack directory and copy boilerplate from a sibling pack**

```bash
cd /home/aleopold/qalita/packs
mkdir -p omop_cdm_pack/omop_dqd/vendor/csv omop_cdm_pack/omop_dqd/checks omop_cdm_pack/tests
cp referential_integrity_pack/run.sh omop_cdm_pack/run.sh
cp referential_integrity_pack/LICENSE omop_cdm_pack/LICENSE
cp fhir_compliance_pack/icon.png omop_cdm_pack/icon.png
chmod +x omop_cdm_pack/run.sh
```

`icon.png` is a placeholder — note it in the PR description so design can replace it.

- [ ] **Step 2: Download the 8 OHDSI metadata CSVs**

These are the vendored artifacts. Never edit them by hand.

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack/omop_dqd/vendor/csv
BASE=https://raw.githubusercontent.com/OHDSI/DataQualityDashboard/main/inst/csv
for v in 5.3 5.4; do
  for f in Check_Descriptions Table_Level Field_Level Concept_Level; do
    curl -fsSL "$BASE/OMOP_CDMv${v}_${f}.csv" -o "OMOP_CDMv${v}_${f}.csv"
  done
done
curl -fsSL https://raw.githubusercontent.com/OHDSI/DataQualityDashboard/main/LICENSE \
  -o ../LICENSE-APACHE-2.0.txt
ls -la
```

Expected: 8 CSV files present and non-empty, plus `../LICENSE-APACHE-2.0.txt`.

- [ ] **Step 3: Record the exact upstream version vendored**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack/omop_dqd/vendor
git ls-remote https://github.com/OHDSI/DataQualityDashboard.git HEAD
```

Write `omop_cdm_pack/omop_dqd/vendor/README.md` with the returned commit SHA substituted for `<SHA>` and today's date substituted for `<DATE>`:

```markdown
# Vendored OHDSI DataQualityDashboard metadata

Source: https://github.com/OHDSI/DataQualityDashboard
Commit: <SHA>
Retrieved: <DATE>
License: Apache License 2.0 (see `../LICENSE-APACHE-2.0.txt`)

`csv/` contains the check metadata files copied **verbatim** from `inst/csv/` upstream.
They are never modified. To refresh them, re-run the download in Task 1 Step 2 of
`docs/superpowers/plans/2026-07-31-omop-cdm-pack.md` and update the commit SHA above.
```

- [ ] **Step 4: Write the NOTICE file**

Create `omop_cdm_pack/NOTICE`:

```
QALITA omop_cdm_pack

This product includes software and data developed by the
Observational Health Data Sciences and Informatics (OHDSI) program.

  DataQualityDashboard
  Copyright 2019 Observational Health Data Sciences and Informatics
  https://github.com/OHDSI/DataQualityDashboard
  Licensed under the Apache License, Version 2.0

The following parts of this pack are derived from that work and remain
subject to the Apache License, Version 2.0:

  - omop_dqd/vendor/csv/*.csv
        Check metadata copied verbatim from inst/csv/ upstream.
  - omop_dqd/checks/*.py
        Reimplementations, in Polars, of the SQL templates in
        inst/sql/sql_server/ upstream. Each module names the SQL
        template it derives from.

A copy of the Apache License 2.0 is provided in
omop_dqd/vendor/LICENSE-APACHE-2.0.txt

All other files in this pack are proprietary to QALITA SAS and are
governed by the QALITA SOFTWARE LICENSE AGREEMENT in LICENSE.
```

- [ ] **Step 5: Write `pyproject.toml`**

```toml
[project]
name = "omop-cdm-pack"
version = "0.1.0"
description = "Evaluates an OMOP CDM instance against the OHDSI Data Quality Dashboard check suite"
authors = [
    {name = "QALITA SAS", email = "contact@qalita.io"}
]
license = {text = "Proprietary"}
readme = "README.md"
requires-python = ">=3.10,<3.12"
dependencies = [
    "qalita-core>=1.5.0",
    "pyarrow>=23.0.1",
    "polars>=1.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 6: Write `properties.yaml`**

```yaml
compatible_sources:
- mysql
- postgresql
- sqlite
- oracle
- file
- folder
description: Evaluates an OMOP CDM instance against the OHDSI Data Quality Dashboard
  check suite (conformance, completeness, plausibility)
icon: icon.png
name: omop_cdm
tags:
- Healthcare
- OMOP
- CDM
- OHDSI
- Interoperability
type: conformity
version: 0.1.0
visibility: private
url: https://github.com/qalita/packs/tree/main/omop_cdm_pack
```

`visibility` is deliberately `private` until the licensing review in the spec §8 is signed off. Do not change it in this plan.

- [ ] **Step 7: Write the attribution header used by every checks module**

Create `omop_cdm_pack/omop_dqd/__init__.py`:

```python
"""OMOP CDM data quality checks, ported from the OHDSI DataQualityDashboard.

See the NOTICE file at the pack root for attribution and licensing.
"""

import os

VENDOR_CSV_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "vendor", "csv"
)

SUPPORTED_CDM_VERSIONS = ("5.3", "5.4")
```

Every module created later under `omop_dqd/checks/` must begin with this exact
docstring form, naming its upstream SQL template(s):

```python
"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars: inst/sql/sql_server/<template>.sql
See the NOTICE file at the pack root.
"""
```

- [ ] **Step 8: Write the failing test**

Create `omop_cdm_pack/tests/test_vendor_metadata.py`:

```python
import csv
import os

from omop_dqd import SUPPORTED_CDM_VERSIONS, VENDOR_CSV_DIR

EXPECTED_CHECK_TYPE_COUNT = 27


def _read(version, kind):
    path = os.path.join(VENDOR_CSV_DIR, f"OMOP_CDMv{version}_{kind}.csv")
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def test_all_vendored_csvs_exist_for_every_supported_version():
    for version in SUPPORTED_CDM_VERSIONS:
        for kind in (
            "Check_Descriptions",
            "Table_Level",
            "Field_Level",
            "Concept_Level",
        ):
            rows = _read(version, kind)
            assert rows, f"OMOP_CDMv{version}_{kind}.csv is empty"


def test_check_descriptions_declare_27_check_types():
    rows = _read("5.4", "Check_Descriptions")
    assert len(rows) == EXPECTED_CHECK_TYPE_COUNT


def test_check_descriptions_expose_the_columns_the_catalog_needs():
    rows = _read("5.4", "Check_Descriptions")
    for column in (
        "checkLevel",
        "checkName",
        "checkDescription",
        "kahnCategory",
        "severity",
    ):
        assert column in rows[0], f"missing column {column}"


def test_field_level_has_the_key_columns_used_for_instantiation():
    rows = _read("5.4", "Field_Level")
    for column in (
        "cdmTableName",
        "cdmFieldName",
        "isRequired",
        "isRequiredThreshold",
        "fkTableName",
        "fkFieldName",
        "standardConceptFieldName",
    ):
        assert column in rows[0], f"missing column {column}"


def test_severities_are_within_the_known_vocabulary():
    rows = _read("5.4", "Check_Descriptions")
    assert {r["severity"] for r in rows} <= {
        "fatal",
        "convention",
        "characterization",
    }
```

- [ ] **Step 9: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_vendor_metadata.py -v
```

Expected: 5 passed. If `test_check_descriptions_declare_27_check_types` fails, upstream changed its check list — do not edit the CSV. Update `EXPECTED_CHECK_TYPE_COUNT` and reconcile `FIELD_CHECK_SPECS` in Task 2 with the new row set.

- [ ] **Step 10: Write the README**

Create `omop_cdm_pack/README.md`:

```markdown
## OMOP CDM Pack

Evaluates an OMOP Common Data Model instance against the check suite of the
[OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard):
27 check types instantiated over the CDM specification into 2 535 checks for CDM 5.4 (2 005 for 5.3),
grouped by the three Kahn framework categories (conformance, completeness, plausibility).

Everything runs in Polars, lazily and in streaming mode. No R, no JVM, no SQL pushdown.

### Source configuration

Point the source at a schema holding OMOP CDM tables. The pack reads the CDM
specification to decide which tables to look for; tables absent from the source are
reported as failures of the `cdmTable` check rather than crashing the run.

The OMOP vocabulary tables (`CONCEPT`, `CONCEPT_ANCESTOR`) are **optional**. When they
are absent, the seven check types that need them are reported as `Not Applicable`.

### Metrics

| Key | Scope | Meaning |
|---|---|---|
| `score` | dataset | Share of passing checks, weighted by severity |
| `conformance_score` / `completeness_score` / `plausibility_score` | dataset | Per Kahn category |
| `score` | table | Share of passing checks for that CDM table |
| `pct_violated_rows` | column | Emitted only for failing checks |

### Attribution

Check metadata and check logic derive from the OHDSI DataQualityDashboard, licensed
under Apache 2.0. See `NOTICE`. All other files are proprietary to QALITA SAS.
```

- [ ] **Step 11: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack
git commit -m "feat(omop_cdm_pack): scaffold pack and vendor OHDSI check metadata"
```

---

### Task 2: The check catalog

Turns the vendored CSVs into check instances. This is the mechanical heart of the port: it reproduces DQD's instantiation logic, where a single CSV row spawns one check instance per applicable check column.

**Files:**
- Create: `omop_cdm_pack/omop_dqd/catalog.py`
- Test: `omop_cdm_pack/tests/test_catalog.py`

**Interfaces:**
- Consumes: `VENDOR_CSV_DIR`, `SUPPORTED_CDM_VERSIONS` from Task 1.
- Produces:
  - `CheckInstance` frozen dataclass with fields `check_name: str`, `check_level: str`, `cdm_table_name: str`, `cdm_field_name: Optional[str]`, `threshold: float`, `severity: str`, `kahn_category: str`, `description: str`, `params: Dict[str, str]`.
  - `load_catalog(cdm_version: str) -> List[CheckInstance]`
  - `CheckDescription` frozen dataclass with `check_name`, `check_level`, `severity`, `kahn_category`, `description`.
  - `load_check_descriptions(cdm_version: str) -> Dict[str, CheckDescription]`

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_catalog.py`:

```python
import pytest

from omop_dqd.catalog import (
    CheckInstance,
    load_catalog,
    load_check_descriptions,
)


def test_descriptions_are_keyed_by_check_name():
    descriptions = load_check_descriptions("5.4")
    assert descriptions["isRequired"].severity == "fatal"
    assert descriptions["isRequired"].check_level == "FIELD"
    assert (
        descriptions["measureValueCompleteness"].severity
        == "characterization"
    )


def test_catalog_instantiates_thousands_of_checks():
    catalog = load_catalog("5.4")
    assert len(catalog) > 3000


def test_every_instance_carries_a_known_severity():
    for check in load_catalog("5.4"):
        assert check.severity in {
            "fatal",
            "convention",
            "characterization",
        }


def test_cdm_field_check_is_instantiated_for_every_field_row():
    catalog = load_catalog("5.4")
    cdm_field_checks = [c for c in catalog if c.check_name == "cdmField"]
    person_fields = [
        c for c in cdm_field_checks if c.cdm_table_name == "PERSON"
    ]
    assert len(person_fields) > 5
    assert all(c.threshold == 0.0 for c in cdm_field_checks)


def test_is_required_is_instantiated_only_when_the_cell_says_yes():
    catalog = load_catalog("5.4")
    required = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isRequired"
    }
    assert ("PERSON", "person_id") in required
    assert ("PERSON", "month_of_birth") not in required


def test_foreign_key_checks_carry_the_referenced_table_and_field():
    catalog = load_catalog("5.4")
    fks = [
        c
        for c in catalog
        if c.check_name == "isForeignKey"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
        and c.cdm_field_name == "condition_concept_id"
    ]
    assert len(fks) == 1
    assert fks[0].params["fkTableName"] == "CONCEPT"
    assert fks[0].params["fkFieldName"] == "CONCEPT_ID"


def test_value_triggered_checks_capture_the_cell_as_a_param():
    catalog = load_catalog("5.4")
    domains = [
        c
        for c in catalog
        if c.check_name == "fkDomain"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
        and c.cdm_field_name == "condition_concept_id"
    ]
    assert len(domains) == 1
    assert domains[0].params["value"] == "Condition"


def test_thresholds_are_parsed_as_floats_defaulting_to_zero():
    catalog = load_catalog("5.4")
    completeness = [
        c
        for c in catalog
        if c.check_name == "standardConceptRecordCompleteness"
        and c.cdm_table_name == "CONDITION_OCCURRENCE"
    ]
    assert completeness
    assert all(isinstance(c.threshold, float) for c in completeness)


def test_table_and_concept_level_checks_are_present():
    catalog = load_catalog("5.4")
    levels = {c.check_level for c in catalog}
    assert levels == {"TABLE", "FIELD", "CONCEPT"}


def test_instances_are_hashable_so_they_can_be_deduplicated():
    catalog = load_catalog("5.4")
    assert isinstance(catalog[0], CheckInstance)
    assert len(set(catalog)) == len(catalog)


def test_unsupported_cdm_version_is_rejected():
    with pytest.raises(ValueError, match="5.2"):
        load_catalog("5.2")


def test_both_supported_versions_load():
    assert load_catalog("5.3")
    assert load_catalog("5.4")
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_catalog.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'omop_dqd.catalog'`.

- [ ] **Step 3: Write the implementation**

Create `omop_cdm_pack/omop_dqd/catalog.py`.

`CheckInstance.params` is a `Dict[str, str]` but the dataclass is frozen and must stay
hashable, so params are stored as a sorted tuple internally and exposed as a dict via a
property.

```python
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
    """How one check column of a metadata CSV instantiates checks."""

    name: str
    trigger: str
    param_columns: Tuple[str, ...] = ()


# Field-level checks, in the order they appear in Check_Descriptions.csv.
FIELD_CHECK_SPECS: Tuple[CheckSpec, ...] = (
    CheckSpec("cdmField", TRIGGER_ALWAYS),
    CheckSpec("isRequired", TRIGGER_YES),
    CheckSpec("cdmDatatype", TRIGGER_VALUE),
    CheckSpec("isPrimaryKey", TRIGGER_YES),
    CheckSpec(
        "isForeignKey", TRIGGER_YES, ("fkTableName", "fkFieldName")
    ),
    CheckSpec("fkDomain", TRIGGER_VALUE),
    CheckSpec("fkClass", TRIGGER_VALUE),
    CheckSpec("isStandardValidConcept", TRIGGER_YES),
    CheckSpec("measureValueCompleteness", TRIGGER_YES),
    CheckSpec("standardConceptRecordCompleteness", TRIGGER_YES),
    CheckSpec(
        "sourceConceptRecordCompleteness",
        TRIGGER_YES,
        ("standardConceptFieldName",),
    ),
    CheckSpec("sourceValueCompleteness", TRIGGER_YES),
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

CONCEPT_CHECK_SPECS: Tuple[CheckSpec, ...] = (
    CheckSpec("plausibleGender", TRIGGER_VALUE),
    CheckSpec("plausibleGenderUseDescendants", TRIGGER_VALUE),
    CheckSpec("plausibleUnitConceptIds", TRIGGER_VALUE),
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
    path = os.path.join(
        VENDOR_CSV_DIR, f"OMOP_CDMv{cdm_version}_{kind}.csv"
    )
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
    row: Dict[str, str], spec: CheckSpec, cell: str
) -> Tuple[Tuple[str, str], ...]:
    params = {}
    if spec.trigger == TRIGGER_VALUE:
        params["value"] = cell
    for column in spec.param_columns:
        value = (row.get(column) or "").strip()
        if value:
            params[column] = value
    return tuple(sorted(params.items()))


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
            cell = (row.get(spec.name) or "").strip()
            if spec.trigger == TRIGGER_YES and cell.lower() != "yes":
                continue
            if spec.trigger == TRIGGER_VALUE and not cell:
                continue
            description = descriptions.get(spec.name)
            if description is None:
                continue
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
                    param_items=_collect_params(row, spec, cell),
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
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_catalog.py -v
```

Expected: 12 passed.

The `Concept_Level.csv` shape differs from the other two — it carries one row per
concept with columns named after the concept-level checks. If
`test_table_and_concept_level_checks_are_present` fails because no CONCEPT instances were
produced, inspect the real header with
`head -1 omop_dqd/vendor/csv/OMOP_CDMv5.4_Concept_Level.csv` and adjust
`CONCEPT_CHECK_SPECS` and the `field_column` argument to match the actual column names.
Do not change the CSV.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/catalog.py omop_cdm_pack/tests/test_catalog.py
git commit -m "feat(omop_cdm_pack): instantiate checks from vendored CDM metadata"
```

---

### Task 3: CDM context and test fixtures

Resolves CDM table names to Polars LazyFrames, detects which tables and vocabulary are
available, and provides the synthetic mini-CDM that every later task tests against.

**Files:**
- Create: `omop_cdm_pack/omop_dqd/context.py`
- Create: `omop_cdm_pack/tests/__init__.py` (empty — makes `tests.fixtures` importable)
- Create: `omop_cdm_pack/tests/fixtures.py`
- Create: `omop_cdm_pack/tests/conftest.py`
- Test: `omop_cdm_pack/tests/test_context.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `CdmContext` with `table(name: str) -> pl.LazyFrame`, `has_table(name: str) -> bool`, `has_column(table: str, column: str) -> bool`, `columns(name: str) -> List[str]`, `dtypes(name: str) -> Dict[str, pl.DataType]`, `has_vocabulary: bool`, `available_tables: Set[str]`.
  - `CdmContext.from_paths(table_paths: Dict[str, List[str]]) -> CdmContext`
  - `VOCABULARY_TABLES: frozenset` = `{"CONCEPT", "CONCEPT_ANCESTOR"}`
  - pytest fixtures `mini_cdm` and `mini_cdm_no_vocabulary`, both returning a `CdmContext`.

- [ ] **Step 0: Make the tests directory a package**

`conftest.py` imports `tests.fixtures`, which only resolves if `tests/` is a package.

```bash
touch /home/aleopold/qalita/packs/omop_cdm_pack/tests/__init__.py
```

- [ ] **Step 1: Write the fixture builder**

Create `omop_cdm_pack/tests/fixtures.py`. Row values are chosen so each check type has a
known, hand-countable number of violations.

```python
"""Synthetic mini OMOP CDM used across the test suite."""

import os

import polars as pl

# PERSON: 4 people. person 4 has a NULL gender_concept_id.
PERSON = pl.DataFrame(
    {
        "person_id": [1, 2, 3, 4],
        "gender_concept_id": [8507, 8532, 8507, None],
        "year_of_birth": [1980, 1990, 2000, 1970],
        "month_of_birth": [1, 6, 12, None],
        "day_of_birth": [15, 1, 31, None],
        "birth_datetime": [
            "1980-01-15",
            "1990-06-01",
            "2000-12-31",
            "1970-01-01",
        ],
        "race_concept_id": [8527, 8527, 8516, 8527],
        "ethnicity_concept_id": [38003564] * 4,
    },
    schema_overrides={
        "person_id": pl.Int64,
        "gender_concept_id": pl.Int64,
        "birth_datetime": pl.Utf8,
    },
).with_columns(pl.col("birth_datetime").str.to_date())

# DEATH: person 3 died in 2020.
DEATH = pl.DataFrame(
    {
        "person_id": [3],
        "death_date": ["2020-01-01"],
        "death_type_concept_id": [32817],
    }
).with_columns(pl.col("death_date").str.to_date())

# VISIT_OCCURRENCE: 3 visits.
VISIT_OCCURRENCE = pl.DataFrame(
    {
        "visit_occurrence_id": [10, 11, 12],
        "person_id": [1, 2, 3],
        "visit_concept_id": [9201, 9202, 9201],
        "visit_start_date": ["2015-01-01", "2016-01-01", "2017-01-01"],
        "visit_end_date": ["2015-01-05", "2016-01-05", "2017-01-05"],
    }
).with_columns(
    pl.col("visit_start_date").str.to_date(),
    pl.col("visit_end_date").str.to_date(),
)

# CONDITION_OCCURRENCE: 6 rows, with deliberate violations.
#   row 1 (id 100) clean
#   row 2 (id 101) start after end               -> plausibleStartBeforeEnd
#   row 3 (id 102) date before birth             -> plausibleAfterBirth
#   row 4 (id 103) date after death (person 3)   -> plausibleBeforeDeath
#   row 5 (id 104) NULL condition_concept_id     -> isRequired, completeness
#   row 6 (id 104) duplicate id                  -> isPrimaryKey
#                  concept 99999 unknown         -> isForeignKey
#
# Knock-on effect, intended: rows 2 and 3 both sit on visit 10, whose
# window is 2015-01-01..2015-01-05, but their dates (2015-06-01 and
# 1970-01-01) fall outside it. So withinVisitDates finds 2 violations,
# not 1 — a consequence of the start-after-end and before-birth plants
# above, not a separate mistake.
CONDITION_OCCURRENCE = pl.DataFrame(
    {
        "condition_occurrence_id": [100, 101, 102, 103, 104, 104],
        "person_id": [1, 1, 1, 3, 2, 2],
        "condition_concept_id": [
            201826,
            201826,
            201826,
            201826,
            None,
            99999,
        ],
        "condition_start_date": [
            "2015-01-02",
            "2015-06-01",
            "1970-01-01",
            "2021-01-01",
            "2016-01-02",
            "2016-01-03",
        ],
        "condition_end_date": [
            "2015-01-03",
            "2015-05-01",
            "1970-01-02",
            "2021-01-02",
            "2016-01-03",
            "2016-01-04",
        ],
        "condition_source_value": ["A", "B", "C", "D", None, "F"],
        "visit_occurrence_id": [10, 10, 10, 12, 11, 11],
    },
    schema_overrides={"condition_concept_id": pl.Int64},
).with_columns(
    pl.col("condition_start_date").str.to_date(),
    pl.col("condition_end_date").str.to_date(),
)

# CONCEPT: minimal vocabulary.
#
# Every concept id referenced anywhere else in this fixture is present
# here EXCEPT 99999, which is the single deliberate foreign-key
# violation (CONDITION_OCCURRENCE row 6). That includes the race,
# ethnicity and death-type concepts, which exist purely so PERSON and
# DEATH are foreign-key clean — without them, an isForeignKey check
# would find 9 violations nobody planted, and every exact count
# downstream would be wrong.
#
# 4181412 is present but deprecated (standard_concept NULL,
# invalid_reason "D") so isStandardValidConcept has something to catch.
CONCEPT = pl.DataFrame(
    {
        "concept_id": [
            201826,
            8507,
            8532,
            9201,
            9202,
            4181412,
            8527,
            8516,
            38003564,
            32817,
        ],
        "concept_name": [
            "Type 2 diabetes",
            "MALE",
            "FEMALE",
            "Inpatient visit",
            "Outpatient visit",
            "Deprecated concept",
            "White",
            "Black or African American",
            "Not Hispanic or Latino",
            "EHR",
        ],
        "domain_id": [
            "Condition",
            "Gender",
            "Gender",
            "Visit",
            "Visit",
            "Condition",
            "Race",
            "Race",
            "Ethnicity",
            "Type Concept",
        ],
        "concept_class_id": [
            "Clinical Finding",
            "Gender",
            "Gender",
            "Visit",
            "Visit",
            "Clinical Finding",
            "Race",
            "Race",
            "Ethnicity",
            "Type Concept",
        ],
        "standard_concept": [
            "S",
            "S",
            "S",
            "S",
            "S",
            None,
            "S",
            "S",
            "S",
            "S",
        ],
        "invalid_reason": [
            None,
            None,
            None,
            None,
            None,
            "D",
            None,
            None,
            None,
            None,
        ],
    },
    schema_overrides={
        "standard_concept": pl.Utf8,
        "invalid_reason": pl.Utf8,
    },
)

CONCEPT_ANCESTOR = pl.DataFrame(
    {
        "ancestor_concept_id": [201826],
        "descendant_concept_id": [201826],
        "min_levels_of_separation": [0],
        "max_levels_of_separation": [0],
    }
)

TABLES = {
    "PERSON": PERSON,
    "DEATH": DEATH,
    "VISIT_OCCURRENCE": VISIT_OCCURRENCE,
    "CONDITION_OCCURRENCE": CONDITION_OCCURRENCE,
    "CONCEPT": CONCEPT,
    "CONCEPT_ANCESTOR": CONCEPT_ANCESTOR,
}

VOCABULARY_TABLE_NAMES = ("CONCEPT", "CONCEPT_ANCESTOR")


def write_mini_cdm(directory, include_vocabulary=True):
    """Write the mini CDM as parquet. Returns {TABLE_NAME: [paths]}."""
    os.makedirs(directory, exist_ok=True)
    table_paths = {}
    for name, frame in TABLES.items():
        if not include_vocabulary and name in VOCABULARY_TABLE_NAMES:
            continue
        path = os.path.join(directory, f"{name.lower()}_part_0.parquet")
        frame.write_parquet(path)
        table_paths[name] = [path]
    return table_paths
```

- [ ] **Step 2: Write conftest**

Create `omop_cdm_pack/tests/conftest.py`:

```python
import pytest

from omop_dqd.context import CdmContext
from tests.fixtures import write_mini_cdm


@pytest.fixture(scope="session")
def mini_cdm(tmp_path_factory):
    directory = tmp_path_factory.mktemp("mini_cdm")
    return CdmContext.from_paths(write_mini_cdm(str(directory)))


@pytest.fixture(scope="session")
def mini_cdm_no_vocabulary(tmp_path_factory):
    directory = tmp_path_factory.mktemp("mini_cdm_no_vocab")
    return CdmContext.from_paths(
        write_mini_cdm(str(directory), include_vocabulary=False)
    )
```

- [ ] **Step 3: Write the failing test**

Create `omop_cdm_pack/tests/test_context.py`:

```python
import polars as pl
import pytest


def test_known_tables_are_available(mini_cdm):
    assert mini_cdm.has_table("PERSON")
    assert mini_cdm.has_table("CONDITION_OCCURRENCE")


def test_table_lookup_is_case_insensitive(mini_cdm):
    assert mini_cdm.has_table("person")
    assert mini_cdm.has_table("Person")


def test_absent_table_is_reported_missing(mini_cdm):
    assert not mini_cdm.has_table("DRUG_EXPOSURE")


def test_table_returns_a_lazyframe(mini_cdm):
    frame = mini_cdm.table("PERSON")
    assert isinstance(frame, pl.LazyFrame)
    assert frame.select(pl.len()).collect().item() == 4


def test_requesting_a_missing_table_raises(mini_cdm):
    with pytest.raises(KeyError, match="DRUG_EXPOSURE"):
        mini_cdm.table("DRUG_EXPOSURE")


def test_columns_are_lowercased(mini_cdm):
    assert "person_id" in mini_cdm.columns("PERSON")
    assert "year_of_birth" in mini_cdm.columns("PERSON")


def test_dtypes_are_exposed_without_reading_rows(mini_cdm):
    dtypes = mini_cdm.dtypes("PERSON")
    assert dtypes["person_id"] == pl.Int64


def test_vocabulary_is_detected_when_present(mini_cdm):
    assert mini_cdm.has_vocabulary


def test_vocabulary_absence_is_detected(mini_cdm_no_vocabulary):
    assert not mini_cdm_no_vocabulary.has_vocabulary
    assert not mini_cdm_no_vocabulary.has_table("CONCEPT")


def test_available_tables_excludes_missing_ones(mini_cdm):
    assert "PERSON" in mini_cdm.available_tables
    assert "DRUG_EXPOSURE" not in mini_cdm.available_tables


def test_has_column_is_case_insensitive_on_both_arguments(mini_cdm):
    assert mini_cdm.has_column("person", "PERSON_ID")
    assert not mini_cdm.has_column("PERSON", "no_such_column")


def test_has_column_is_false_for_a_missing_table(mini_cdm):
    assert not mini_cdm.has_column("DRUG_EXPOSURE", "person_id")
```

- [ ] **Step 4: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_context.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'omop_dqd.context'`.

- [ ] **Step 5: Write the implementation**

Create `omop_cdm_pack/omop_dqd/context.py`:

```python
"""Resolution of CDM table names to Polars LazyFrames."""

from typing import Dict, List, Set

import polars as pl

VOCABULARY_TABLES = frozenset({"CONCEPT", "CONCEPT_ANCESTOR"})


class CdmContext:
    """Lazy access to the CDM tables materialised as parquet.

    Table names are normalised to upper case, column names to lower case,
    so callers never have to worry about the casing a given source used.
    """

    def __init__(self, table_paths: Dict[str, List[str]]):
        self._paths = {
            name.upper(): paths for name, paths in table_paths.items()
        }
        self._schema_cache: Dict[str, Dict[str, pl.DataType]] = {}

    @classmethod
    def from_paths(
        cls, table_paths: Dict[str, List[str]]
    ) -> "CdmContext":
        return cls(table_paths)

    @property
    def available_tables(self) -> Set[str]:
        return set(self._paths)

    @property
    def has_vocabulary(self) -> bool:
        return VOCABULARY_TABLES.issubset(self.available_tables)

    def has_table(self, name: str) -> bool:
        return name.upper() in self._paths

    def table(self, name: str) -> pl.LazyFrame:
        key = name.upper()
        if key not in self._paths:
            raise KeyError(f"CDM table {key} is not available")
        frame = pl.scan_parquet(self._paths[key])
        return frame.rename(
            {c: c.lower() for c in frame.collect_schema().names()}
        )

    def _schema(self, name: str) -> Dict[str, pl.DataType]:
        key = name.upper()
        if key not in self._schema_cache:
            schema = self.table(key).collect_schema()
            self._schema_cache[key] = dict(schema)
        return self._schema_cache[key]

    def columns(self, name: str) -> List[str]:
        return list(self._schema(name))

    def dtypes(self, name: str) -> Dict[str, pl.DataType]:
        return dict(self._schema(name))

    def has_column(self, table: str, column: str) -> bool:
        if not self.has_table(table):
            return False
        return column.lower() in self._schema(table)
```

- [ ] **Step 6: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_context.py -v
```

Expected: 12 passed.

- [ ] **Step 7: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/context.py omop_cdm_pack/tests/
git commit -m "feat(omop_cdm_pack): add CDM context and synthetic test fixtures"
```

---

### Task 4: Result types, registry and threshold evaluation

**Files:**
- Create: `omop_cdm_pack/omop_dqd/results.py`
- Create: `omop_cdm_pack/omop_dqd/registry.py`
- Create: `omop_cdm_pack/omop_dqd/evaluate.py`
- Create: `omop_cdm_pack/omop_dqd/checks/__init__.py`
- Test: `omop_cdm_pack/tests/test_evaluate.py`

**Interfaces:**
- Consumes: `CheckInstance` (Task 2), `CdmContext` (Task 3).
- Produces:
  - `CheckStatus` string constants: `PASS`, `FAIL`, `NOT_APPLICABLE`, `ERROR`.
  - `CheckResult` dataclass with `num_violated_rows: int`, `num_denominator_rows: int`, `status: str`, `message: str`, and property `pct_violated_rows: float`.
  - `not_applicable(reason: str) -> CheckResult`, `counted(violated: int, denominator: int) -> CheckResult`
  - `register(check_name)` decorator, `get_check(check_name)`, `is_registered(check_name)`.
  - `evaluate(instance: CheckInstance, result: CheckResult) -> CheckResult` — applies the DQD threshold rule and returns a result whose `status` is resolved.
  - `EvaluatedCheck` dataclass pairing a `CheckInstance` with its `CheckResult`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_evaluate.py`:

```python
import pytest

from omop_dqd.catalog import CheckInstance
from omop_dqd.evaluate import evaluate
from omop_dqd.registry import get_check, is_registered, register
from omop_dqd.results import CheckStatus, counted, not_applicable


def _instance(threshold=0.0, check_name="isRequired"):
    return CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name="PERSON",
        cdm_field_name="person_id",
        threshold=threshold,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
    )


def test_pct_violated_rows_is_derived():
    assert counted(1, 4).pct_violated_rows == 25.0


def test_pct_violated_rows_is_zero_when_denominator_is_zero():
    assert counted(0, 0).pct_violated_rows == 0.0


def test_zero_threshold_fails_on_any_violation():
    result = evaluate(_instance(threshold=0.0), counted(1, 100))
    assert result.status == CheckStatus.FAIL


def test_zero_threshold_passes_with_no_violation():
    result = evaluate(_instance(threshold=0.0), counted(0, 100))
    assert result.status == CheckStatus.PASS


def test_non_zero_threshold_tolerates_violations_below_it():
    # 4 violations out of 100 is 4%, threshold is 5%
    result = evaluate(_instance(threshold=5.0), counted(4, 100))
    assert result.status == CheckStatus.PASS


def test_non_zero_threshold_fails_strictly_above_it():
    # 6 violations out of 100 is 6%, threshold is 5%
    result = evaluate(_instance(threshold=5.0), counted(6, 100))
    assert result.status == CheckStatus.FAIL


def test_threshold_boundary_is_inclusive():
    # exactly at the threshold passes, matching DQD
    result = evaluate(_instance(threshold=5.0), counted(5, 100))
    assert result.status == CheckStatus.PASS


def test_empty_denominator_is_not_applicable():
    result = evaluate(_instance(threshold=0.0), counted(0, 0))
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_not_applicable_survives_evaluation():
    result = evaluate(_instance(), not_applicable("no vocabulary"))
    assert result.status == CheckStatus.NOT_APPLICABLE
    assert "vocabulary" in result.message


def test_registry_round_trip():
    @register("dummyCheckForTest")
    def _dummy(ctx, chk):
        return counted(0, 1)

    assert is_registered("dummyCheckForTest")
    assert get_check("dummyCheckForTest") is _dummy


def test_unregistered_check_raises():
    with pytest.raises(KeyError, match="noSuchCheck"):
        get_check("noSuchCheck")


def test_duplicate_registration_is_rejected():
    @register("duplicateCheckForTest")
    def _first(ctx, chk):
        return counted(0, 1)

    with pytest.raises(ValueError, match="duplicateCheckForTest"):

        @register("duplicateCheckForTest")
        def _second(ctx, chk):
            return counted(0, 1)
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_evaluate.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'omop_dqd.results'`.

- [ ] **Step 3: Write `results.py`**

```python
"""Outcome types shared by every check."""

from dataclasses import dataclass


class CheckStatus:
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    ERROR = "ERROR"


@dataclass(frozen=True)
class CheckResult:
    num_violated_rows: int = 0
    num_denominator_rows: int = 0
    status: str = CheckStatus.PASS
    message: str = ""

    @property
    def pct_violated_rows(self) -> float:
        if self.num_denominator_rows == 0:
            return 0.0
        return (
            100.0 * self.num_violated_rows / self.num_denominator_rows
        )


def counted(violated: int, denominator: int) -> CheckResult:
    """A measured result whose status is resolved later by evaluate()."""
    return CheckResult(
        num_violated_rows=int(violated),
        num_denominator_rows=int(denominator),
    )


def not_applicable(reason: str) -> CheckResult:
    return CheckResult(
        status=CheckStatus.NOT_APPLICABLE, message=reason
    )


def errored(reason: str) -> CheckResult:
    return CheckResult(status=CheckStatus.ERROR, message=reason)
```

- [ ] **Step 4: Write `registry.py`**

```python
"""Mapping from OHDSI check names to their Polars implementation."""

from typing import Callable, Dict

_REGISTRY: Dict[str, Callable] = {}


def register(check_name: str) -> Callable:
    def decorator(function: Callable) -> Callable:
        if check_name in _REGISTRY:
            raise ValueError(
                f"Check {check_name!r} is already registered"
            )
        _REGISTRY[check_name] = function
        return function

    return decorator


def get_check(check_name: str) -> Callable:
    if check_name not in _REGISTRY:
        raise KeyError(f"No implementation for check {check_name!r}")
    return _REGISTRY[check_name]


def is_registered(check_name: str) -> bool:
    return check_name in _REGISTRY


def registered_names():
    return sorted(_REGISTRY)
```

- [ ] **Step 5: Write `evaluate.py`**

The threshold rule mirrors DQD: with no threshold, any violation fails; with a
threshold, only a violation percentage strictly above it fails.

```python
"""Threshold application, mirroring the DQD pass/fail rule."""

from dataclasses import dataclass, replace

from omop_dqd.catalog import CheckInstance
from omop_dqd.results import CheckResult, CheckStatus


@dataclass(frozen=True)
class EvaluatedCheck:
    instance: CheckInstance
    result: CheckResult


def evaluate(
    instance: CheckInstance, result: CheckResult
) -> CheckResult:
    """Resolve a measured result into PASS or FAIL."""
    if result.status in (
        CheckStatus.NOT_APPLICABLE,
        CheckStatus.ERROR,
    ):
        return result

    if result.num_denominator_rows == 0:
        return replace(
            result,
            status=CheckStatus.NOT_APPLICABLE,
            message="no rows to evaluate",
        )

    if instance.threshold <= 0:
        failed = result.num_violated_rows > 0
    else:
        failed = result.pct_violated_rows > instance.threshold

    return replace(
        result,
        status=CheckStatus.FAIL if failed else CheckStatus.PASS,
    )
```

- [ ] **Step 6: Write the checks package init**

Create `omop_cdm_pack/omop_dqd/checks/__init__.py`. Importing the package must register
every check, so later tasks add their module to this import list.

```python
"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars the SQL templates of inst/sql/sql_server/.
See the NOTICE file at the pack root.

Importing this package registers every check implementation.
"""

from omop_dqd.checks import (  # noqa: F401
    concept_level,
    field_level,
    table_level,
)
```

Create the three modules as empty stubs for now so the import resolves; each gets its
attribution docstring:

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack/omop_dqd/checks
for m in field_level table_level concept_level; do
  printf '"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.\n\nSee the NOTICE file at the pack root.\n"""\n' > "$m.py"
done
```

- [ ] **Step 7: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_evaluate.py -v
```

Expected: 12 passed.

- [ ] **Step 8: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd omop_cdm_pack/tests/test_evaluate.py
git commit -m "feat(omop_cdm_pack): add result types, check registry and threshold rule"
```

---

### Task 5: Field-level checks — schema and single-column family

Nine check types that need at most one column of one table.

**Files:**
- Modify: `omop_cdm_pack/omop_dqd/checks/field_level.py`
- Test: `omop_cdm_pack/tests/test_checks_field_simple.py`

**Interfaces:**
- Consumes: `register` (Task 4), `CdmContext` (Task 3), `CheckInstance` (Task 2), `counted`/`not_applicable` (Task 4).
- Produces: registered implementations for `cdmField`, `isRequired`, `cdmDatatype`, `isPrimaryKey`, `measureValueCompleteness`, `sourceValueCompleteness`, `plausibleValueLow`, `plausibleValueHigh`, `plausibleStartBeforeEnd`. Also the helper `guard(ctx, chk)` returning `Optional[CheckResult]`, reused by Tasks 6 and 7.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_checks_field_simple.py`:

```python
import omop_dqd.checks  # noqa: F401  (registers implementations)
from omop_dqd.catalog import CheckInstance
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


def test_cdm_field_passes_for_an_existing_column(mini_cdm):
    result = _run(mini_cdm, "cdmField", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_cdm_field_flags_a_missing_column(mini_cdm):
    result = _run(mini_cdm, "cdmField", "PERSON", "no_such_column")
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 1


def test_cdm_field_is_not_applicable_when_the_table_is_missing(mini_cdm):
    result = _run(mini_cdm, "cdmField", "DRUG_EXPOSURE", "anything")
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_is_required_counts_nulls(mini_cdm):
    # condition_concept_id has 1 NULL out of 6 rows
    result = _run(
        mini_cdm,
        "isRequired",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_is_required_passes_on_a_fully_populated_column(mini_cdm):
    result = _run(mini_cdm, "isRequired", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_measure_value_completeness_counts_nulls(mini_cdm):
    result = _run(
        mini_cdm,
        "measureValueCompleteness",
        "PERSON",
        "gender_concept_id",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 4


def test_source_value_completeness_treats_null_as_incomplete(mini_cdm):
    result = _run(
        mini_cdm,
        "sourceValueCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_source_value",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_is_primary_key_detects_the_duplicate(mini_cdm):
    # condition_occurrence_id 104 appears twice among 6 rows
    result = _run(
        mini_cdm,
        "isPrimaryKey",
        "CONDITION_OCCURRENCE",
        "condition_occurrence_id",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_is_primary_key_passes_on_a_unique_column(mini_cdm):
    result = _run(mini_cdm, "isPrimaryKey", "PERSON", "person_id")
    assert result.num_violated_rows == 0


def test_plausible_value_low_counts_values_below_the_bound(mini_cdm):
    # years of birth are 1980, 1990, 2000, 1970 -> one below 1975
    result = _run(
        mini_cdm,
        "plausibleValueLow",
        "PERSON",
        "year_of_birth",
        value="1975",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 4


def test_plausible_value_high_counts_values_above_the_bound(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleValueHigh",
        "PERSON",
        "year_of_birth",
        value="1995",
    )
    assert result.num_violated_rows == 1


def test_plausible_start_before_end_detects_inverted_dates(mini_cdm):
    # condition_occurrence_id 101 starts 2015-06-01, ends 2015-05-01
    result = _run(
        mini_cdm,
        "plausibleStartBeforeEnd",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleStartBeforeEndFieldName="condition_end_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_cdm_datatype_passes_for_a_matching_integer(mini_cdm):
    result = _run(
        mini_cdm, "cdmDatatype", "PERSON", "person_id", value="integer"
    )
    assert result.num_violated_rows == 0


def test_cdm_datatype_flags_a_mismatch(mini_cdm):
    result = _run(
        mini_cdm,
        "cdmDatatype",
        "PERSON",
        "person_id",
        value="varchar(50)",
    )
    assert result.num_violated_rows == 1
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_field_simple.py -v
```

Expected: every test errors with `KeyError: "No implementation for check 'cdmField'"`.

- [ ] **Step 3: Write the implementation**

Replace `omop_cdm_pack/omop_dqd/checks/field_level.py`:

```python
"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/field_cdm_field.sql
  inst/sql/sql_server/field_is_not_nullable.sql
  inst/sql/sql_server/field_cdm_datatype.sql
  inst/sql/sql_server/field_is_primary_key.sql
  inst/sql/sql_server/field_measure_value_completeness.sql
  inst/sql/sql_server/field_source_value_completeness.sql
  inst/sql/sql_server/field_plausible_value_low.sql
  inst/sql/sql_server/field_plausible_value_high.sql
  inst/sql/sql_server/field_plausible_start_before_end.sql
See the NOTICE file at the pack root.
"""

from typing import Optional

import polars as pl

from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable

# OMOP datatype names mapped onto the Polars dtype groups that satisfy
# them. A source is free to widen a type (int32 where int64 is
# specified) so membership, not equality, is what matters.
_DATATYPE_GROUPS = {
    "integer": pl.INTEGER_DTYPES,
    "bigint": pl.INTEGER_DTYPES,
    "float": pl.FLOAT_DTYPES,
    "date": frozenset({pl.Date}),
    "datetime": frozenset({pl.Datetime}),
}


def guard(ctx, chk) -> Optional[CheckResult]:
    """Return a NOT_APPLICABLE result when the check cannot run.

    Used by every field-level check before touching data.
    """
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if chk.cdm_field_name and not ctx.has_column(
        chk.cdm_table_name, chk.cdm_field_name
    ):
        return not_applicable(
            f"column {chk.qualified_field} is absent from the source"
        )
    return None


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


def _count_where(
    ctx, chk, predicate: pl.Expr, denominator: Optional[pl.Expr] = None
) -> CheckResult:
    """Count rows matching predicate, over an optional denominator."""
    frame = ctx.table(chk.cdm_table_name)
    if denominator is not None:
        frame = frame.filter(denominator)
    total = _row_count(frame)
    violated = _row_count(frame.filter(predicate))
    return counted(violated, total)


@register("cdmField")
def cdm_field(ctx, chk) -> CheckResult:
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    present = ctx.has_column(chk.cdm_table_name, chk.cdm_field_name)
    return counted(0 if present else 1, 1)


@register("cdmDatatype")
def cdm_datatype(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    declared = chk.params.get("value", "").strip().lower()
    group = None
    for prefix, dtypes in _DATATYPE_GROUPS.items():
        if declared.startswith(prefix):
            group = dtypes
            break
    if group is None:
        # varchar and other free-text types: any dtype is acceptable,
        # matching DQD, which only checks numeric and date columns.
        return not_applicable(f"datatype {declared!r} is not checked")
    actual = ctx.dtypes(chk.cdm_table_name)[chk.cdm_field_name]
    return counted(0 if actual in group else 1, 1)


@register("isRequired")
def is_required(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    return _count_where(ctx, chk, pl.col(chk.cdm_field_name).is_null())


@register("measureValueCompleteness")
def measure_value_completeness(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    return _count_where(ctx, chk, pl.col(chk.cdm_field_name).is_null())


@register("sourceValueCompleteness")
def source_value_completeness(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column.is_null() | (column.cast(pl.Utf8) == "")
    )


@register("isPrimaryKey")
def is_primary_key(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    frame = ctx.table(chk.cdm_table_name)
    counts = (
        frame.select(
            pl.len().alias("total"),
            pl.col(chk.cdm_field_name).n_unique().alias("distinct"),
        )
        .collect(engine="streaming")
        .row(0)
    )
    total, distinct = counts
    return counted(total - distinct, total)


def _bound(chk) -> Optional[float]:
    raw = chk.params.get("value", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


@register("plausibleValueLow")
def plausible_value_low(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    bound = _bound(chk)
    if bound is None:
        return not_applicable("non-numeric plausible bound")
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column < bound, denominator=column.is_not_null()
    )


@register("plausibleValueHigh")
def plausible_value_high(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    bound = _bound(chk)
    if bound is None:
        return not_applicable("non-numeric plausible bound")
    column = pl.col(chk.cdm_field_name)
    return _count_where(
        ctx, chk, column > bound, denominator=column.is_not_null()
    )


@register("plausibleStartBeforeEnd")
def plausible_start_before_end(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    end_field = chk.params.get(
        "plausibleStartBeforeEndFieldName", ""
    ).lower()
    if not end_field or not ctx.has_column(
        chk.cdm_table_name, end_field
    ):
        return not_applicable(f"end field {end_field!r} unavailable")
    start = pl.col(chk.cdm_field_name)
    end = pl.col(end_field)
    return _count_where(
        ctx,
        chk,
        start > end,
        denominator=start.is_not_null() & end.is_not_null(),
    )
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_field_simple.py -v
```

Expected: 14 passed.

If `pl.INTEGER_DTYPES` raises `AttributeError` on the installed Polars version, replace
the `_DATATYPE_GROUPS` values with explicit sets:
`frozenset({pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64})`
for integers and `frozenset({pl.Float32, pl.Float64})` for floats.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/checks/field_level.py \
        omop_cdm_pack/tests/test_checks_field_simple.py
git commit -m "feat(omop_cdm_pack): implement single-column field-level checks"
```

---

### Task 6: Field-level checks — join family

Six check types that join a second CDM table.

**Files:**
- Modify: `omop_cdm_pack/omop_dqd/checks/field_level.py`
- Test: `omop_cdm_pack/tests/test_checks_field_joins.py`

**Interfaces:**
- Consumes: `guard`, `_row_count` (Task 5).
- Produces: registered implementations for `isForeignKey`, `plausibleAfterBirth`, `plausibleBeforeDeath`, `plausibleDuringLife`, `plausibleTemporalAfter`, `withinVisitDates`. Also `person_birth_date(ctx) -> pl.LazyFrame` with columns `person_id`, `birth_date`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_checks_field_joins.py`:

```python
import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
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


def test_foreign_key_detects_the_orphan_concept(mini_cdm):
    # condition_concept_id 99999 is absent from CONCEPT;
    # the NULL row is excluded from the denominator, so 5 rows
    result = _run(
        mini_cdm,
        "isForeignKey",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        fkTableName="CONCEPT",
        fkFieldName="CONCEPT_ID",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 5


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
    # condition_occurrence_id 102 starts 1970-01-01, person 1 born 1980
    result = _run(
        mini_cdm,
        "plausibleAfterBirth",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 6


def test_plausible_before_death_detects_the_post_mortem_event(mini_cdm):
    # condition_occurrence_id 103 is 2021-01-01, person 3 died 2020-01-01
    result = _run(
        mini_cdm,
        "plausibleBeforeDeath",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1


def test_plausible_during_life_counts_both_ends(mini_cdm):
    # only the post-death event; pre-birth people have no death record
    result = _run(
        mini_cdm,
        "plausibleDuringLife",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows == 1


def test_within_visit_dates_passes_on_aligned_events(mini_cdm):
    # every condition sits inside its visit except the outliers
    result = _run(
        mini_cdm,
        "withinVisitDates",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
    )
    assert result.num_violated_rows >= 1
    assert result.num_denominator_rows == 6


def test_plausible_temporal_after_compares_against_another_table(
    mini_cdm,
):
    # condition_start_date must not precede the visit start date
    result = _run(
        mini_cdm,
        "plausibleTemporalAfter",
        "CONDITION_OCCURRENCE",
        "condition_start_date",
        plausibleTemporalAfterTableName="VISIT_OCCURRENCE",
        plausibleTemporalAfterFieldName="visit_start_date",
    )
    assert result.num_denominator_rows == 6
    assert result.num_violated_rows >= 1
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_field_joins.py -v
```

Expected: `KeyError: "No implementation for check 'isForeignKey'"`.

- [ ] **Step 3: Append the implementation**

Append to `omop_cdm_pack/omop_dqd/checks/field_level.py`. Also extend the module
docstring's template list with `is_foreign_key.sql`, `field_plausible_after_birth.sql`,
`field_plausible_before_death.sql`, `field_plausible_during_life.sql`,
`field_plausible_temporal_after.sql` and `field_within_visit_dates.sql`.

```python
def person_birth_date(ctx) -> pl.LazyFrame:
    """PERSON birth dates, falling back to year/month/day parts.

    Mirrors the COALESCE(birth_datetime, CONCAT(year, month, day))
    used by the upstream SQL, defaulting month and day to 1.
    """
    person = ctx.table("PERSON")
    columns = ctx.columns("PERSON")
    composed = pl.date(
        pl.col("year_of_birth").cast(pl.Int32),
        pl.col("month_of_birth").cast(pl.Int32).fill_null(1)
        if "month_of_birth" in columns
        else pl.lit(1, dtype=pl.Int32),
        pl.col("day_of_birth").cast(pl.Int32).fill_null(1)
        if "day_of_birth" in columns
        else pl.lit(1, dtype=pl.Int32),
    )
    if "birth_datetime" in columns:
        birth = (
            pl.col("birth_datetime").cast(pl.Date).fill_null(composed)
        )
    else:
        birth = composed
    return person.select(
        pl.col("person_id"), birth.alias("birth_date")
    )


def _join_violation_count(
    ctx, chk, other: pl.LazyFrame, on: str, predicate: pl.Expr
) -> CheckResult:
    """Count rows of the CDM table violating a cross-table predicate.

    The denominator is every row with a non-null checked field, matching
    the upstream SQL. Rows without a join partner cannot violate.
    """
    column = pl.col(chk.cdm_field_name)
    base = ctx.table(chk.cdm_table_name).filter(column.is_not_null())
    total = _row_count(base)
    violated = _row_count(
        base.join(other, on=on, how="inner").filter(predicate)
    )
    return counted(violated, total)


@register("isForeignKey")
def is_foreign_key(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    parent_table = chk.params.get("fkTableName", "").upper()
    parent_field = chk.params.get("fkFieldName", "").lower()
    if not ctx.has_table(parent_table):
        return not_applicable(
            f"referenced table {parent_table} is absent"
        )
    if not ctx.has_column(parent_table, parent_field):
        return not_applicable(
            f"referenced column {parent_table}.{parent_field} is absent"
        )
    column = pl.col(chk.cdm_field_name)
    child = ctx.table(chk.cdm_table_name).filter(column.is_not_null())
    total = _row_count(child)
    parent_keys = (
        ctx.table(parent_table)
        .select(pl.col(parent_field).alias(chk.cdm_field_name))
        .drop_nulls()
        .unique()
    )
    orphans = child.join(
        parent_keys, on=chk.cdm_field_name, how="anti"
    )
    return counted(_row_count(orphans), total)


@register("plausibleAfterBirth")
def plausible_after_birth(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")
    return _join_violation_count(
        ctx,
        chk,
        person_birth_date(ctx),
        on="person_id",
        predicate=pl.col(chk.cdm_field_name).cast(pl.Date)
        < pl.col("birth_date"),
    )


@register("plausibleBeforeDeath")
def plausible_before_death(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("DEATH"):
        return not_applicable("DEATH is absent from the source")
    death = (
        ctx.table("DEATH")
        .select(
            pl.col("person_id"),
            pl.col("death_date").cast(pl.Date).alias("death_date"),
        )
        .drop_nulls()
    )
    return _join_violation_count(
        ctx,
        chk,
        death,
        on="person_id",
        predicate=pl.col(chk.cdm_field_name).cast(pl.Date)
        > pl.col("death_date"),
    )


@register("plausibleDuringLife")
def plausible_during_life(ctx, chk) -> CheckResult:
    """Upstream only flags events after death for this check."""
    return plausible_before_death(ctx, chk)


@register("plausibleTemporalAfter")
def plausible_temporal_after(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    other_table = chk.params.get(
        "plausibleTemporalAfterTableName", ""
    ).upper()
    other_field = chk.params.get(
        "plausibleTemporalAfterFieldName", ""
    ).lower()
    if not other_table or not ctx.has_table(other_table):
        return not_applicable(f"table {other_table} is absent")
    if not ctx.has_column(other_table, other_field):
        return not_applicable(
            f"column {other_table}.{other_field} is absent"
        )

    if other_table == chk.cdm_table_name:
        column = pl.col(chk.cdm_field_name)
        return _count_where(
            ctx,
            chk,
            column.cast(pl.Date) < pl.col(other_field).cast(pl.Date),
            denominator=column.is_not_null()
            & pl.col(other_field).is_not_null(),
        )

    join_key = "person_id"
    if not ctx.has_column(other_table, join_key):
        return not_applicable(
            f"cannot join {other_table} on {join_key}"
        )
    other = (
        ctx.table(other_table)
        .select(
            pl.col(join_key),
            pl.col(other_field).cast(pl.Date).alias("_other_date"),
        )
        .drop_nulls()
    )
    return _join_violation_count(
        ctx,
        chk,
        other,
        on=join_key,
        predicate=pl.col(chk.cdm_field_name).cast(pl.Date)
        < pl.col("_other_date"),
    )


@register("withinVisitDates")
def within_visit_dates(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("VISIT_OCCURRENCE"):
        return not_applicable("VISIT_OCCURRENCE is absent")
    if not ctx.has_column(chk.cdm_table_name, "visit_occurrence_id"):
        return not_applicable(
            f"{chk.cdm_table_name} has no visit_occurrence_id"
        )
    visits = ctx.table("VISIT_OCCURRENCE").select(
        pl.col("visit_occurrence_id"),
        pl.col("visit_start_date").cast(pl.Date).alias("_visit_start"),
        pl.col("visit_end_date").cast(pl.Date).alias("_visit_end"),
    )
    event = pl.col(chk.cdm_field_name).cast(pl.Date)
    return _join_violation_count(
        ctx,
        chk,
        visits,
        on="visit_occurrence_id",
        predicate=(event < pl.col("_visit_start"))
        | (event > pl.col("_visit_end")),
    )
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_field_joins.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/checks/field_level.py \
        omop_cdm_pack/tests/test_checks_field_joins.py
git commit -m "feat(omop_cdm_pack): implement join-based field-level checks"
```

---

### Task 7: Field-level checks — vocabulary family

Five check types requiring `CONCEPT`. Each must degrade to `NOT_APPLICABLE` when the
vocabulary is absent.

**Files:**
- Modify: `omop_cdm_pack/omop_dqd/checks/field_level.py`
- Test: `omop_cdm_pack/tests/test_checks_vocabulary.py`

**Interfaces:**
- Consumes: `guard`, `_row_count`, `_count_where` (Tasks 5–6).
- Produces: registered implementations for `fkDomain`, `fkClass`, `isStandardValidConcept`, `standardConceptRecordCompleteness`, `sourceConceptRecordCompleteness`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_checks_vocabulary.py`:

```python
import pytest

import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus

VOCABULARY_CHECKS = (
    "fkDomain",
    "fkClass",
    "isStandardValidConcept",
    "standardConceptRecordCompleteness",
    "sourceConceptRecordCompleteness",
)


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="convention",
        kahn_category="Conformance",
        description="d",
        param_items=tuple(sorted(params.items())),
    )
    return get_check(check_name)(ctx, instance)


@pytest.mark.parametrize("check_name", VOCABULARY_CHECKS)
def test_all_vocabulary_checks_degrade_gracefully(
    mini_cdm_no_vocabulary, check_name
):
    result = _run(
        mini_cdm_no_vocabulary,
        check_name,
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_fk_domain_flags_the_unknown_concept(mini_cdm):
    # 5 non-null concept ids; 99999 is not in CONCEPT so its domain
    # cannot be Condition
    result = _run(
        mini_cdm,
        "fkDomain",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        value="Condition",
    )
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 1


def test_fk_domain_passes_when_every_concept_matches(mini_cdm):
    result = _run(
        mini_cdm,
        "fkDomain",
        "VISIT_OCCURRENCE",
        "visit_concept_id",
        value="Visit",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 3


def test_fk_class_checks_the_concept_class(mini_cdm):
    result = _run(
        mini_cdm,
        "fkClass",
        "VISIT_OCCURRENCE",
        "visit_concept_id",
        value="Visit",
    )
    assert result.num_violated_rows == 0


def test_is_standard_valid_concept_flags_non_standard(mini_cdm):
    # 99999 is absent from CONCEPT entirely -> violation
    result = _run(
        mini_cdm,
        "isStandardValidConcept",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 5
    assert result.num_violated_rows == 1


def test_standard_concept_record_completeness_counts_zeros(mini_cdm):
    # NULL concept ids count as incomplete, as do 0 values
    result = _run(
        mini_cdm,
        "standardConceptRecordCompleteness",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
    )
    assert result.num_denominator_rows == 6
    assert result.num_violated_rows == 1
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_vocabulary.py -v
```

Expected: `KeyError: "No implementation for check 'fkDomain'"`.

- [ ] **Step 3: Append the implementation**

Append to `omop_cdm_pack/omop_dqd/checks/field_level.py`, extending the module docstring
with `field_fk_domain.sql`, `field_fk_class.sql`,
`field_is_standard_valid_concept.sql` and `field_concept_record_completeness.sql`.

```python
def _concept_attribute(ctx, attribute: str) -> pl.LazyFrame:
    """CONCEPT reduced to concept_id plus one attribute column."""
    return ctx.table("CONCEPT").select(
        pl.col("concept_id"),
        pl.col(attribute).alias("_attribute"),
    )


def _concept_join_violation(
    ctx, chk, other: pl.LazyFrame, predicate: pl.Expr
) -> CheckResult:
    """Count concept-id rows violating a vocabulary predicate.

    Rows whose concept is absent from CONCEPT are violations, so the
    join is a left join and a null attribute counts against the check.
    """
    column = pl.col(chk.cdm_field_name)
    base = ctx.table(chk.cdm_table_name).filter(column.is_not_null())
    total = _row_count(base)
    joined = base.join(
        other,
        left_on=chk.cdm_field_name,
        right_on="concept_id",
        how="left",
    )
    return counted(_row_count(joined.filter(predicate)), total)


def _vocabulary_guard(ctx, chk):
    skip = guard(ctx, chk)
    if skip:
        return skip
    if not ctx.has_table("CONCEPT"):
        return not_applicable(
            "the OMOP vocabulary (CONCEPT) is absent from the source"
        )
    return None


@register("fkDomain")
def fk_domain(ctx, chk) -> CheckResult:
    skip = _vocabulary_guard(ctx, chk)
    if skip:
        return skip
    expected = chk.params.get("value", "")
    return _concept_join_violation(
        ctx,
        chk,
        _concept_attribute(ctx, "domain_id"),
        predicate=pl.col("_attribute").is_null()
        | (pl.col("_attribute") != expected),
    )


@register("fkClass")
def fk_class(ctx, chk) -> CheckResult:
    skip = _vocabulary_guard(ctx, chk)
    if skip:
        return skip
    expected = chk.params.get("value", "")
    return _concept_join_violation(
        ctx,
        chk,
        _concept_attribute(ctx, "concept_class_id"),
        predicate=pl.col("_attribute").is_null()
        | (pl.col("_attribute") != expected),
    )


@register("isStandardValidConcept")
def is_standard_valid_concept(ctx, chk) -> CheckResult:
    skip = _vocabulary_guard(ctx, chk)
    if skip:
        return skip
    concepts = ctx.table("CONCEPT").select(
        pl.col("concept_id"),
        pl.col("standard_concept").alias("_standard"),
        pl.col("invalid_reason").alias("_invalid"),
    )
    column = pl.col(chk.cdm_field_name)
    base = ctx.table(chk.cdm_table_name).filter(column.is_not_null())
    total = _row_count(base)
    joined = base.join(
        concepts,
        left_on=chk.cdm_field_name,
        right_on="concept_id",
        how="left",
    )
    violating = joined.filter(
        (pl.col("_standard").is_null())
        | (pl.col("_standard") != "S")
        | (pl.col("_invalid").is_not_null())
    )
    return counted(_row_count(violating), total)


@register("standardConceptRecordCompleteness")
def standard_concept_record_completeness(ctx, chk) -> CheckResult:
    """Share of records with no mapped standard concept.

    Upstream treats both NULL and the sentinel 0 as unmapped.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    column = pl.col(chk.cdm_field_name)
    return _count_where(ctx, chk, column.is_null() | (column == 0))


@register("sourceConceptRecordCompleteness")
def source_concept_record_completeness(ctx, chk) -> CheckResult:
    skip = guard(ctx, chk)
    if skip:
        return skip
    column = pl.col(chk.cdm_field_name)
    return _count_where(ctx, chk, column.is_null() | (column == 0))
```

`standardConceptRecordCompleteness` and `sourceConceptRecordCompleteness` do not read
`CONCEPT` — they measure mapping completeness on the CDM table itself, matching the
shared upstream template `field_concept_record_completeness.sql`. They therefore use
`guard` and not `_vocabulary_guard`, and the parametrised degradation test covers them
because the guard still trips on a missing table or column.

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_vocabulary.py -v
```

Expected: 10 passed (5 parametrised + 5 explicit).

If the two completeness checks fail the parametrised degradation test — because the
mini CDM without vocabulary still has `CONDITION_OCCURRENCE.condition_concept_id` and
so the guard does not trip — remove those two names from `VOCABULARY_CHECKS` in the test
and add a dedicated test asserting they still measure correctly without a vocabulary.
That is the correct behaviour, not a bug.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/checks/field_level.py \
        omop_cdm_pack/tests/test_checks_vocabulary.py
git commit -m "feat(omop_cdm_pack): implement vocabulary-dependent field checks"
```

---

### Task 8: Table-level checks

**Files:**
- Modify: `omop_cdm_pack/omop_dqd/checks/table_level.py`
- Test: `omop_cdm_pack/tests/test_checks_table.py`

**Interfaces:**
- Consumes: `CdmContext`, `register`, `counted`, `not_applicable`.
- Produces: registered implementations for `cdmTable`, `measurePersonCompleteness`, `measureConditionEraCompleteness`, `measureObservationPeriodOverlap`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_checks_table.py`:

```python
import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
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
    import polars as pl

    from omop_dqd.context import CdmContext

    # person 1 has two overlapping periods, person 2 has two disjoint
    periods = pl.DataFrame(
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
    ).with_columns(
        pl.col("observation_period_start_date").str.to_date(),
        pl.col("observation_period_end_date").str.to_date(),
    )
    path = tmp_path / "observation_period_part_0.parquet"
    periods.write_parquet(path)
    ctx = CdmContext.from_paths({"OBSERVATION_PERIOD": [str(path)]})

    result = _run(
        ctx, "measureObservationPeriodOverlap", "OBSERVATION_PERIOD"
    )
    assert result.num_violated_rows == 1
    assert result.num_denominator_rows == 2


def test_condition_era_completeness_is_not_applicable_when_absent(
    mini_cdm,
):
    result = _run(
        mini_cdm, "measureConditionEraCompleteness", "CONDITION_ERA"
    )
    assert result.status == CheckStatus.NOT_APPLICABLE
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_table.py -v
```

Expected: `KeyError: "No implementation for check 'cdmTable'"`.

- [ ] **Step 3: Write the implementation**

Replace `omop_cdm_pack/omop_dqd/checks/table_level.py`:

```python
"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/table_cdm_table.sql
  inst/sql/sql_server/table_person_completeness.sql
  inst/sql/sql_server/table_condition_era_completeness.sql
  inst/sql/sql_server/table_observation_period_overlap.sql
See the NOTICE file at the pack root.
"""

import polars as pl

from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


@register("cdmTable")
def cdm_table(ctx, chk) -> CheckResult:
    present = ctx.has_table(chk.cdm_table_name)
    return counted(0 if present else 1, 1)


@register("measurePersonCompleteness")
def measure_person_completeness(ctx, chk) -> CheckResult:
    """People with no record in the checked table."""
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, "person_id"):
        return not_applicable(
            f"{chk.cdm_table_name} has no person_id column"
        )
    people = ctx.table("PERSON").select("person_id")
    total = _row_count(people)
    referenced = (
        ctx.table(chk.cdm_table_name)
        .select("person_id")
        .drop_nulls()
        .unique()
    )
    missing = people.join(referenced, on="person_id", how="anti")
    return counted(_row_count(missing), total)


@register("measureConditionEraCompleteness")
def measure_condition_era_completeness(ctx, chk) -> CheckResult:
    """People with conditions but no condition era."""
    if not ctx.has_table("CONDITION_ERA"):
        return not_applicable("CONDITION_ERA is absent")
    if not ctx.has_table("CONDITION_OCCURRENCE"):
        return not_applicable("CONDITION_OCCURRENCE is absent")
    occurrences = (
        ctx.table("CONDITION_OCCURRENCE")
        .select("person_id")
        .drop_nulls()
        .unique()
    )
    total = _row_count(occurrences)
    eras = (
        ctx.table("CONDITION_ERA")
        .select("person_id")
        .drop_nulls()
        .unique()
    )
    missing = occurrences.join(eras, on="person_id", how="anti")
    return counted(_row_count(missing), total)


@register("measureObservationPeriodOverlap")
def measure_observation_period_overlap(ctx, chk) -> CheckResult:
    """People whose observation periods overlap each other.

    Sorting by person and start date lets a single shift compare each
    period with its predecessor, replacing the upstream self-join.
    """
    table = "OBSERVATION_PERIOD"
    if not ctx.has_table(table):
        return not_applicable(f"{table} is absent from the source")
    periods = ctx.table(table).select(
        pl.col("person_id"),
        pl.col("observation_period_start_date")
        .cast(pl.Date)
        .alias("start_date"),
        pl.col("observation_period_end_date")
        .cast(pl.Date)
        .alias("end_date"),
    )
    total = _row_count(periods.select("person_id").unique())
    overlapping = (
        periods.sort(["person_id", "start_date"])
        .with_columns(
            pl.col("end_date")
            .shift(1)
            .over("person_id")
            .alias("previous_end")
        )
        .filter(
            pl.col("previous_end").is_not_null()
            & (pl.col("start_date") <= pl.col("previous_end"))
        )
        .select("person_id")
        .unique()
    )
    return counted(_row_count(overlapping), total)
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_table.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/checks/table_level.py \
        omop_cdm_pack/tests/test_checks_table.py
git commit -m "feat(omop_cdm_pack): implement table-level checks"
```

---

### Task 9: Concept-level checks

**Files:**
- Modify: `omop_cdm_pack/omop_dqd/checks/concept_level.py`
- Test: `omop_cdm_pack/tests/test_checks_concept.py`

**Interfaces:**
- Consumes: `CdmContext`, `register`, `counted`, `not_applicable`.
- Produces: registered implementations for `plausibleGender`, `plausibleGenderUseDescendants`, `plausibleUnitConceptIds`.

Concept-level checks are scoped to one concept id: the check instance carries the
concept in `params["conceptId"]` and the expected gender or unit list in
`params["value"]`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_checks_concept.py`:

```python
import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance
from omop_dqd.registry import get_check
from omop_dqd.results import CheckStatus


def _run(ctx, check_name, table, field, **params):
    instance = CheckInstance(
        check_name=check_name,
        check_level="CONCEPT",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="characterization",
        kahn_category="Plausibility",
        description="d",
        param_items=tuple(sorted(params.items())),
    )
    return get_check(check_name)(ctx, instance)


def test_plausible_gender_flags_the_wrong_gender(mini_cdm):
    # concept 201826 is recorded for persons 1 (MALE 8507) and
    # 3 (MALE 8507); requiring FEMALE makes every row a violation
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8532",
    )
    assert result.num_denominator_rows == 4
    assert result.num_violated_rows == 4


def test_plausible_gender_passes_for_the_right_gender(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8507",
    )
    assert result.num_violated_rows == 0


def test_plausible_gender_is_not_applicable_without_person(
    mini_cdm_no_vocabulary,
):
    result = _run(
        mini_cdm_no_vocabulary,
        "plausibleGenderUseDescendants",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8507",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE


def test_plausible_gender_with_descendants_needs_the_ancestor_table(
    mini_cdm,
):
    result = _run(
        mini_cdm,
        "plausibleGenderUseDescendants",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8507",
    )
    assert result.num_violated_rows == 0
    assert result.num_denominator_rows == 4


def test_unknown_concept_yields_an_empty_denominator(mini_cdm):
    result = _run(
        mini_cdm,
        "plausibleGender",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="123456789",
        value="8507",
    )
    assert result.num_denominator_rows == 0


def test_plausible_unit_concept_ids_is_not_applicable_without_the_column(
    mini_cdm,
):
    result = _run(
        mini_cdm,
        "plausibleUnitConceptIds",
        "CONDITION_OCCURRENCE",
        "condition_concept_id",
        conceptId="201826",
        value="8840",
    )
    assert result.status == CheckStatus.NOT_APPLICABLE
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_concept.py -v
```

Expected: `KeyError: "No implementation for check 'plausibleGender'"`.

- [ ] **Step 3: Write the implementation**

Replace `omop_cdm_pack/omop_dqd/checks/concept_level.py`:

```python
"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars:
  inst/sql/sql_server/concept_plausible_gender.sql
  inst/sql/sql_server/concept_plausible_gender_use_descendants.sql
  inst/sql/sql_server/concept_plausible_unit_concept_ids.sql
See the NOTICE file at the pack root.
"""

from typing import List, Optional

import polars as pl

from omop_dqd.registry import register
from omop_dqd.results import CheckResult, counted, not_applicable


def _row_count(frame: pl.LazyFrame) -> int:
    return frame.select(pl.len()).collect(engine="streaming").item()


def _int_list(raw: str) -> List[int]:
    values = []
    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if token:
            try:
                values.append(int(token))
            except ValueError:
                continue
    return values


def _concept_id(chk) -> Optional[int]:
    raw = chk.params.get("conceptId", "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _rows_for_concept(
    ctx, chk, concept_id: int, use_descendants: bool
) -> Optional[pl.LazyFrame]:
    """Rows of the CDM table whose concept is (a descendant of) the id."""
    frame = ctx.table(chk.cdm_table_name)
    column = pl.col(chk.cdm_field_name)
    if not use_descendants:
        return frame.filter(column == concept_id)
    if not ctx.has_table("CONCEPT_ANCESTOR"):
        return None
    descendants = (
        ctx.table("CONCEPT_ANCESTOR")
        .filter(pl.col("ancestor_concept_id") == concept_id)
        .select(
            pl.col("descendant_concept_id").alias(chk.cdm_field_name)
        )
        .unique()
    )
    return frame.join(descendants, on=chk.cdm_field_name, how="semi")


def _plausible_gender(ctx, chk, use_descendants: bool) -> CheckResult:
    if not ctx.has_table("PERSON"):
        return not_applicable("PERSON is absent from the source")
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, chk.cdm_field_name):
        return not_applicable(
            f"column {chk.qualified_field} is absent from the source"
        )
    concept_id = _concept_id(chk)
    if concept_id is None:
        return not_applicable("no concept id on the check instance")
    allowed = _int_list(chk.params.get("value", ""))
    if not allowed:
        return not_applicable("no plausible gender concept id")

    rows = _rows_for_concept(ctx, chk, concept_id, use_descendants)
    if rows is None:
        return not_applicable("CONCEPT_ANCESTOR is absent")

    people = ctx.table("PERSON").select(
        pl.col("person_id"),
        pl.col("gender_concept_id").alias("_gender"),
    )
    joined = rows.join(people, on="person_id", how="inner")
    total = _row_count(joined)
    violating = joined.filter(
        pl.col("_gender").is_null()
        | ~pl.col("_gender").is_in(allowed)
    )
    return counted(_row_count(violating), total)


@register("plausibleGender")
def plausible_gender(ctx, chk) -> CheckResult:
    return _plausible_gender(ctx, chk, use_descendants=False)


@register("plausibleGenderUseDescendants")
def plausible_gender_use_descendants(ctx, chk) -> CheckResult:
    return _plausible_gender(ctx, chk, use_descendants=True)


@register("plausibleUnitConceptIds")
def plausible_unit_concept_ids(ctx, chk) -> CheckResult:
    """Rows for a concept carrying an implausible unit."""
    if not ctx.has_table(chk.cdm_table_name):
        return not_applicable(
            f"table {chk.cdm_table_name} is absent from the source"
        )
    if not ctx.has_column(chk.cdm_table_name, "unit_concept_id"):
        return not_applicable(
            f"{chk.cdm_table_name} has no unit_concept_id column"
        )
    concept_id = _concept_id(chk)
    if concept_id is None:
        return not_applicable("no concept id on the check instance")
    allowed = _int_list(chk.params.get("value", ""))
    if not allowed:
        return not_applicable("no plausible unit concept ids")

    rows = ctx.table(chk.cdm_table_name).filter(
        pl.col(chk.cdm_field_name) == concept_id
    )
    total = _row_count(rows)
    violating = rows.filter(
        pl.col("unit_concept_id").is_not_null()
        & ~pl.col("unit_concept_id").is_in(allowed)
    )
    return counted(_row_count(violating), total)
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_checks_concept.py -v
```

Expected: 6 passed.

The concept-level param names (`conceptId`, `value`) must match what Task 2's
`CONCEPT_CHECK_SPECS` actually produces from `Concept_Level.csv`. Verify with:

```bash
python -c "from omop_dqd.catalog import load_catalog; \
print([c.params for c in load_catalog('5.4') if c.check_level=='CONCEPT'][:3])"
```

If the params differ, adjust `CONCEPT_CHECK_SPECS` in `catalog.py` to emit `conceptId`
and `value`, and rerun Task 2's tests.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/checks/concept_level.py \
        omop_cdm_pack/tests/test_checks_concept.py
git commit -m "feat(omop_cdm_pack): implement concept-level checks"
```

---

### Task 10: The runner

Executes a catalog against a context, grouping by table and never letting one failure
abort the run.

**Files:**
- Create: `omop_cdm_pack/omop_dqd/runner.py`
- Test: `omop_cdm_pack/tests/test_runner.py`

**Interfaces:**
- Consumes: `CheckInstance`, `CdmContext`, `get_check`/`is_registered`, `evaluate`, `EvaluatedCheck`, `errored`.
- Produces: `run_checks(ctx: CdmContext, catalog: List[CheckInstance]) -> List[EvaluatedCheck]`.

**Deliberate deviation from spec §5.** The spec proposed collecting every aggregate of a
table in a single pass via `pl.collect_all()`. This task groups by table but still
collects once per check. The reason: a single-pass design forces each check function to
return an unevaluated `pl.Expr` rather than a count, which makes the join-based and
anti-join checks (Task 6) impossible to express uniformly and destroys the one-to-one
correspondence with the upstream SQL that makes the port verifiable. Correctness and
auditability come first; grouping by table still gives Polars its scan cache. True
single-pass batching is listed under "Deferred to a follow-up" and should only be
attempted once the fidelity cross-validation has passed.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_runner.py`:

```python
import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import CheckInstance, load_catalog
from omop_dqd.registry import register
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks


def _instance(check_name, table="PERSON", field="person_id"):
    return CheckInstance(
        check_name=check_name,
        check_level="FIELD",
        cdm_table_name=table,
        cdm_field_name=field,
        threshold=0.0,
        severity="fatal",
        kahn_category="Conformance",
        description="d",
    )


def test_runner_evaluates_each_instance(mini_cdm):
    results = run_checks(
        mini_cdm,
        [_instance("isRequired"), _instance("cdmField")],
    )
    assert len(results) == 2
    assert all(
        r.result.status
        in {
            CheckStatus.PASS,
            CheckStatus.FAIL,
            CheckStatus.NOT_APPLICABLE,
        }
        for r in results
    )


def test_runner_reports_a_crashing_check_as_error(mini_cdm):
    @register("explodingCheckForTest")
    def _explode(ctx, chk):
        raise RuntimeError("boom")

    results = run_checks(
        mini_cdm,
        [_instance("explodingCheckForTest"), _instance("isRequired")],
    )
    statuses = {r.instance.check_name: r.result.status for r in results}
    assert statuses["explodingCheckForTest"] == CheckStatus.ERROR
    # the run continued
    assert statuses["isRequired"] == CheckStatus.PASS


def test_runner_marks_unimplemented_checks_as_error(mini_cdm):
    results = run_checks(mini_cdm, [_instance("notImplementedCheck")])
    assert results[0].result.status == CheckStatus.ERROR
    assert "no implementation" in results[0].result.message.lower()


def test_runner_handles_the_full_catalog(mini_cdm):
    catalog = load_catalog("5.4")
    results = run_checks(mini_cdm, catalog)
    assert len(results) == len(catalog)
    # nothing may be left unevaluated
    assert all(r.result.status for r in results)


def test_full_catalog_run_produces_no_errors(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    errors = [
        r for r in results if r.result.status == CheckStatus.ERROR
    ]
    assert not errors, [
        (r.instance.check_name, r.result.message) for r in errors[:10]
    ]
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_runner.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'omop_dqd.runner'`.

- [ ] **Step 3: Write the implementation**

Create `omop_cdm_pack/omop_dqd/runner.py`:

```python
"""Execution of a check catalog against a CDM context."""

import logging
from collections import defaultdict
from typing import Dict, List

from omop_dqd.catalog import CheckInstance
from omop_dqd.context import CdmContext
from omop_dqd.evaluate import EvaluatedCheck, evaluate
from omop_dqd.registry import get_check, is_registered
from omop_dqd.results import errored

logger = logging.getLogger(__name__)


def _group_by_table(
    catalog: List[CheckInstance],
) -> Dict[str, List[CheckInstance]]:
    """Group instances so each CDM table is handled together.

    Polars caches the parquet scan per table, so grouping keeps the
    working set small and the file handles few.
    """
    grouped = defaultdict(list)
    for instance in catalog:
        grouped[instance.cdm_table_name].append(instance)
    return grouped


def _run_one(ctx: CdmContext, instance: CheckInstance) -> EvaluatedCheck:
    if not is_registered(instance.check_name):
        return EvaluatedCheck(
            instance,
            errored(
                f"no implementation for check {instance.check_name!r}"
            ),
        )
    try:
        raw = get_check(instance.check_name)(ctx, instance)
    except Exception as exc:  # noqa: BLE001 - one check must not stop the run
        logger.warning(
            "check %s failed on %s: %s",
            instance.check_name,
            instance.qualified_field,
            exc,
        )
        return EvaluatedCheck(instance, errored(str(exc)))
    return EvaluatedCheck(instance, evaluate(instance, raw))


def run_checks(
    ctx: CdmContext, catalog: List[CheckInstance]
) -> List[EvaluatedCheck]:
    """Evaluate every instance, in table-grouped order."""
    results = []
    for table_name, instances in _group_by_table(catalog).items():
        logger.info(
            "running %d checks on %s", len(instances), table_name
        )
        for instance in instances:
            results.append(_run_one(ctx, instance))
    return results
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_runner.py -v
```

Expected: 5 passed.

`test_full_catalog_run_produces_no_errors` is the integration gate for Tasks 5–9: it
asserts that no check type crashes across the whole ~2 800-instance catalog. If it fails,
the assertion message names the offending check and its error — fix that check, do not
weaken the test.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/runner.py omop_cdm_pack/tests/test_runner.py
git commit -m "feat(omop_cdm_pack): add fault-tolerant check runner"
```

---

### Task 11: Metrics and recommendations aggregation

Turns ~2 800 evaluated checks into the handful of QALITA metrics defined in the spec.

**Files:**
- Create: `omop_cdm_pack/omop_dqd/reporting.py`
- Test: `omop_cdm_pack/tests/test_reporting.py`

**Interfaces:**
- Consumes: `EvaluatedCheck`, `CheckStatus`.
- Produces:
  - `build_metrics(results: List[EvaluatedCheck], dataset_label: str) -> List[dict]`
  - `build_recommendations(results: List[EvaluatedCheck], dataset_label: str) -> List[dict]`
  - `SEVERITY_WEIGHTS: Dict[str, float]` = `{"fatal": 3.0, "convention": 2.0, "characterization": 1.0}`

Metric records use the shape every other pack emits:
`{"key": str, "value": str, "scope": {"perimeter": str, "value": str}}`.
Recommendation records use
`{"content": str, "type": str, "scope": {...}, "level": str}`.

- [ ] **Step 1: Write the failing test**

Create `omop_cdm_pack/tests/test_reporting.py`:

```python
from omop_dqd.catalog import CheckInstance
from omop_dqd.evaluate import EvaluatedCheck
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckResult, CheckStatus


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


def test_score_is_one_when_everything_passes():
    metrics = build_metrics(
        [_evaluated("isRequired", CheckStatus.PASS)], "ds"
    )
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
    # (weight 1) -> 1 / 4 = 0.25
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


def test_not_applicable_checks_are_excluded_from_the_score():
    results = [
        _evaluated("isRequired", CheckStatus.PASS),
        _evaluated("fkDomain", CheckStatus.NOT_APPLICABLE),
        _evaluated("fkClass", CheckStatus.ERROR),
    ]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 1.0


def test_score_is_zero_when_nothing_is_applicable():
    results = [_evaluated("fkDomain", CheckStatus.NOT_APPLICABLE)]
    score = _by_key(build_metrics(results, "ds"), "score", "dataset")[0]
    assert float(score["value"]) == 0.0


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


def test_pct_violated_is_emitted_only_for_failures():
    results = [
        _evaluated(
            "isRequired",
            CheckStatus.FAIL,
            violated=3,
            denominator=10,
            field="a",
        ),
        _evaluated(
            "isRequired", CheckStatus.PASS, field="b", violated=0
        ),
    ]
    metrics = _by_key(build_metrics(results, "ds"), "pct_violated_rows")
    assert len(metrics) == 1
    assert float(metrics[0]["value"]) == 30.0
    assert metrics[0]["scope"]["value"] == "PERSON.a"


def test_metric_values_are_strings():
    metrics = build_metrics(
        [_evaluated("isRequired", CheckStatus.PASS)], "ds"
    )
    assert all(isinstance(m["value"], str) for m in metrics)


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
    assert (
        recommendation["scope"]["parent_scope"]["value"] == "ds"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_reporting.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'omop_dqd.reporting'`.

- [ ] **Step 3: Write the implementation**

Create `omop_cdm_pack/omop_dqd/reporting.py`:

```python
"""Aggregation of check results into QALITA metrics and recommendations."""

from collections import defaultdict
from typing import Dict, List

from omop_dqd.evaluate import EvaluatedCheck
from omop_dqd.results import CheckStatus

SEVERITY_WEIGHTS: Dict[str, float] = {
    "fatal": 3.0,
    "convention": 2.0,
    "characterization": 1.0,
}

# Kahn categories in the vendored metadata map onto these metric keys.
KAHN_METRIC_KEYS = {
    "conformance": "conformance_score",
    "completeness": "completeness_score",
    "plausibility": "plausibility_score",
}

DECIDED = (CheckStatus.PASS, CheckStatus.FAIL)


def _weighted_score(results: List[EvaluatedCheck]) -> float:
    """Share of passing checks, weighted by severity.

    NOT_APPLICABLE and ERROR checks are excluded: a check that could not
    run says nothing about quality. A scope with nothing decidable
    scores 0 rather than a misleading 1.
    """
    total = 0.0
    passed = 0.0
    for evaluated in results:
        if evaluated.result.status not in DECIDED:
            continue
        weight = SEVERITY_WEIGHTS.get(evaluated.instance.severity, 1.0)
        total += weight
        if evaluated.result.status == CheckStatus.PASS:
            passed += weight
    if total == 0.0:
        return 0.0
    return passed / total


def _metric(key: str, value: float, perimeter: str, scope_value: str):
    return {
        "key": key,
        "value": str(round(value, 4)),
        "scope": {"perimeter": perimeter, "value": scope_value},
    }


def build_metrics(
    results: List[EvaluatedCheck], dataset_label: str
) -> List[dict]:
    metrics = [
        _metric(
            "score", _weighted_score(results), "dataset", dataset_label
        )
    ]

    by_category = defaultdict(list)
    for evaluated in results:
        category = evaluated.instance.kahn_category.strip().lower()
        if category in KAHN_METRIC_KEYS:
            by_category[category].append(evaluated)
    for category, key in KAHN_METRIC_KEYS.items():
        if by_category[category]:
            metrics.append(
                _metric(
                    key,
                    _weighted_score(by_category[category]),
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
    if pct_violated >= 20.0:
        return "high"
    if pct_violated > 0.0:
        return "warning"
    return "info"


def build_recommendations(
    results: List[EvaluatedCheck], dataset_label: str
) -> List[dict]:
    """One recommendation per failing fatal check.

    The wording reuses the upstream checkDescription, which is the
    remediation guidance the OMOP community wrote for that check.
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
```

- [ ] **Step 4: Run the tests**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/test_reporting.py -v
```

Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack/omop_dqd/reporting.py \
        omop_cdm_pack/tests/test_reporting.py
git commit -m "feat(omop_cdm_pack): aggregate check results into metrics"
```

---

### Task 12: Pack entry point and end-to-end test

**Files:**
- Create: `omop_cdm_pack/main.py`
- Create: `omop_cdm_pack/pack_conf.json`
- Create: `omop_cdm_pack/omop_dqd/loader.py`
- Test: `omop_cdm_pack/tests/test_end_to_end.py`

**Interfaces:**
- Consumes: everything above, plus `qalita_core.pack.Pack`.
- Produces: `load_cdm_tables(pack, cdm_version, excluded_tables) -> CdmContext` in `loader.py`; a runnable `main.py`.

- [ ] **Step 1: Write `pack_conf.json`**

```json
{
  "job": {
    "cdm_version": "5.4",
    "excluded_tables": [],
    "threshold_overrides": {},
    "source": {
      "skip_missing_tables": true
    }
  },
  "charts": {
    "overview": [
      { "metric_key": "score", "chart_type": "text", "display_title": true, "justify": true },
      { "metric_key": "score", "chart_type": "bar_chart", "display_title": true, "justify": false }
    ],
    "scoped": [
      { "metric_key": "conformance_score", "chart_type": "text", "display_title": true, "justify": true },
      { "metric_key": "completeness_score", "chart_type": "text", "display_title": true, "justify": true },
      { "metric_key": "plausibility_score", "chart_type": "text", "display_title": true, "justify": true },
      { "metric_key": "score", "chart_type": "spark_area_chart", "display_title": false, "justify": false }
    ]
  }
}
```

- [ ] **Step 2: Write the loader**

Create `omop_cdm_pack/omop_dqd/loader.py`. It calls `load_data` **once per table** so
each table's parquet paths stay identifiable; passing a list would return a flat,
unattributable path list.

```python
"""Materialisation of the CDM tables the catalog needs."""

import logging
from typing import Iterable, List, Set

from omop_dqd.catalog import load_catalog
from omop_dqd.context import CdmContext, VOCABULARY_TABLES

logger = logging.getLogger(__name__)


def catalog_table_names(cdm_version: str) -> List[str]:
    """Every CDM table the catalog references, plus the vocabulary."""
    names: Set[str] = {
        check.cdm_table_name for check in load_catalog(cdm_version)
    }
    names.update(VOCABULARY_TABLES)
    # PERSON, DEATH and VISIT_OCCURRENCE are joined by field-level
    # checks even when no check targets them directly.
    names.update({"PERSON", "DEATH", "VISIT_OCCURRENCE"})
    return sorted(names)


def load_cdm_tables(
    pack, cdm_version: str, excluded_tables: Iterable[str] = ()
) -> CdmContext:
    """Load every available CDM table, skipping the ones that error.

    A table absent from the source raises inside qalita_core; that is
    expected and simply means the table is not part of this CDM
    instance. The cdmTable check reports it.
    """
    excluded = {name.upper() for name in excluded_tables}
    table_paths = {}
    for table_name in catalog_table_names(cdm_version):
        if table_name in excluded:
            logger.info("skipping excluded table %s", table_name)
            continue
        try:
            paths = pack.load_data(
                "source", table_or_query=table_name
            )
        except Exception as exc:  # noqa: BLE001 - absence is expected
            logger.info(
                "table %s unavailable in source: %s", table_name, exc
            )
            continue
        if paths:
            table_paths[table_name] = paths
    return CdmContext.from_paths(table_paths)
```

- [ ] **Step 3: Write `main.py`**

```python
"""QALITA pack entry point: OMOP CDM data quality assessment."""

import logging

from qalita_core.pack import Pack

import omop_dqd.checks  # noqa: F401  (registers every check)
from omop_dqd.catalog import load_catalog
from omop_dqd.loader import load_cdm_tables
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with Pack() as pack:
    job = pack.pack_config.get("job", {})
    cdm_version = str(job.get("cdm_version", "5.4"))
    excluded_tables = job.get("excluded_tables", [])
    dataset_label = pack.source_config.get("name", "omop_cdm")

    context = load_cdm_tables(pack, cdm_version, excluded_tables)
    logger.info(
        "loaded %d CDM tables (vocabulary: %s)",
        len(context.available_tables),
        "yes" if context.has_vocabulary else "no",
    )

    catalog = load_catalog(cdm_version)
    overrides = job.get("threshold_overrides", {})
    if overrides:
        from dataclasses import replace

        catalog = [
            replace(check, threshold=float(overrides[check.check_name]))
            if check.check_name in overrides
            else check
            for check in catalog
        ]

    results = run_checks(context, catalog)

    tally = {}
    for evaluated in results:
        tally[evaluated.result.status] = (
            tally.get(evaluated.result.status, 0) + 1
        )
    logger.info(
        "checks: %d passed, %d failed, %d not applicable, %d errored",
        tally.get(CheckStatus.PASS, 0),
        tally.get(CheckStatus.FAIL, 0),
        tally.get(CheckStatus.NOT_APPLICABLE, 0),
        tally.get(CheckStatus.ERROR, 0),
    )

    pack.metrics.data = build_metrics(results, dataset_label)
    pack.recommendations.data = build_recommendations(
        results, dataset_label
    )

    pack.metrics.save()
    pack.recommendations.save()
```

- [ ] **Step 4: Write the end-to-end test**

Create `omop_cdm_pack/tests/test_end_to_end.py`. It bypasses `Pack` (which needs
platform config) and drives the pipeline directly.

```python
import omop_dqd.checks  # noqa: F401
from omop_dqd.catalog import load_catalog
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks


def test_full_pipeline_produces_metrics(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    metrics = build_metrics(results, "mini_cdm")

    keys = {m["key"] for m in metrics}
    assert "score" in keys
    dataset_scores = [
        m
        for m in metrics
        if m["key"] == "score" and m["scope"]["perimeter"] == "dataset"
    ]
    assert len(dataset_scores) == 1
    assert 0.0 <= float(dataset_scores[0]["value"]) <= 1.0


def test_full_pipeline_finds_the_planted_violations(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    failed = {
        (r.instance.check_name, r.instance.qualified_field)
        for r in results
        if r.result.status == CheckStatus.FAIL
    }
    # the duplicate primary key planted in the fixture
    assert (
        "isPrimaryKey",
        "CONDITION_OCCURRENCE.condition_occurrence_id",
    ) in failed


def test_full_pipeline_produces_recommendations(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    recommendations = build_recommendations(results, "mini_cdm")
    assert recommendations
    assert all(r["type"] == "OMOP CDM" for r in recommendations)


def test_missing_vocabulary_yields_not_applicable_not_failures(
    mini_cdm_no_vocabulary,
):
    results = run_checks(
        mini_cdm_no_vocabulary, load_catalog("5.4")
    )
    vocabulary_checks = [
        r
        for r in results
        if r.instance.check_name
        in {"fkDomain", "fkClass", "isStandardValidConcept"}
    ]
    assert vocabulary_checks
    assert all(
        r.result.status == CheckStatus.NOT_APPLICABLE
        for r in vocabulary_checks
    )


def test_both_cdm_versions_run(mini_cdm):
    for version in ("5.3", "5.4"):
        results = run_checks(mini_cdm, load_catalog(version))
        assert results
```

- [ ] **Step 5: Run the whole suite**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
python -m pytest tests/ -v
```

Expected: every test passes across all 9 test modules.

- [ ] **Step 6: Lint and format**

```bash
cd /home/aleopold/qalita/packs/omop_cdm_pack
black . --line-length 79
python -m flake8 . --max-line-length 88
python -m pylint omop_dqd --disable=C0114,C0115,C0116
```

Fix anything reported. `black` reformatting is expected on first run.

- [ ] **Step 7: Commit**

```bash
cd /home/aleopold/qalita/packs
git add omop_cdm_pack
git commit -m "feat(omop_cdm_pack): add pack entry point and end-to-end tests"
```

- [ ] **Step 8: Update the repo AGENTS.md architecture list**

In `/home/aleopold/qalita/packs/AGENTS.md`, add this line to the architecture tree,
after the `fhir_compliance_pack` entry:

```
├── omop_cdm_pack/               # OMOP CDM quality (OHDSI DQD port)
```

While editing, correct the false licence claim in the same file: the **License** row of
the Project section says `Apache 2.0`, but every pack ships the proprietary QALITA
SOFTWARE LICENSE AGREEMENT. Change it to `Proprietary (QALITA Software License Agreement)`.

```bash
cd /home/aleopold/qalita/packs
git add AGENTS.md
git commit -m "docs: register omop_cdm_pack and correct the licence statement"
```

---

## Deferred to a follow-up

These are deliberately **not** in this plan and must not be attempted as part of it.

- **Cross-validation against real DQD on Eunomia.** The spec's gold-standard fidelity check. It needs an R runtime, so it is a one-off manual exercise, not CI. Do it before raising `visibility` to `public`.
- **Legal sign-off on the derivative-work question** (spec §8). `properties.yaml` stays `visibility: private` until that is resolved.
- **Replacing the placeholder `icon.png`.**
- **Single-pass aggregate batching per table** (spec §5), as argued in Task 10. Only worth attempting after the Eunomia cross-validation locks in correct results to compare against.
- **Extracting the Polars check primitives into `qalita_core`.** Wait for a second consumer.
- **`runForCohort` support**, CDM versions before 5.3, and DQD's `sqlOnly` mode.
