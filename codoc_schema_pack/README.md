## codoc Schema Pack

### Overview
Checks that a source conforms to the [codoc data model](https://github.com/codoc-health/codoc-data-model-docs),
the open clinical data warehouse model published by codoc (itself derived from Dr. Warehouse):
11 tables across clinical data (`dwh_patient`, `dwh_patient_stay`, `dwh_patient_mvt`,
`dwh_patient_ipphist`, `dwh_document`, `dwh_data`), vocabularies (`dwh_thesaurus_data`) and
health-system referentials (`dwh_thesaurus_site`, `dwh_thesaurus_department`,
`dwh_thesaurus_unit`, `hospital_instance`).

### How it works
The model ships in `codoc_model/model.json`, pinned to an upstream commit (see its `source`
block). For each codoc table in scope:

| Check | What fails it | Scored |
|---|---|---|
| `table_present` | the table is absent from the source | yes |
| `column_present` | a model column is absent | yes |
| `column_type` | the stored type does not belong to the declared family (`bigint`/`integer` → integer, `double` → float, `varchar`/`text` → string, `timestamptz` → datetime, `date`, `boolean`) | yes |
| `not_null` | a `Required` column holds nulls | yes |
| `primary_key_unique` | the primary key holds nulls or duplicates | yes |
| `max_length` | a `varchar(n)` value is longer than `n` characters | yes |
| `unexpected_column` | the source carries a column the model does not define | no — reported only |

### Value conventions
The *ETL Conventions* of the codoc guide that can be verified from the data are checked too
(`job.conventions`, on by default) and count in the table score:

| Check | Convention |
|---|---|
| `accepted_values` | `dwh_patient.sex` ∈ F, M, O (empty = unknown) · `dwh_patient.death_code` ∈ d · `dwh_patient_stay.type_dos` ∈ Consultation, HDJ, HAD, Urgence, Hospitalisation, Ambulatoire, Externes · `dwh_patient_mvt.type_mvt` ∈ C, J, U, H, A, S, AM, AP, E · `dwh_thesaurus_data.value_type` ∈ numeric, text, present, liste · `dwh_data.activity_form` (PMSI) ∈ 30, 31, 32 |
| `date_order` | `out_date >= entry_date` on `dwh_patient_stay` — and on `dwh_patient_mvt`, which carries the same admission/discharge pair (the guide states it on the stay only) |
| `conditional_value` | `dwh_thesaurus_data.list_values` set if and only if `value_type = liste` |
| `master_identifier` | exactly one `master_patient_id = true` per patient in `dwh_patient_ipphist` (counted in patients) |
| `upload_id_format` | `upload_id` is a `YYYYMMDDHHMMSS` timestamp, on every table |
| `instance_code` | every `instance_*_id` code of the clinical tables exists in `hospital_instance.code` — only when `hospital_instance` is in scope and loaded |
| `cnil_null` | with `job.cnil_compliant: true`: `lastname`, `maiden_name`, `firstname`, `nss`, `phone_number`, `email`, `residence_address` and `dwh_patient_stay.encounter_num` are null. `encounter_num` is also declared *Required* upstream; in CNIL mode its NOT NULL check is dropped, the two cannot both hold |

Nulls never break a value convention (whether a column may be null is the `not_null` check's
business), and a convention whose columns are absent is not applicable rather than failed.
Not checked, on purpose: values the guide calls "not standardized yet" (`entry_mode`,
`mvt_exit_mode`…), how the `*_pid` hashes were computed, the January-1st default of partial
dates, and `mvt_order`.

Structural checks read the parquet footers only. Constraint checks run as **one streaming
aggregation per table**; only counts leave the pack, never a source value.

**Type strictness.** A column stored under another type whose every value converts to the
declared one — timestamps stored as text in a CSV, an integer key read as float — is
*convertible*. In `lenient` mode it passes; in `strict` mode it fails. `auto` (default) is
strict for database sources, where the DDL is under the warehouse's control, and lenient for
files, whose types are inferred. Values that do not convert always fail.

**Extensions.** The PMSI (`activity_form`, `intersectoriel_number`, `residency_zip_code`) and
drug (`frequency`, `period`, `route`…) field groups of `dwh_data` are expected only when
enabled in `job.extensions`. Present without being enabled, they are still type- and
length-checked, never reported as unexpected.

### Configuration
- `job.tables` (list, default all 11): codoc tables to check.
- `job.excluded_tables` (list, default `[]`).
- `job.extensions` (list, default `[]`): `pmsi`, `drugs`.
- `job.type_strictness` (`auto` | `strict` | `lenient`, default `auto`).
- `job.conventions` (bool, default `true`): check the ETL value conventions.
- `job.cnil_compliant` (bool, default `false`): require identifying columns to be null.
- `job.table` (string, optional): the codoc table a single-file source holds, when the file is
  not named after it.
- `job.source.skiprows` (int, default 0).

Unknown table, extension or strictness names fail the job instead of silently matching
nothing.

### Usage
- **Database** (`postgresql`, `mysql`, `oracle`, `mssql`, `sqlite`): point the source at the
  schema holding the warehouse (`config.schema`). Tables are listed, then each codoc table is
  read by name, case-insensitively.
- **Directory** (`folder`, or `file` with a directory path): one file per table, named after it
  (`dwh_patient.csv`, `hospital_instance.parquet`…). Files matching no codoc table are skipped
  and listed in an `info` recommendation.
- **Single file**: checked as the codoc table it is named after (or `job.table`); the other
  tables are out of scope, not missing.

### Outputs
- `metrics.json`
  - dataset: `score` (mean of the table scores: every table weighs the same, a missing one
    counts 0), `tables_expected`, `tables_present`, `tables_missing`, `columns_checked`,
    `column_missing_count`, `type_violations_count`, `null_violations_count`,
    `primary_key_violations_count`, `length_violations_count`, `column_unexpected_count`,
    one `<metric>_count` per convention below, and `conventions_failed_count` (number of
    failing checks of each kind).
  - table: `score` (share of passed scored checks), `row_count`, `checks_failed`.
  - column (`<table>.<column>`): `declared_type`, `stored_type`, and for each failing check
    `column_missing`, `type_violations`, `null_violations`, `primary_key_violations`,
    `length_violations`, `accepted_values_violations`, `date_order_violations`,
    `conditional_value_violations`, `upload_id_format_violations`,
    `master_identifier_violations` (patients), `instance_code_violations`,
    `identifying_values` (rows in violation) or `column_unexpected`.
- `recommendations.json`: one per failing check; missing and unexpected columns are grouped per
  table. `high` for missing tables, required columns, nulls in required columns, primary
  key violations, master identifiers and identifying values in a CNIL warehouse.
- `schemas.json`: present tables and their columns — model columns included even when missing,
  so their metrics have a node to attach to.
- `figures.json`: score by table, pass/fail composition of the checks, failures by check type.

### Not covered
Foreign-key integrity across tables (use `referential_integrity_pack`); only the instance codes
are matched across tables.

### Updating the model
```bash
git clone https://github.com/codoc-health/codoc-data-model-docs /tmp/codoc
python codoc_model/build_model.py /tmp/codoc > codoc_model/model.json
```
Only structural facts are extracted (name, datatype, length, required, keys); the descriptive
text stays upstream. The value conventions are transcribed by hand in
`codoc_model/conventions.py`: review them against the upstream *ETL Conventions* column when
moving the pinned commit.

### Tests
```bash
uv sync && .venv/bin/python -m pytest tests -q
```

### Icon
`icon.png` is the codoc logo, taken from the avatar of the
[codoc-health](https://github.com/codoc-health) GitHub organisation that publishes the data
model; it identifies the model this pack checks against and remains codoc's property.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
