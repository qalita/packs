## FHIR Compliance Pack

Validates a dataset's compliance against a subset of HL7 FHIR (default
`Patient`). Columns are mapped onto FHIR fields, and each rule — required, enum,
pattern, ISO date, boolean — is evaluated as a Polars expression, so a whole
dataset is scored in one streaming pass.

### Metrics

| Key | Scope | Meaning |
|-----|-------|---------|
| `field_violations` | column | Records violating a rule on this mapped field |
| `field_completeness` | column | Share of records where the field is populated |
| `completeness` | dataset | Populated mapped cells / (records x mapped fields) |
| `records` | dataset | Records examined |
| `invalid_records` | dataset | Records violating at least one rule |
| `validity_ratio` | dataset | Valid records / records |
| `invalid_record_examples` | dataset | Up to `examples` invalid records (JSON) |
| `score` | dataset (source) | `validity_ratio`, rounded to 2 decimals |
| `valid_records` | dataset (source) | Valid records across every table |

### Minimal configuration (`pack_conf.json`)

```json
{
  "job": {
    "resource_type": "Patient",
    "examples": 10,
    "field_mappings": {"id": "id", "gender": "gender", "birthDate": "birthDate", "active": "active"},
    "required_fields": ["id"],
    "enums": {"gender": ["male", "female", "other", "unknown"]},
    "patterns": {"id": "^[A-Za-z0-9.-]{1,64}$"},
    "date_fields": ["birthDate"],
    "boolean_fields": ["active"]
  }
}
```

`examples` bounds the invalid records reported per table (default 10, hard cap
1000, `0` disables).

### Changes in 0.2.0

- **The row loop is gone.** 0.1.x ran `for idx in range(row_count)` and, per
  mapped field, did `series.iloc[idx]`, a `re.match` and a
  `datetime.fromisoformat` inside a `try/except`: O(rows x fields) of Python
  interpreter, which does not terminate at 1e9 rows whatever the machine's
  memory. Each rule is now one expression, reduced horizontally, evaluated once
  per dataset.
- **Chunked sources are fully validated.** The previous code zipped the
  `table_or_query` config against the loaded parquet parts, so on a chunked
  source it scored the first part and reported it as the dataset.
- **`completeness` is fixed.** The per-dataset value used a running global
  record count as its denominator, so with several tables every value after the
  first was wrong. It is now computed per table, and the source-level value is
  the record-weighted mean.
- Date validation uses an explicit `%Y-%m-%d` parse plus an ISO-8601 shape
  check, rather than format inference. Inference is decided per batch, so on a
  multi-part source it could accept a value in one part and reject the same
  value in another. `2023-02-30` is still rejected.
- `patterns` are still anchored at the start, matching `re.match` semantics.
- New keys: `records`, `invalid_records`, `valid_records`, `field_violations`,
  `field_completeness`, `invalid_record_examples`. `score`, `validity_ratio` and
  `completeness` keep their meaning.
- With several tables, `validity_ratio`, `completeness` and `records` are also
  emitted at the source scope; with a single table they are emitted once, at the
  dataset scope, to avoid two rows behind one (key, scope).
- pandas is gone from the dependencies; polars is declared explicitly.
