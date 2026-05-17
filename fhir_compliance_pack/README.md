## FHIR Compliance Pack

Validates a dataset's compliance against a subset of HL7 FHIR (default `Patient`).

### Principles
- Configurable mapping of columns to FHIR fields (`pack_conf.json`)
- Checks: required fields, enums, patterns, ISO date, booleans
- Metrics: `score` (validity ratio), `completeness`, `validity_ratio`

### Minimal configuration (`pack_conf.json`)
```
{
  "job": {
    "resource_type": "Patient",
    "field_mappings": {"id": "id", "gender": "gender", "birthDate": "birthDate", "active": "active"},
    "required_fields": ["id"],
    "enums": {"gender": ["male", "female", "other", "unknown"]},
    "patterns": {"id": "^[A-Za-z0-9\-\.]{1,64}$"},
    "date_fields": ["birthDate"],
    "boolean_fields": ["active"]
  }
}
```
