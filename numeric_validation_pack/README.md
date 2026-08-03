# Numeric Validation Pack

Validates numeric data against configurable ranges and constraints.

##  Checks Covered

- `number_below_min_value` / `number_above_max_value`
- `number_in_range_percent` / `integer_in_range_percent`
- `negative_values` / `negative_values_percent`
- `min_in_range` / `max_in_range` / `sum_in_range` / `mean_in_range`
- `valid_latitude_percent` / `valid_longitude_percent`
- `invalid_latitude` / `invalid_longitude`

## How it works

Data is read through `pack.scan()` as a Polars LazyFrame. Every rule of every
column — plus the negative-value check of every numeric column — is folded into
ONE streaming aggregation per dataset, so the source is read once whatever the
number of rules. Column dtypes come from the parquet footers, which costs no
data read at all.

## Configuration

```json
{
  "job": {
    "rules": [
      {"column": "age", "min_value": 0, "max_value": 150},
      {"column": "price", "min_value": 0},
      {"column": "latitude", "type": "latitude"},
      {"column": "longitude", "type": "longitude"},
      {"column": "percentage", "type": "percentage"},
      {"column": "quantity", "type": "non_negative"}
    ],
    "check_negative_values": true,
    "id_columns": ["order_id"],
    "examples": true,
    "example_rows": 10,
    "example_max_checks": 20
  }
}
```

- `id_columns` (list, default `[]`) — columns added to the example rows so a failing value can be traced back to a record.
- `examples` (bool, default `true`) — emit bounded failing-row examples.
- `example_rows` (int, default 10, hard cap 1000) — rows per failing check.
- `example_max_checks` (int, default 20) — how many checks may emit examples; each example set costs one extra filtered pass over the source.

## Rule Types

| Type | Min Value | Max Value | Description |
|------|-----------|-----------|-------------|
| `latitude` | -90 | 90 | Geographic latitude |
| `longitude` | -180 | 180 | Geographic longitude |
| `percentage` | 0 | 100 | Percentage values |
| `non_negative` | 0 | - | Non-negative numbers |
| (custom) | configurable | configurable | Custom range |

## Metrics Output

- `number_below_min_value`: Count of values below minimum
- `number_above_max_value`: Count of values above maximum
- `number_in_range_percent`: Percentage of values in valid range
- `min_value`: Actual minimum value in column
- `max_value`: Actual maximum value in column
- `sum_value`: Sum of all values
- `mean_value`: Mean of all values
- `negative_values`: Count of negative values
- `negative_values_percent`: Percentage of negative values
- `out_of_range_examples`: Bounded list of rows violating the range rule
- `negative_values_examples`: Bounded list of rows carrying a negative value
- `score`: Overall validity score (0-1)

## Changes in 0.2.0

- Streaming rewrite: `pandas`/`numpy` and the per-part `read_parquet` preamble are gone. The pack now depends on `qalita-core>=2.0.0` and declares `polars` explicitly.
- **Breaking**: dataset scope values are now the logical object names returned by `pack.tables("source")` (e.g. `mysource_orders`) instead of `{source_name}_{index}`. A chunked source used to produce one numbered dataset per part, and every part was validated as if it were a separate table.
- New metrics: `out_of_range_examples` and `negative_values_examples`.
- Column typing now comes from the parquet schema. Boolean columns are no longer treated as numeric (pandas' `is_numeric_dtype` accepted them), so a rule targeting a boolean column is skipped.
- NaN is excluded from every aggregation, as `dropna()` did; Polars would otherwise let a single NaN poison `sum` and `mean`.

## Tests

```bash
cd numeric_validation_pack && python -m pytest tests -q
```

## License

Proprietary - QALITA SAS
