## Accuracy Pack

### Overview
Assesses decimal precision consistency of float columns and computes per-column and per-dataset accuracy metrics and recommendations. Latitude/longitude columns detected by name are validated against their geographic range.

### How it works
- Data is read through `pack.scan()` as a Polars LazyFrame; nothing is materialized in memory.
- Two streaming passes per dataset, whatever the number of columns: the first resolves the maximum decimal count of every float column (and settles the coordinate ranges), the second counts the decimal buckets of every column at once.
- Emits `decimal_precision` (max decimals), `most_common_decimals`, `proportion_score` (share of values carrying the most common decimal count), and dataset `score` / `float_score`.
- Emits recommendations when `proportion_score` is below a threshold, with up to `job.example_rows` failing rows as evidence.

### Configuration
- `job.source.skiprows` (int, default 0)
- `job.id_columns` (list, default `[]`) — columns added to the example rows so a failing value can be traced back to a record.
- `job.examples` (bool, default `true`) — emit bounded failing-row examples.
- `job.example_rows` (int, default 10, hard cap 1000) — rows per failing check.
- `job.example_max_checks` (int, default 20) — how many checks may emit examples; each example set costs one extra filtered pass over the source.

### Outputs
- `metrics.json`:
  - per column: `decimal_precision`, `most_common_decimals`, `proportion_score`, `invalid_latitude` / `valid_latitude_percent`, `invalid_longitude` / `valid_longitude_percent`
  - per column, when the check fails: `uneven_decimals_examples`, `invalid_latitude_examples`, `invalid_longitude_examples` — a bounded list of failing rows
  - per dataset: `score`, `float_score` (data-point-weighted proportion)
- `recommendations.json`: entries for columns and datasets with uneven rounding, and for invalid coordinates.

### Multi-table handling and scopes
- Each logical object of the source is a dataset, named by `pack.tables("source")`. A chunked object (`<source>_<object>_part_N.parquet`) is one dataset, not one per part.

### Changes in 2.1.0
- Streaming rewrite: pandas and the per-value `apply` that computed decimal counts are gone, and the source is no longer decompressed into memory. `pandas`/`pyarrow` were dropped from the dependencies; `polars` is declared explicitly.
- **Breaking**: dataset scope values are now the logical object names (e.g. `mysource_orders`) instead of `{source_name}_{index}`. A chunked source used to produce one numbered dataset per part.
- New metrics: `most_common_decimals` and the `*_examples` evidence lists.
- Coordinate checks now also run on datasets that contain no float column (they used to be skipped entirely in that case).
- Decimal counts come from the Polars rendering of a float, which matches Python's `str()` on the values a float64 can hold. NaN and null are excluded, as `dropna()` did.

### Tests
```bash
cd accuracy_pack && python -m pytest tests -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
