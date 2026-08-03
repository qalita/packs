## Duplicates Finder Pack

### Overview
Detects duplicate rows per dataset and computes duplication metrics and a dataset score. Supports multiple tables from databases and chunked file sources.

### How it works
- Reaches data only through `pack.scan(trigger, table=...)`, so the parts of a chunked table are a single lazy dataset and nothing is materialized.
- Duplicate counting stays inside the Polars streaming engine: `group_by(uniqueness_columns).agg(pl.len())` followed by `sum(size - 1)`, collected in streaming mode. The group table is never turned into a Python object.
- `job.compute_uniqueness_columns` selects the columns whose combination defines a duplicate; the default is every column. A configured column that does not exist is now an error instead of being silently ignored.
- Row-returning paths are bounded: the duplicates report goes through `analytics.failures()`, which caps the rows inside the lazy plan.

### Configuration
- `job.source.skiprows` (int, default 0)
- `job.compute_uniqueness_columns` (list, optional)
- `job.id_columns` (list, optional; kept for compatibility, no longer used for export indexing)
- `job.exact` (bool, default `true`) — exact duplicate counting via a streaming group-by. Set to `false` to derive the duplicate count from a HyperLogLog distinct count instead: O(1) memory, but it subtracts two large numbers, so at a low duplication rate the estimation error can exceed the answer. Whatever the setting, `duplicates_method` and `distinct_count_method` report which was used.
- `job.duplicate_rows_limit` (int, default 10000, hard cap 100000) — maximum rows written to the duplicates report. `0` disables the report.

### Usage
1) Configure `source_conf.json` and `pack_conf.json`.
2) For databases, set `table_or_query` to a string, a list, or `*`.
3) Run the pack.

### Outputs
- `metrics.json`: per-dataset `score`, `duplicates`, `distinct_count`, `distinct_percent`, plus `rows`, `duplicated_rows`, `duplicates_method` and `distinct_count_method`.
- For file sources: `{YYYYMMDD}_duplicates_finder_report_{source}.xlsx` for the first dataset, listing up to `job.duplicate_rows_limit` rows that belong to a duplicated key group.

### Multi-table handling and scopes
- One logical object is one dataset. With a single object the scope is the source name (unchanged); with several, the scope is the object name reported by the loader instead of a positional `{source_name}_{index}` label.

### Behaviour changes in 2.1.0
- **Memory**: the pack no longer reads every parquet chunk into pandas before counting. Peak memory is bounded by the streaming engine and by `job.duplicate_rows_limit`, not by the dataset size.
- **New metrics**: `rows`, `duplicated_rows` (exact number of rows living in a duplicated group), `duplicates_method`, `distinct_count_method`.
- **`duplicated_rows` and the xlsx report** are produced for `file` sources only, as before, and only when `job.duplicate_rows_limit > 0`.
- **Report contents**: the exported sheet now always carries every column. It used to drop the `id_columns` (they were moved to the index, then written with `index=False`) or add a synthetic `index` column.
- **Chunked sources**: counts are computed across every part. Datasets whose parts were previously dropped will report different values.
- **Recommendation message**: the duplication rate is now formatted with one decimal on every code path.
- **Dependencies**: `pandas` is gone; `polars` and `openpyxl` are declared explicitly.

### Tests
```bash
PYTHONPATH=<path-to-qalita-core> python -m pytest tests/ -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
