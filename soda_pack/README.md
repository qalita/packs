## Soda Pack

### Overview
Runs data quality checks using Soda Core on your dataset(s) and computes
per-check metrics and dataset scores. Supports databases returning multiple
tables and file-based sources.

### Where the checks run

Since 1.1.0 the checks execute **in DuckDB, over the parquet staging** written
by `Pack.load_data`, not in pandas. One DuckDB view is created per logical
object and it spans **every** part file of that object. DuckDB evaluates
aggregates, filters and sorts out-of-core, so a run's memory is bounded by
DuckDB's buffer pool rather than by the row count.

Previously the pack used `scan.add_pandas_dataframe`, soda-core's only
in-memory-frame entry point. It comes from the separate
`soda-core-pandas-dask` plugin and needs the whole dataset materialized as a
pandas frame before dask ever sees it, so a source larger than the worker could
not be checked at all. `soda-core-pandas-dask`, `dask` and `pandas` are no
longer dependencies of this pack.

### How it works
- Loads the source into parquet parts (`Pack.load_data`).
- For each logical object, creates a DuckDB view over all of its parts with
  slugified column names, registers it as a Soda `duckdb` data source, loads
  `checks.yaml`, executes the scan and extracts metrics from Soda results.
- Computes a dataset score as the proportion of passed checks; emits per-column
  completion scores and recommendations for failed checks.

### Configuration
- Provide `checks.yaml` in the pack folder.
- `source.config.table_or_query` (string | list | `*`) for databases.

### Outputs
- `metrics.json`: per-check Soda metrics, plus `score`, `score_method`,
  `check_passed`, `check_failed`, `rows_analyzed` and per-column
  `check_completion_score`.
- `recommendations.json`: entries for failed checks (column- or
  dataset-scoped).

### Multi-table handling and scopes
Each logical object is one dataset. A single-object source keeps the source
name as its scope value; a multi-object source uses the object name. Column
scopes carry a `parent_scope` pointing at the dataset.

### Breaking changes in 1.1.0
- **Dataset count fix.** The old code zipped configured table names with
  parquet paths and, when the lengths disagreed — which is what chunking
  causes — fell through to labelling each *chunk* as its own dataset. A single
  table split into four parts was reported as four datasets `{source}_1..4`,
  each with its own score. Datasets now come from `pack.tables()`.
- Scope values therefore change for chunked and multi-table sources.
- New metric keys: `score_method` (always `exact` — every check runs over the
  whole dataset in DuckDB) and `rows_analyzed`.
- Column names that slugify to the same identifier now keep their original
  name instead of shadowing each other.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are
welcome: https://github.com/qalita/packs.
