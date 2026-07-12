## Soda Pack

### Overview
Runs data quality checks using Soda Core on your dataset(s) and computes per-check metrics and dataset scores. Supports databases returning multiple tables and file-based DataFrames.

### How it works
- Loads the source as a pandas DataFrame or a list of DataFrames.
- For each dataset, registers it in Soda, loads `checks.yaml`, executes the scan, and extracts metrics from Soda results.
- Computes a dataset score as proportion of passed checks; emits per-column completion scores and recommendations for failed checks.

### Configuration
- Provide `checks.yaml` in the pack folder.
- `source.config.table_or_query` (string | list | `*`) for databases.

### Outputs
- `metrics.json`: includes per-check metrics, dataset score (`score`), `check_passed`, `check_failed`, and per-column `check_completion_score`.
- `recommendations.json`: entries for failed checks (column- or dataset-scoped).

### Multi-table handling and scopes
- Each table is treated as a dataset; names come from `table_or_query` or default to `{source_name}_{index}`. Scopes include `parent_scope` for databases.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
