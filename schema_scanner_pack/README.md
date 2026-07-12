## Schema Scanner Pack

### Overview
Generates a minimal profile and extracts schema (column list) per dataset, emitting standardized `schemas.json`. Supports multiple datasets from databases and single files.

### How it works
- Loads the source as a DataFrame or list of DataFrames.
- For each dataset, runs a minimal `ydata-profiling` report, writes HTML/JSON, and extracts variable names to build schema entries.

### Configuration
- `source.config.table_or_query` (string | list | `*`) for databases.

### Outputs
- Files per dataset: `{dataset_name}_report.html`, `{dataset_name}_report.json` (minimal profile).
- `schemas.json`: entries for each column and dataset; dataset entries include `parent_scope` when source is a database.

### Multi-table handling and scopes
- Each table is represented as an individual dataset; names are from `table_or_query` or `{source_name}_{index}`.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
