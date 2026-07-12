## Duplicates Finder Pack

### Overview
Detects duplicate rows per dataset and computes duplication metrics and a dataset score. Supports multiple tables from databases and single DataFrames from files.

### How it works
- Loads the source as a DataFrame or list of DataFrames.
- For each dataset, selects `job.compute_uniqueness_columns` or uses all columns; counts duplicate rows and computes `duplication_score` and `score = 1 - duplication_score`.
- Emits recommendations if the score is below a threshold (implicit in the pack logic).

### Configuration
- `job.source.skiprows` (int, default 0)
- `job.compute_uniqueness_columns` (list, optional)
- `job.id_columns` (list, optional; used for export indexing)

### Usage
1) Configure `source_conf.json` and `pack_conf.json`.
2) For databases, set `table_or_query` to string, list, or `*`.
3) Run the pack.

### Outputs
- `metrics.json`: includes per-dataset `score` and `duplicates` counts.
- For file sources: `{YYYYMMDD}_duplicates_finder_report_{dataset}.xlsx` for the first dataset, listing duplicate rows with identifiers.

### Multi-table handling and scopes
- Each table is treated as a dataset; dataset names are taken from `table_or_query` or `{source_name}_{index}`. Scopes are dataset-specific and include `parent_scope` for databases when relevant.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
