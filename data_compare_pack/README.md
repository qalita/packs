## Data Compare Pack

### Overview
Compares a source dataset against a target dataset using DataComPy and produces matching metrics and an optional mismatches report. Supports lists of tables for databases by pairing source and target datasets.

### How it works
- Loads `df_source` and `df_target` as DataFrames or lists of DataFrames; pairs them by index.
- Selects columns to compare from `job.compare_col_list` or uses the intersection of columns.
- Computes metrics per pairing: `precision`, `recall`, `f1_score`, and a `score` based on mismatches ratio; emits dataset-scoped counts and an optional formatted mismatches table.

### Configuration
- `job.compare_col_list` (list, optional): columns to compare.
- `job.id_columns` (list): join keys for comparison.
- `job.abs_tol` (float, default 1e-4), `job.rel_tol` (float, default 0): numeric tolerances.

### Usage
1) Configure `source_conf.json`, `target_conf.json`, and `pack_conf.json`.
2) Set `table_or_query` for databases (string, list, or `*`); datasets are paired in order.
3) Run the pack.

### Outputs
- `metrics.json`: per-pairing dataset metrics including `score`, `precision`, `recall`, `f1_score`, row/column summaries, and `mismatches_table` when present.
- For file sources: `{YYYYMMDD}_data_compare_report_{source_dataset}_vs_{target_dataset}.xlsx` per pairing with mismatched rows.

### Multi-table handling and scopes
- Each pairing uses dataset labels from `table_or_query` or `{source_name}_{index}`; scopes are dataset-specific.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
