## Accuracy Pack

### Overview
Assesses decimal precision consistency of float columns and computes per-column and per-dataset accuracy metrics and recommendations.

### How it works
- For each dataset, inspects float columns and counts decimal places per value.
- Emits `decimal_precision` (max decimals), `proportion_score` (share with most common decimal count), and dataset `score` (mean of per-column proportions).
- Emits recommendations when `proportion_score` is below a threshold.

### Configuration
- `job.source.skiprows` (int, default 0)
- Optional thresholds are handled inside the pack logic.

### Outputs
- `metrics.json`: per-column `decimal_precision`, `proportion_score`; per-dataset `score` and `float_score` (data-point-weighted proportion).
- `recommendations.json`: entries for columns and datasets with uneven rounding.

### Multi-table handling and scopes
- Each table is treated as a dataset; names from `table_or_query` or `{source_name}_{index}`. Database sources include `parent_scope` in scopes.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
