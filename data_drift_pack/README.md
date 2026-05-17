## Data Drift Pack

Monitors statistical drift between a reference and a current dataset using KS-tests for numeric columns.

### Metrics
- `p_value` per column
- `score` dataset-level (share of non-drifting columns at alpha=0.05)

### Config
Configure windows and test type in `pack_conf.json`.
