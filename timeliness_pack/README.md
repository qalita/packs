## Timeliness Pack

### Overview
Assesses the freshness of date columns and computes per-column and per-dataset timeliness metrics and a dataset score. Supports multiple datasets from databases.

### How it works
- Identifies date-like columns using patterns and parsing.
- For each dataset and each date column, computes `earliest_date`, `latest_date`, `days_since_earliest_date`, `days_since_latest_date` and per-column `timeliness_score`.
- Computes a dataset `score` from average `days_since_latest_date` across selected columns.

### Configuration
- `job.source.skiprows` (int, default 0)
- `job.compute_score_columns` (list, optional): subset of date columns used to compute dataset score.

### Outputs
- `metrics.json`: per-column date metrics and timeliness scores, plus a per-dataset `score` and a `date_columns_count` summary.
- `recommendations.json`: entries when latest dates are older than one year (high level).

### Multi-table handling and scopes
- Each table is a dataset; names from `table_or_query` or `{source_name}_{index}` and scopes include `parent_scope` for databases.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
