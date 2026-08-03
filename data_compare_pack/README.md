## Data Compare Pack

### Overview
Compares a source dataset against a target dataset and produces matching metrics plus a bounded mismatches report. Supports lists of tables for databases by pairing source and target objects.

### How it works
- Both sides are reached through `pack.scan(trigger, table=...)` and joined lazily: `source.join(target, on=id_columns, how="full", coalesce=True, suffix="_target", nulls_equal=True)`.
- Row counts (in common / source-only / target-only), per-column mismatch counts, mismatching rows and mismatching values all come out of **one** streamed aggregation over that join. Scalars come back; rows do not.
- Example mismatch rows go through `analytics.failures()`, so they are capped inside the lazy plan.
- Numeric columns use the tolerance rule `|a - b| <= abs_tol + rel_tol * |b|`. Two nulls are equal, one null is a mismatch, and columns whose dtypes differ between the two sides are compared as text instead of raising.

### Why datacompy was dropped
`datacompy` 0.18.1 ships `datacompy.PolarsCompare`, but it does not accept LazyFrames: `_validate_dataframe` raises `TypeError(f"{index} must be a Polars DataFrame")`, and the comparison itself is an eager `df1.join(df2, how="full", coalesce=True, join_nulls=True, ...)`. Both `Compare` (pandas) and `PolarsCompare` therefore materialize a frame holding every column of both sides, which is exactly what this pack must not do. The join is now done directly and the dependency is gone.

### Configuration
- `job.compare_col_list` (list, optional): columns to compare. Default: the intersection of both schemas, in source order.
- `job.id_columns` (list): join keys. Default: every compared column.
- `job.abs_tol` (float, default 1e-4), `job.rel_tol` (float, default 0): numeric tolerances.
- `job.mismatch_examples` (int, default 10, hard cap 1000): rows kept in `mismatches_table` and in the xlsx report. `0` disables example rows.

### Usage
1) Configure `source_conf.json`, `target_conf.json`, and `pack_conf.json`.
2) Set `table_or_query` for databases (string, list, or `*`); objects are paired in order.
3) Run the pack.

### Outputs
- `metrics.json`: per-pairing `score`, `precision`, `recall`, `f1_score`, the `dataframe_summary_*`, `column_summary_*`, `row_summary_*` and `column_comparison_*` counts, `recommendation_levels_mismatches` and `mismatches_table`.
- `comparison_report_{source}_vs_{target}.txt`: plain-text summary, same sections as before.
- For file sources: `{YYYYMMDD}_data_compare_report_{source}_vs_{target}.xlsx` with the bounded mismatching rows.

### Caveats
- `id_columns` should be unique on both sides. A full join on duplicated keys produces the cartesian product of the matching groups, which inflates `rows_in_common` and can be very large. This is the same requirement datacompy had.

### Behaviour changes in 2.1.0
- **Correctness**: above 1,000,000 rows the previous version `head()`-sampled each side **independently**. Two independent head samples of a join have near-disjoint key sets, so `precision`, `recall`, `f1_score` and the row-summary counts were wrong, not approximate. Sampling is removed entirely; both sides are always joined in full.
- **Correctness**: counts are no longer parsed out of a rendered text report. The old parser read `12,345` as `12` (it stopped at the thousands separator), so every count above 999 was silently truncated.
- **Row counts**: `dataframe_summary_number_rows_*` are exact dataset row counts read from the parquet footers, not the row count of a sampled pandas frame.
- **Column counts**: `column_summary_number_of_columns_in_*_but_not_in_*` are computed on the full schemas. They were previously always `0`, because datacompy only ever saw the pre-intersected column subset.
- **Tolerances**: `row_summary_default_absolute_tolerance` and `row_summary_default_relative_tolerance` now carry the configured value. They were previously always `"0"`, an artefact of the same parser.
- **New metrics**: `row_summary_number_of_rows_with_some_compared_columns_unequal` and `row_summary_number_of_rows_with_all_compared_columns_equal`, which the old section regex cut off at the blank line.
- **Mismatch examples**: `mismatches_table` now holds at most `job.mismatch_examples` rows (default 10, was up to 10000) and keeps its `truncated` / `total_mismatches` flags. The xlsx export is bounded by the same limit.
- **Case sensitivity**: column names are no longer lower-cased. datacompy lower-cased them by default, so a source with mixed-case column names will now match by its real names.
- **Scopes**: with a single object per side the labels are the source and target names (unchanged); with several, the labels are the object names reported by the loader instead of positional `{name}_{index}` labels.
- **Dependencies**: `datacompy` and `pandas` are gone; `polars` and `openpyxl` are declared explicitly.

### Tests
```bash
PYTHONPATH=<path-to-qalita-core> python -m pytest tests/ -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
