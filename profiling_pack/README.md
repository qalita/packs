## Profiling Pack

### Overview
Profiles your dataset(s) and produces metrics, recommendations, schema entries
and figures. Supports file sources and databases returning one or several
tables.

### What changed in 2.1.0 (breaking)

**Statistics are now computed over the whole dataset.** Until 2.0.x the pack
loaded the source into pandas and, above one million rows, profiled
`head(500_000)` while labelling the result a profile of the dataset. On a
chunked source `head()` reads only the first part files, so every distribution
described the first partition instead of the data. `ydata-profiling` (and with
it pandas, matplotlib, lxml, html5lib and beautifulsoup4) has been replaced by
the streaming profiler in `qalita_core.profiling`, which reads the source with
Polars in a bounded number of streaming passes and never materializes it.

**Distinct counts and quantiles are approximate by default.** Distinct counts
use HyperLogLog and quantiles a fixed-width histogram, both with memory
independent of the row count. Set `exact: true` in `pack_conf.json` to compute
them exactly — exact distinct counts cost one entry per distinct value, which
on a primary key is one entry per row. Every metric derived from an approximate
statistic ships a sibling `<key>_method` metric whose value is `hyperloglog`,
`histogram` or `exact`, so the origin of a number is always readable.

**Metrics dropped.** They needed either the whole frame in memory or one
group per distinct value, and neither is available on a source larger than the
worker:

| Dropped                                                                                                  | Why                                                                        |
| -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `word_counts`, `character_counts`, `script_counts`, `script_char_counts`, `n_scripts`, `n_characters_distinct` | one entry per word / character / script per column                         |
| `category_alias_values`, `category_alias_char_counts`, `block_alias_*`, `n_category`                     | same, and specific to ydata's category detection                            |
| `histogram`, `histogram_length`                                                                          | already stripped from the output before being emitted                       |
| `value_counts_without_nan`, `value_counts_index_sorted`                                                  | already stripped; the bounded top values are emitted as figures instead      |
| `first_rows`                                                                                             | raw row values; the pack emits bounded top values instead                    |
| `mad`                                                                                                    | needs a second ordering pass per numeric column                             |
| `n_duplicates`, `p_duplicates`                                                                           | an exact duplicate count needs one entry per distinct row; use the duplicates_finder pack |
| `hashable`                                                                                               | always true for a Parquet-backed column                                     |
| `memory_size` / `record_size` semantics                                                                  | still emitted, now an estimate of the uncompressed footprint rather than the pandas frame size |

**Metrics whose meaning changed.**

- `n_unique` / `p_unique` now mean *number of distinct values* (the same figure
  as `n_distinct`). ydata counted values occurring exactly once, which needs
  one group per distinct value.
- `is_unique` is exact when `exact: true`; otherwise it is `true` when the
  approximate distinct ratio is at least 0.9, because an approximate count
  cannot answer an equality. `is_unique_method` says which test ran.
- `p_missing`, `p_distinct`, `p_zeros`, `p_negative`, `p_infinite`,
  `p_cells_missing` are now rounded to 6 decimals instead of 2.
- `type` keeps its ydata-style family (`Numeric`, `Text`, `DateTime`,
  `Boolean`, `Categorical`, `Unsupported`); the exact Polars dtype is emitted
  next to it as `dtype`.
- Recommendations are derived from the streamed statistics instead of being
  scraped out of the HTML report. The families (`Missing`, `Constant`,
  `Unique`, `High cardinality`, `Zeros`, `Skewed`, `Infinite`) and the
  `content` / `type` / `scope` / `level` shape are unchanged.
- The HTML report is gone with `ydata-profiling`. `{dataset}_report.json` is
  still written, and for a file source a dated copy is dropped next to the
  source file as `{YYYYMMDD}_profiling_report_{source_name}.json`.

### How it works
- `pack.load_data()` materializes the source to Parquet parts, then the pack
  iterates `pack.tables()` and scans each logical object with `pack.scan()`.
  The parts of a chunked object are a single dataset to the engine, so no
  metric is ever computed on one partition.
- For each object: one streaming pass for every scalar statistic of every
  column, one pass for the statistics the profiler does not cover (skewness,
  kurtosis, monotonicity, infinities, byte sizes), two passes for approximate
  quantiles, two more for string lengths, and one bounded pass per column for
  top values.
- Dataset aggregates (`n_cells_missing`, `p_cells_missing`, `score`, the
  `types_*` counts) are derived from the per-column profile without re-reading
  the source.

### Configuration
- `exact` (bool, default `false`): compute distinct counts and quantiles
  exactly instead of approximately.
- `top_k` (int, default 10): number of most frequent values reported per
  column. `0` disables the per-column pass.
- `high_cardinality_threshold` (int, default 50): distinct values above which a
  text column raises a `High cardinality` recommendation.
- `job.source.skiprows` (int, default 0): number of rows to skip when reading
  files.
- `source.config.table_or_query` (string | list | `*`): database table name,
  SQL query, list of tables, or `*` to scan all tables.

### Metrics

| Name                        | Description                                                     | Scope          | Type      |
| --------------------------- | --------------------------------------------------------------- | -------------- | --------- |
| `n`                         | Number of records                                               | dataset        | `integer` |
| `n_var`                     | Number of variables                                             | dataset        | `integer` |
| `n_cells_missing`           | Number of empty cells                                           | dataset        | `integer` |
| `p_cells_missing`           | Share of empty cells                                            | dataset        | `float`   |
| `n_vars_with_missing`       | Number of variables containing missing values                   | dataset        | `integer` |
| `n_vars_all_missing`        | Number of variables containing 100% missing values              | dataset        | `integer` |
| `memory_size`               | Estimated uncompressed size of the dataset, in bytes            | dataset        | `integer` |
| `record_size`               | Estimated uncompressed size of one record, in bytes             | dataset        | `integer` |
| `types_numeric`             | Number of numeric variables                                     | dataset        | `integer` |
| `types_text`                | Number of text variables                                        | dataset        | `integer` |
| `types_datetime`            | Number of date/datetime variables                               | dataset        | `integer` |
| `types_boolean`             | Number of boolean variables                                     | dataset        | `integer` |
| `types_categorical`         | Number of categorical variables                                 | dataset        | `integer` |
| `types_unsupported`         | Number of variables with an unsupported type                    | dataset        | `integer` |
| `score`                     | Completeness score of the dataset, `1 - p_cells_missing`        | dataset        | `float`   |
| `type`                      | Family of the variable                                          | column         | `string`  |
| `dtype`                     | Polars dtype of the variable                                    | column         | `string`  |
| `n`                         | Number of records                                               | column         | `integer` |
| `count`                     | Number of non-null values                                       | column         | `integer` |
| `n_missing`                 | Number of missing values                                        | column         | `integer` |
| `p_missing`                 | Share of missing values                                         | column         | `float`   |
| `completeness_score`        | Completeness score of the variable                              | column         | `float`   |
| `n_distinct`                | Number of distinct values (approximate by default)              | column         | `integer` |
| `p_distinct`                | Share of distinct values (approximate by default)               | column         | `float`   |
| `n_unique` / `p_unique`     | Aliases of `n_distinct` / `p_distinct`                          | column         | `integer` |
| `is_unique`                 | Whether the variable looks like a key                           | column         | `boolean` |
| `*_method`                  | `hyperloglog`, `histogram` or `exact` for the metric it names   | column         | `string`  |
| `min` / `max`               | Extreme values                                                  | column         | `float`   |
| `range`                     | `max - min`                                                     | column:Numeric | `float`   |
| `sum` / `mean` / `std`      | Sum, mean, sample standard deviation                            | column:Numeric | `float`   |
| `variance`                  | Sample variance                                                 | column:Numeric | `float`   |
| `sample_stddev`             | Standard deviation, ddof=1                                      | column:Numeric | `float`   |
| `population_stddev`         | Standard deviation, ddof=0                                      | column:Numeric | `float`   |
| `sample_variance`           | Variance, ddof=1                                                | column:Numeric | `float`   |
| `population_variance`       | Variance, ddof=0                                                | column:Numeric | `float`   |
| `cv`                        | Coefficient of variation                                        | column:Numeric | `float`   |
| `n_zeros` / `p_zeros`       | Number and share of zeros                                       | column:Numeric | `float`   |
| `n_negative` / `p_negative` | Number and share of negative values                             | column:Numeric | `float`   |
| `n_infinite` / `p_infinite` | Number and share of infinite values (float columns)             | column:Numeric | `float`   |
| `skewness` / `kurtosis`     | Bias-corrected skewness and excess kurtosis                     | column:Numeric | `float`   |
| `iqr`                       | Interquartile range                                             | column:Numeric | `float`   |
| `5%` … `95%`                | 5th, 25th, 50th, 75th, 95th percentiles                         | column:Numeric | `float`   |
| `percentile_10` … `percentile_90` | 10th, 25th, 75th, 90th percentiles                        | column:Numeric | `float`   |
| `monotonic*` / `ordering`   | Monotonicity of the variable in the order it was loaded         | column:Numeric | `boolean` |
| `min_length` / `max_length` | Extreme value lengths, in characters                            | column:Text    | `integer` |
| `mean_length`               | Mean value length                                               | column:Text    | `float`   |
| `median_length`             | Median value length (approximate by default)                    | column:Text    | `float`   |
| `n_characters`              | Total number of characters                                      | column:Text    | `integer` |
| `n_empty` / `p_empty`       | Number and share of empty strings                               | column:Text    | `float`   |

### Outputs
- `{dataset_name}_report.json` — the full profile of the dataset.
- For a file source: `{YYYYMMDD}_profiling_report_{source_name}.json` next to
  the source file.
- `metrics.json` — dataset- and column-scoped metrics.
- `recommendations.json` — findings with a level and a scope.
- `schemas.json` — dataset and column entries.
- `figures.json` — missing values per column, column type composition, and the
  bounded top values of each column.

### Multi-table handling and scopes
- A source holding one object is scoped to the source name, as before. When a
  database returns several tables, each is profiled separately, scoped to its
  own name, with the source as `parent_scope` of perimeter `database`.

### Tests
```
PYTHONPATH=/path/to/qalita-core python -m pytest tests/ -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
