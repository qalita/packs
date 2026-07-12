## Profiling Pack

### Overview
Profiles your dataset(s) using `ydata-profiling` and produces comprehensive metrics, recommendations, and schema information. Supports single DataFrame sources and databases returning multiple tables.

### How it works
- Loads the source as a pandas DataFrame or a list of DataFrames (for databases when `table_or_query` is `*` or a list).
- For each dataset:
  - Generates a `ydata-profiling` HTML and JSON report.
  - Computes column-level completeness and general table metrics; derives a dataset `score = 1 - p_cells_missing`.
  - Extracts warnings from the profiling HTML as recommendations with levels.
  - Builds schema entries for each column and dataset.

### Configuration
- `job.source.skiprows` (int, default 0): number of rows to skip when reading files.
- `source.config.table_or_query` (string | list | `*`): database table name, SQL query, list of tables, or `*` to scan all tables.

### Metrics

| Name                         | Description                                                                                        | Scope          | Type                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------- | -------------- | ----------------------------------- |
| `n`                          | Number of records                                                                                  | dataset        | `integer`                           |
| `n_var`                      | Number of variables                                                                                | dataset        | `integer`                           |
| `memory_size`                | Memory size of the dataset                                                                         | dataset        | `integer`                           |
| `record_size`                | Size of a record in bytes                                                                          | dataset        | `integer`                           |
| `n_cells_missing`            | Number of cells empty                                                                              | dataset        | `integer`                           |
| `n_vars_with_missing`        | Number of variables containing missing values                                                      | dataset        | `integer`                           |
| `n_vars_all_missing`         | Number of variables containing 100% missing values                                                 | dataset        | `integer`                           |
| `p_cells_missing`            | Percentage of cells empty                                                                          | dataset        | `float`                             |
| `record_size`                | Number of records                                                                                  | dataset        | `integer`                           |
| `types_unsupported`          | Number of variables with unsupported types                                                         | dataset        | `integer`                           |
| `types_numeric`              | Number of variables with numeric types                                                             | dataset        | `integer`                           |
| `types_text`                 | Number of variables with text types                                                                | dataset        | `integer`                           |
| `score`                      | Completeness Score of the dataset, Average of all var `completeness_score`                         | dataset        | `float`                             |
| `n_distinct`                 | Number of distinct values                                                                          | column         | `integer`                           |
| `n_missing`                  | Number of missing values                                                                           | column         | `integer`                           |
| `p_missing`                  | Percentage of missing values                                                                       | column         | `float`                             |
| `p_distinct`                 | Percentage of distinct values                                                                      | column         | `float`                             |
| `is_unique`                  | Is the variable unique                                                                             | column         | `boolean`                           |
| `n_unique`                   | Number of unique values                                                                            | column         | `integer`                           |
| `p_unique`                   | Percentage of unique values                                                                        | column         | `float`                             |
| `type`                       | Type of the variable                                                                               | column         | `string`                            |
| `hashable`                   | Is the variable hashable                                                                           | column         | `boolean`                           |
| `value_counts_without_nan`   | Value counts without NaN                                                                           | column         | `dict` of value:count               |
| `value_counts_index_sorted`  | `value_counts_without_nan` index sorted                                                            | column         | `dict` of value:count               |
| `ordering`                   | Ordering of the variable    1: ASC / 0: DESC                                                       | column         | `boolean`                           |
| `completeness_score`         | Completeness score of the variable                                                                 | column         | `float`                             |
| `n_negative`                 | Number of negative values                                                                          | column:Numeric | `integer`                           |
| `p_negative`                 | Percentage of negative values                                                                      | column:Numeric | `float`                             |
| `n_zeros`                    | Number of zero values                                                                              | column:Numeric | `integer`                           |
| `p_zeros`                    | Percentage of zero values                                                                          | column:Numeric | `float`                             |
| `n_infinite`                 | Number of infinite values                                                                          | column:Numeric | `integer`                           |
| `p_infinite`                 | Percentage of infinite values                                                                      | column:Numeric | `float`                             |
| `mean`                       | Mean of the variable                                                                               | column:Numeric | `float`                             |
| `min`                        | Minimum value of the variable                                                                      | column:Numeric | `float`                             |
| `max`                        | Maximum value of the variable                                                                      | column:Numeric | `float`                             |
| `range`                      | Range of the variable                                                                              | column:Numeric | `float`                             |
| `sum`                        | Sum of the variable                                                                                | column:Numeric | `float`                             |
| `5%`                         | 5th percentile of the variable                                                                     | column:Numeric | `float`                             |
| `25%`                        | 25th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `50%`                        | 50th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `75%`                        | 75th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `95%`                        | 95th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `std`                        | [Standard deviation](https://en.wikipedia.org/wiki/Standard_deviation) of the variable             | column:Numeric | `float`                             |
| `variance`                   | [Variance](https://en.wikipedia.org/wiki/Variance) of the variable                                 | column:Numeric | `float`                             |
| `mad`                        | [Mean absolute deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation) of the variable | column:Numeric | `float`                             |
| `skewness`                   | [Skewness](https://en.wikipedia.org/wiki/Skewness) of the variable                                 | column:Numeric | `float`                             |
| `kurtosis`                   | [Kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of the variable                                 | column:Numeric | `float`                             |
| `iqr`                        | [Interquartile range](https://en.wikipedia.org/wiki/Interquartile_range) of the variable           | column:Numeric | `float`                             |
| `cv`                         | [Coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) of the variable | column:Numeric | `float`                             |
| `monotonic`                  | Is the variable [monotonic](https://en.wikipedia.org/wiki/Monotonic_function)                      | column:Numeric | `boolean`                           |
| `monotonic_increase`         | Is the variable monotonic increasing                                                               | column:Numeric | `boolean`                           |
| `monotonic_decrease`         | Is the variable monotonic decreasing                                                               | column:Numeric | `boolean`                           |
| `monotonic_increase_strict`  | Is the variable strictly monotonic increasing                                                      | column:Numeric | `boolean`                           |
| `monotonic_decrease_strict`  | Is the variable strictly monotonic decreasing                                                      | column:Numeric | `boolean`                           |
| `histogram`                  | Histogram of the variable                                                                          | column:Numeric | `dict` {counts[], bin_edges[]}      |
| `n_category`                 | Number of categories                                                                               | column:Text    | `integer`                           |
| `word_counts`                | Word counts                                                                                        | column:Text    | `dict` of word:count                |
| `n_distinct`                 | Number of distinct values                                                                          | column:Text    | `integer`                           |
| `p_distinct`                 | Percentage of distinct values                                                                      | column:Text    | `float`                             |
| `first_rows`                 | First rows of the variable                                                                         | column:Text    | `dict` rowId:value                  |
| `category_alias_values`      | Alias values of the variable                                                                       | column:Text    | `dict` of value:ALIAS               |
| `category_alias_char_counts` | Alias character counts of the variable                                                             | column:Text    | `dict` of ALIAS:dict of char_count  |
| `max_length`                 | Maximum length of the variable                                                                     | column:Text    | `integer`                           |
| `min_length`                 | Minimum length of the variable                                                                     | column:Text    | `integer`                           |
| `mean_length`                | Mean length of the variable                                                                        | column:Text    | `float`                             |
| `median_length`              | Median length of the variable                                                                      | column:Text    | `float`                             |
| `histogram_length`           | Histogram of the variable length                                                                   | column:Text    | `dict` {counts[], bin_edges[]}      |
| `n_scripts`                  | Number of scripts                                                                                  | column:Text    | `integer`                           |
| `script_counts`              | Script counts    EX: `{'Latin': 918}`                                                              | column:Text    | `dict` of script:count              |
| `script_char_counts`         | Script character counts    EX: `{'Latin': {'a': 918}}`                                             | column:Text    | `dict` of script:dict of char_count |

### Outputs
- Files per dataset:
  - `{dataset_name}_report.html`
  - `{dataset_name}_report.json`
  - If file source (single dataset): `{YYYYMMDD}_profiling_report_{source_name}.html` saved next to the source file
- JSON artifacts:
  - `metrics.json`: includes dataset- and column-scoped metrics (e.g., `completeness_score`, `p_cells_missing`, `score`)
  - `recommendations.json`: alerts parsed from profiling report with levels and scopes
  - `schemas.json`: dataset and column entries

### Multi-table handling and scopes
- When multiple tables are returned, each table is treated as a separate dataset. Names come from `table_or_query` (if a list) or default to `{source_name}_{index}`. Dataset metrics include a `parent_scope` when the source is a database.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
