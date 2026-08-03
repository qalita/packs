# Text Validation Pack

Validates text data for length constraints, word counts, and whitespace issues.

## How it works

Every rule of every text column of a dataset is compiled into a single
expression mapping and evaluated in **one** streaming pass
(`qalita_core.analytics.agg`). The source is read once, whatever the number of
columns or rules.

Text columns are identified from the Parquet footers, not from a materialized
frame: version 0.2.0 removed the eager preamble that `pd.read_parquet`-ed every
chunk of the source and kept them all alive at once.

##  Checks Covered

- `text_min_length` / `text_max_length` / `text_mean_length`
- `text_length_below_min_length` / `text_length_above_max_length`
- `text_length_in_range_percent`
- `min_word_count` / `max_word_count`
- `empty_text_found` / `whitespace_text_found`
- `null_placeholder_text_found`
- `text_surrounded_by_whitespace_found`

## Configuration

```json
{
  "job": {
    "rules": [
      {"column": "name", "min_length": 2, "max_length": 100},
      {"column": "description", "max_length": 500}
    ],
    "analyze_all_text_columns": true,
    "examples_limit": 10,
    "max_example_columns": 20
  }
}
```

- `job.rules` (list): per-column `min_length` / `max_length` constraints.
- `job.analyze_all_text_columns` (bool, default `true`): when `false`, only the
  columns named in `job.rules` are analyzed.
- `job.examples_limit` (int, default 10, hard cap 1000): number of failing rows
  reported per column. `0` disables examples.
- `job.max_example_columns` (int, default 20): how many columns get an example
  query at all.

## Null Placeholders Detected

The pack detects common null placeholder strings:
- `null`, `NULL`, `Null`
- `none`, `NONE`, `None`
- `n/a`, `N/A`, `NA`, `na`
- `nan`, `NaN`, `NAN`
- `-`, `--`, `---`
- `.`, `..`
- `undefined`, `UNDEFINED`
- `missing`, `MISSING`
- `unknown`, `UNKNOWN`
- `#N/A`, `#NA`, `#NULL!`
- `(blank)`, `(empty)`
- `<null>`, `<NULL>`

## Metrics Output

- `text_min_length`: Minimum text length in column
- `text_max_length`: Maximum text length in column
- `text_mean_length`: Average text length
- `text_length_below_min_length`: Count below minimum constraint
- `text_length_above_max_length`: Count above maximum constraint
- `text_length_in_range_percent`: Percentage within constraints
- `min_word_count`: Minimum word count
- `max_word_count`: Maximum word count
- `empty_text_found`: Count of empty strings
- `whitespace_text_found`: Count of whitespace-only strings
- `null_placeholder_text_found`: Count of null placeholders
- `text_surrounded_by_whitespace_found`: Count with leading/trailing whitespace
- `total_text_issues`: Total issues found
- `score`: Overall validity score (0-1)
- `violation_examples` (**new in 0.2.0**, column scope): up to
  `job.examples_limit` failing values per column, produced by
  `analytics.failures()` so the export is bounded by construction.

## Changes in 0.2.0

No metric key was removed or renamed and no value changed for a
single-object source. Two things do change:

- **Chunked sources are one dataset again.** The dataset scope was
  `{source_name}_{index}` — one "dataset" per Parquet chunk of the same table,
  with the metrics of the table repeated per chunk. It is now the source name,
  and the statistics are computed over the whole table rather than per chunk.
  On a chunked source `text_min_length`, `text_max_length`, `text_mean_length`,
  `min_word_count`, `max_word_count`, the counts and `score` therefore change:
  they now describe the dataset instead of its first chunk.
- **`violation_examples` is new** (see above).

`pandas` and `numpy` are no longer imported (`numpy` was imported and never
used); `polars` is now a declared dependency.

## License

Proprietary - QALITA SAS
