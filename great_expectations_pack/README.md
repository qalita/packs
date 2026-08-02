## Great Expectations Pack

Runs a Great Expectations expectation suite on the dataset loaded via QALITA.

### Where the checks run

Checks execute **in DuckDB, over the parquet staging** written by
`Pack.load_data`, not in pandas. One DuckDB view is created per logical object
and it spans **every** part file of that object, so a source split into chunks
is one dataset rather than several. DuckDB evaluates filters, aggregates, joins
and sorts out-of-core, so the memory a run needs is bounded by DuckDB's buffer
pool instead of by the row count.

### This pack was broken before 0.2.0

Versions up to 0.1.21 imported `great_expectations.dataset.PandasDataset` —
the V2 API removed in Great Expectations 1.0 — while the lockfile pinned
1.9.0. Every job raised `ModuleNotFoundError: No module named
'great_expectations.dataset'` before reading a single row. If you have runs of
this pack recorded as failures, that is why.

The same code also read every parquet part with `pd.read_parquet` (so it needed
the whole dataset in RAM) and paired part files with configured table names
using `zip` (so on a chunked source parts 2..N were dropped or relabelled).

### Expectations without a SQL implementation

Great Expectations 1.x cannot evaluate every expectation through its SQLAlchemy
engine. Verified against `great-expectations 1.9.0` + `duckdb-engine 0.17.0`,
**39 of the 53** expectation types tested run in DuckDB. These **14** do not,
and are evaluated on a bounded uniform sample instead — never silently skipped:

| Expectation | Why |
| --- | --- |
| `expect_column_values_to_be_increasing` | no SQLAlchemy metric provider |
| `expect_column_values_to_be_decreasing` | no SQLAlchemy metric provider |
| `expect_column_values_to_be_dateutil_parseable` | no SQLAlchemy metric provider |
| `expect_column_values_to_be_json_parseable` | no SQLAlchemy metric provider |
| `expect_column_values_to_match_json_schema` | no SQLAlchemy metric provider |
| `expect_column_values_to_match_strftime_format` | no SQLAlchemy metric provider |
| `expect_column_values_to_match_regex` | no DuckDB branch in GX's regex helper |
| `expect_column_values_to_not_match_regex` | idem |
| `expect_column_values_to_match_regex_list` | idem |
| `expect_column_values_to_not_match_regex_list` | idem |
| `expect_column_values_to_match_like_pattern` | no DuckDB branch in GX's LIKE helper |
| `expect_column_values_to_not_match_like_pattern` | idem |
| `expect_column_values_to_match_like_pattern_list` | idem |
| `expect_column_values_to_not_match_like_pattern_list` | idem |

GX's PostgreSQL regex SQL is *not* reused for DuckDB even though duckdb-engine
derives from the PostgreSQL dialect: PostgreSQL's `~` is a partial match while
DuckDB's is a full match, so borrowing it makes every regex expectation report
100% unexpected values. See `gx_duckdb.py`.

Any expectation that unexpectedly fails to run in DuckDB (for instance after a
GX upgrade) also falls back to the sample rather than being reported as failed.

### Config (`pack_conf.json`)

- `job.suite_name`: logical name of the suite
- `job.sample_rows`: rows drawn for sampled expectations (default `100000`)
- `job.failed_rows_limit`: example values kept per failed expectation
  (default `10`, hard cap `1000`)
- `job.expectations`: list of expectations (type + kwargs), e.g.:

```json
{
  "expectation_type": "expect_table_row_count_to_be_between",
  "kwargs": {"min_value": 1}
}
```

### Metrics

| Key | Scope | Notes |
| --- | --- | --- |
| `expectation_result` | dataset | one per expectation; `{expectation, success, observed_value?, unexpected_count?, examples?, sampled_rows?, error?}` |
| `expectation_result_method` | dataset | `duckdb`, `sampled` or `unavailable`, in the same order as `expectation_result` |
| `score` | dataset | success ratio, as a string |
| `score_method` | dataset | `exact`, or `sampled` if any expectation was sampled |
| `expectations_total` | dataset | |
| `expectations_passed` | dataset | |
| `expectations_failed` | dataset | |
| `expectations_sampled` | dataset | |
| `expectations_unavailable` | dataset | expectations that could not be evaluated at all |
| `rows_analyzed` | dataset | |

Recommendations are emitted for failed and for non-evaluated expectations.

### Breaking changes in 0.2.0

- `score` is now emitted **once per dataset**, scoped to that dataset. It used
  to be a single value computed across every dataset but scoped to the first
  one.
- `expectation_result` values carry extra keys (`observed_value`,
  `unexpected_count`, `examples`, …). `expectation`/`success` are unchanged.
- Dataset scope values come from the loaded objects. A single-object source
  still uses the source name; a multi-object source now uses the object name
  instead of the raw `table_or_query` entry, which is what made parts 2..N
  mislabelled.
- New metric keys, listed above.
- `recommendations.json` is now produced (it was not before).
