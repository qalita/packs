## PII Scanner Pack

Detects Personally Identifiable Information (PII) patterns and computes
sensitivity metrics.

### This pack never exports a failing row

Every other validation pack ships bounded example rows via
`analytics.failures()`. This one must not: a failing row in a PII scan is, by
definition, a row containing personal data, so exporting it would move the PII
out of the customer's perimeter and into the platform. **Counts only.** The rule
is restated in `main.py` and covered by a test.

### Metrics

| Key | Scope | Meaning |
|-----|-------|---------|
| `pii_hits` | column | Matching values in the column, across all patterns |
| `pii_hits_<pattern>` | column | Matching values for one configured pattern |
| `rows` | dataset | Rows scanned |
| `pii_rows` | dataset | Rows carrying at least one PII match |
| `pii_columns` | dataset | Columns with at least one match |
| `pii_records_ratio` | dataset | `pii_rows / rows` |

### Configuration

Patterns live in `pack_conf.json` under `job.pii_patterns`, each with a `key`
and a `regex`.

Regexes are evaluated by Polars, which uses the Rust `regex` crate: **no
lookaround and no backreferences**. A pattern using them is checked up front,
logged by name and skipped, rather than blowing up mid-scan.

### Changes in 0.2.0

- **`rows_with_pii` no longer tracks row identity.** It used to be a Python
  `set` of pandas row indices, accumulated over every (column, pattern) pair and
  then only ever passed to `len()` — roughly 4-6 GB of Python objects at 1e8
  rows, held to produce a single ratio. The rows carrying PII are now counted by
  an `any_horizontal` reduction inside the streaming engine.
- **Chunked sources are fully scanned.** The previous code zipped the
  `table_or_query` config against the loaded parquet parts, which truncated
  against the shorter list: when one table produced several parts, parts 2..N
  were silently discarded and the metrics described the first chunk only.
- `pii_hits` at column scope is now emitted **once per column**, holding the
  total across patterns. 0.1.x emitted one `pii_hits` entry per (column,
  pattern) pair, all under the same key and scope. The per-pattern detail moved
  to `pii_hits_<pattern>`.
- New keys: `rows`, `pii_rows`, `pii_hits_<pattern>`.
- Recommendations are now emitted for every column with matches — naming the
  column, never the value.
- pandas is gone from the dependencies; polars is declared explicitly.
