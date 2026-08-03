## Timeliness Pack

### Overview
Assesses the freshness of date columns and computes per-column and per-dataset
timeliness metrics and a dataset score. Supports multiple datasets from
databases.

### How it works
1. **Sniffing** — at most 50 non-null values per column are brought back in a
   *single* streaming pass and matched against the supported patterns.
2. **Parsing** — the formats those values actually matched are compiled into one
   `pl.coalesce([...])` expression per column.
3. **Bounds** — `min`/`max` of every date column and every year column, plus the
   count of values no supported format could parse, are computed in ONE
   streaming pass for the whole dataset.

Version 3.0.0 replaced two unbounded shapes:

- `df[column].dropna().unique()` on **every** column — an exact distinct set
  over the whole dataset, built only to look at ten values. On a high-cardinality
  column that is one Python object per distinct value.
- `pd.to_datetime(errors="coerce", format="mixed")` / `dateutil.parser.parse`,
  which need every value in memory as a pandas Series.

### ⚠️ Breaking change in 3.0.0 — supported date formats

Polars has no per-value `format="mixed"` mode, so the pack now parses an
explicit list of formats. Textual dates outside this list are **no longer
parsed** and are reported as unparsable instead of being silently coerced.

| pattern | formats tried, in order |
| --- | --- |
| `yyyy-mm-dd` | `%Y-%m-%d` |
| `yyyy/mm/dd` | `%Y/%m/%d` |
| `yyyy.mm.dd` | `%Y.%m.%d` |
| `dd-mm-yyyy` / `mm-dd-yyyy` | `%m-%d-%Y`, then `%d-%m-%Y` |
| `dd/mm/yyyy` / `mm/dd/yyyy` | `%m/%d/%Y`, then `%d/%m/%Y` |
| `dd.mm.yyyy` / `mm.dd.yyyy` | `%m.%d.%Y`, then `%d.%m.%Y` |
| `yyyy-mm-dd HH:MM:SS` (or `T`) | `%Y-%m-%d %H:%M:%S`, then `%Y-%m-%dT%H:%M:%S` |
| `yyyy` (1900 → current year) | read as a **year**, not a date |

Columns already typed `Date`/`Datetime` by the Parquet footers skip sniffing and
parsing entirely.

Month-first is tried before day-first for the ambiguous patterns, which is what
the previous pandas/`dateutil` path defaulted to. `pl.coalesce` falls through
per value, so `15/01/2024` still parses as 15 January — only genuinely ambiguous
values such as `01/02/2024` are resolved month-first.

**What was accepted before and is rejected now**: anything `dateutil` guessed at
that is not in the table above — `Jan 15, 2024`, `15 January 2024`, `2024-1-5`,
`20240115`, ISO strings with a timezone offset or fractional seconds, and Excel
serial numbers. Those values now show up in the `Unparsable Date Values`
recommendation and in `unparsed_date_examples`.

### Configuration
- `job.source.skiprows` (int, default 0)
- `job.compute_score_columns` (list, optional): subset of date columns used to
  compute the dataset score.
- `job.examples_limit` (int, default 10, hard cap 1000): number of unparsable
  values reported per column. `0` disables examples.
- `job.max_example_columns` (int, default 20): how many columns get an example
  query at all.

### Outputs

`metrics.json`

| key | scope | notes |
| --- | --- | --- |
| `earliest_date` / `latest_date` | column | unchanged |
| `days_since_earliest_date` / `days_since_latest_date` | column | unchanged |
| `earliest_year` / `latest_year` | column | unchanged |
| `days_since_earliest_year` / `days_since_latest_year` | column | unchanged |
| `timeliness_score` | column | unchanged |
| `score` | dataset | unchanged |
| `date_columns_count` | dataset | unchanged |
| `data_staleness_days` | dataset | unchanged (file/folder sources) |
| `unparsed_date_examples` | column | **new in 3.0.0**, bounded row examples |

`recommendations.json`
- `Latest Date far in the past` — unchanged.
- `Unparsable Date Values` — **new in 3.0.0**, one per column whose values no
  supported format could parse.

### Multi-table handling and scopes
Datasets come from `pack.tables("source")`. A single object split into several
Parquet chunks is ONE dataset scoped to the source name, so the after-the-fact
"are these datasets really chunks?" detection is gone: the previous code
labelled each chunk `{source_name}_{index}` and had to re-merge them.

### Tests
```bash
PYTHONPATH=/path/to/core python -m pytest tests/ -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
