## Schema Scanner Pack

### Overview
Publishes the schema of every logical object of a source — column list, column
count, per-column type — and the drift hashes computed from them.

### How it works
Everything comes from `pack.schema()`, which reads the **Parquet footers only**.
No data page is touched, so the pack costs O(number of columns) in memory and
milliseconds in time whatever the dataset size.

Version 3.0.0 removed the previous implementation, which:

1. built a full `ydata-profiling` `ProfileReport` (pandas-only, needs the whole
   frame in RAM);
2. wrote it to `{dataset}_report.html`;
3. re-read that HTML and re-parsed it with `pd.read_html` into DataFrames that
   were **never used**;
4. re-read the JSON side of the same report just to recover the column names.

Nothing this pack emits ever needed any of that.

### ⚠️ Breaking change in 3.0.0 — one-time schema-drift alert

`column_type` values are now **Polars** type names instead of pandas dtype
names:

| pandas (≤ 2.0.28) | Polars (≥ 3.0.0) |
| --- | --- |
| `int64` | `Int64` |
| `float64` | `Float64` |
| `object` / `string` | `String` |
| `bool` | `Boolean` |
| `datetime64[ns]` | `Datetime` |

`column_types_hash` is derived from those strings, so **its value changes on
every source at the first run of 3.0.0**: each source will fire exactly one
false `column_types_changed` / schema-drift alert. Subsequent runs are stable
again. `column_list_hash` and `column_order_hash` are unaffected — they hash
column names only.

Parametrized dtypes are reported by their base name, so `Datetime(us)` and
`Datetime(ns)` both read as `Datetime`: the physical time unit chosen by the
Parquet writer is not a schema change the user made.

### Configuration
- `source.config.table_or_query` (string | list | `*`) for databases.

### Outputs

`schemas.json`
- `column` — one entry per column, scoped to the column with the dataset as
  parent scope.
- `dataset` — one entry per logical object (with a `database` parent scope for
  database sources).
- `database` — one entry for database sources.

`metrics.json`

| key | scope | notes |
| --- | --- | --- |
| `column_count` | dataset | |
| `row_count` | dataset | **new in 3.0.0**, from the Parquet footers |
| `column_list_hash` | dataset | md5 of the sorted column names |
| `column_order_hash` | dataset | md5 of the column names in order |
| `column_types_hash` | dataset | md5 of `name:type` pairs — **value changes in 3.0.0** |
| `types_numeric` | dataset | **new in 3.0.0**, already charted by `pack_conf.json` |
| `types_text` | dataset | **new in 3.0.0**, already charted by `pack_conf.json` |
| `types_temporal` | dataset | **new in 3.0.0** |
| `column_type` | column | **value changes in 3.0.0** |
| `type` | column | **new in 3.0.0**, mirrors `column_type` for the badge chart |

The `{dataset}_report.html` and `{dataset}_report.json` files are **no longer
produced**. Nothing downstream read them.

This pack emits no failure-row examples: every row-level example would mean
reading data pages, which is exactly the cost the pack exists to avoid.

### Multi-table handling and scopes
Datasets come from `pack.tables("source")`. A single object split into several
Parquet chunks is ONE dataset scoped to the source name — the previous code
labelled each chunk `{source_name}_{index}` and published one dataset per chunk.

### Dependencies
`ydata-profiling`, `matplotlib`, `lxml`, `html5lib`, `beautifulsoup4` and
`pandas` are gone. Dropping `ydata-profiling` also lifted the `<3.13` Python cap.

### Tests
```bash
PYTHONPATH=/path/to/core python -m pytest tests/ -q
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
