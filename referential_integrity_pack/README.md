## Referential Integrity Pack

Validates foreign key relationships between tables/datasets with a streaming
anti-join: the child is scanned once per relation and the parent keys are joined
inside the engine, so nothing is materialized in Python.

### Metrics

| Key | Scope | Meaning |
|-----|-------|---------|
| `missing_foreign_keys` | child dataset | Child rows with no matching parent key |
| `checked_foreign_keys` | child dataset | Child rows examined |
| `missing_foreign_keys_ratio` | child dataset | Orphans / rows checked |
| `missing_foreign_keys_examples` | child dataset | Up to `examples` orphan key values (JSON) |
| `score` | dataset | `1 - (total orphans / total rows checked)` |
| `missing_foreign_keys_total` | dataset | Orphans across every relation |
| `checked_foreign_keys_total` | dataset | Rows checked across every relation |

### Configuration (`pack_conf.json`, under `job`)

```json
{
  "job": {
    "examples": 10,
    "relations": [
      {"parent": {"source": "source", "table": "dim_customers", "key": ["customer_id"]},
       "child":  {"source": "source", "table": "fact_orders",   "key": ["customer_id"]}}
    ]
  }
}
```

`table` is resolved against the objects actually loaded. A database source names
its objects `<dialect>_<schema>_<table>`, so `dim_customers` matches
`postgresql_public_dim_customers`. A single-object source (one file) matches
whatever the relation declares.

`examples` bounds the orphan key values reported per relation (default 10, hard
cap 1000, `0` disables).

### Changes in 0.2.0

- **`parent.table` and `child.table` are now honoured.** They were declared in
  `pack_conf.json` and never read: every relation was checked against
  `pack.df_source` (or `pack.df_target`) wholesale, i.e. against the union of
  every loaded object. Counts for any configuration with more than one table
  were wrong. Each declared table is now loaded once and scanned by name.
- **The pandas fallback is gone.** It read `parquet[0]` — the first 100k-row
  chunk — into memory, so any transient error on the streaming path silently
  replaced a correct answer with a plausible-looking wrong one. There is no
  in-memory retry: a streaming failure raises.
- The total and the orphan count now come out of **one** streamed aggregation
  per relation instead of two separate passes over the child.
- New keys: `checked_foreign_keys`, `missing_foreign_keys_ratio`,
  `missing_foreign_keys_examples`, `missing_foreign_keys_total`,
  `checked_foreign_keys_total`. `missing_foreign_keys` and `score` are unchanged
  in meaning.
- Recommendations are now emitted for relations with violations.
- pandas is gone from the dependencies; polars is declared explicitly.
