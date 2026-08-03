# Pattern Validation Pack

Validates data formats using predefined patterns (email, UUID, IP, URL) and custom regex rules.

##  Checks Covered

- `invalid_email_format_found` / `invalid_email_format_percent`
- `invalid_uuid_format_found` / `invalid_uuid_format_percent`
- `invalid_ip4_address_format_found` / `invalid_ip6_address_format_found`
- `text_not_matching_regex_found` / `texts_not_matching_regex_percent`
- `text_not_matching_date_pattern_found`

## How it works

Data is read through `pack.scan()` as a Polars LazyFrame and matching runs
inside the engine as a vectorized expression. Every rule of every column is
folded into ONE streaming aggregation per dataset, so the source is read once
whatever the number of patterns.

Patterns keep `re.match` semantics: they are evaluated as `^(?:<pattern>)`, so
an alternation is anchored as a whole (`^a|b` would anchor only its first
branch and accept "xb").

Nulls are excluded from the denominator and empty strings count as valid, which
is what the previous pandas implementation did.

## Built-in Pattern Types

| Type | Description |
|------|-------------|
| `email` | Standard email format |
| `uuid` | UUID/GUID format |
| `ipv4` | IPv4 address format |
| `ipv6` | IPv6 address format |
| `url` | HTTP/HTTPS URL format |
| `phone_international` | International phone number (E.164) |
| `date_iso` | ISO date format (YYYY-MM-DD) |
| `date_us` | US date format (MM/DD/YYYY) |
| `date_eu` | European date format (DD-MM-YYYY) |
| `datetime_iso` | ISO datetime format |
| `credit_card` | Credit card number format |
| `hex_color` | Hex color code (#RGB or #RRGGBB) |
| `mac_address` | MAC address format |
| `postal_code_us` | US postal code format |
| `alphanumeric` | Alphanumeric characters only |

## Configuration

```json
{
  "job": {
    "patterns": [
      {"column": "email", "type": "email"},
      {"column": "user_id", "type": "uuid"},
      {"column": "ip_address", "type": "ipv4"},
      {"column": "custom_code", "type": "regex", "regex": "^[A-Z]{2}\\d{4}$"}
    ],
    "id_columns": ["user_id"],
    "examples": true,
    "example_rows": 10,
    "example_max_checks": 20,
    "sample_rows": 100000
  }
}
```

- `id_columns` (list, default `[]`) — columns added to the example rows so a failing value can be traced back to a record.
- `examples` (bool, default `true`) — emit bounded failing-row examples.
- `example_rows` (int, default 10, hard cap 1000) — rows per failing check.
- `example_max_checks` (int, default 20) — how many checks may emit examples; each example set costs one extra filtered pass over the source.
- `sample_rows` (int, default 100000) — sample size used by the fallback below.

## Backreferences and look-around

Polars matches with the Rust regex crate, which supports neither backreferences
nor look-around. A configured pattern using either cannot run in the engine.
Rather than failing the job, the pack detects it up front (the pattern is
probed on a one-row frame) and falls back to Python's `re` on a bounded uniform
sample of `sample_rows` rows. The resulting counts are estimates and are
labelled as such: every metric is emitted with a sibling `<key>_method` whose
value is `exact` or `sampled_python_regex`.

## Metrics Output

- `invalid_<pattern>_format_found`: Count of invalid values
- `invalid_<pattern>_format_percent`: Percentage of invalid values
- `valid_<pattern>_percent`: Percentage of valid values
- `invalid_format_examples`: Bounded list of rows failing the pattern
- `<key>_method`: how the value above was obtained (`exact` / `sampled_python_regex`)
- `score`: Overall validity score (0-1)

## Changes in 0.2.0

- Streaming rewrite: `pandas`, the per-part `read_parquet` preamble, and the per-row `Series.apply(compiled.match)` are gone. The pack now depends on `qalita-core>=2.0.0` and declares `polars` explicitly.
- **Breaking**: dataset scope values are now the logical object names returned by `pack.tables("source")` (e.g. `mysource_users`) instead of `{source_name}_{index}`. A chunked source used to produce one numbered dataset per part, and every part was validated as if it were a separate table.
- New metrics: `invalid_format_examples` and the `<key>_method` siblings.
- A pattern the engine cannot compile no longer crashes the job; it degrades to a labelled sampled estimate.
- Columns with a nested dtype (list/struct) are skipped instead of being stringified.

## Auto-Detection

If no patterns are configured, the pack will auto-detect and validate:
- Columns with "email" or "mail" in the name
- Columns with "uuid" or "guid" in the name
- Columns with "ip" and "address" in the name

## Tests

```bash
cd pattern_validation_pack && python -m pytest tests -q
```

## License

Proprietary - QALITA SAS
