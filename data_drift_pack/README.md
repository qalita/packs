## Data Drift Pack

Monitors distribution drift between a reference dataset (source) and a current
dataset (target), for every numeric column present on both sides.

### Metrics

| Key | Scope | Meaning |
|-----|-------|---------|
| `p_value` | column | Two-sample KS p-value, derived from the binned KS distance |
| `p_value_method` | column | `binned_ks_asymptotic` |
| `ks_statistic` | column | Largest CDF gap between the two binned distributions |
| `ks_statistic_method` | column | `binned_cdf` |
| `psi` | column | Population Stability Index |
| `psi_method` | column | `binned_histogram` |
| `drift_detected` | column | `1` when the configured test declares drift |
| `drift_example_rows` | column | Up to `examples` rows of the current dataset from the bin that moved most (JSON) |
| `score` | dataset | Share of compared columns that did not drift |
| `columns_compared` | dataset | Numeric columns present and numeric on both sides |
| `drifted_columns` | dataset | How many of them drifted |
| `drift_test` | dataset | The decision rule actually applied |

### Configuration (`pack_conf.json`, under `job`)

| Key | Default | Meaning |
|-----|---------|---------|
| `drift_test` | `ks` | Decision rule: `ks` (p-value < `alpha`) or `psi` (PSI >= `psi_threshold`) |
| `bins` | `10` | Histogram buckets derived from the reference |
| `alpha` | `0.05` | Significance level for `drift_test: "ks"` |
| `psi_threshold` | `0.2` | Decision threshold for `drift_test: "psi"` |
| `exact` | `false` | Compute the reference quantiles exactly instead of from a histogram |
| `examples` | `10` | Bounded example rows per drifted column (hard cap 1000, `0` disables) |
| `example_columns` | `5` | How many of the most-drifted columns get example rows |

### Breaking changes in 0.2.0

**The statistic changed. `p_value` is no longer scipy's exact two-sample KS
test.** Absolute values will differ from 0.1.x runs; historical series for
`p_value` are not comparable across the upgrade.

- The exact KS test needs both samples sorted in memory and has no streaming or
  sketch form. It was fed `.dropna().values`, so it could only ever describe
  what fitted in RAM.
- Worse, the previous code read `paths[0]` on each side. On a chunked source it
  compared the first 100k-row chunk of the reference against the first chunk of
  the current dataset, and published the answer as if it described both
  datasets. **Results on any dataset above one chunk were wrong, not merely
  approximate.** Expect drift to be reported now where 0.1.x reported none.
- Drift is now measured from binned CDFs: bin edges come from the reference side
  alone (its quantiles, so the bins are equi-frequent on the reference), then one
  streaming pass per side counts rows per bin. Memory is O(bins) per column at
  any row count.
- `p_value` is kept, and is now obtained by feeding the binned KS distance to the
  asymptotic Kolmogorov distribution. The binned distance under-estimates the
  exact KS statistic (the supremum is only sampled at bin edges), so the p-value
  is an upper bound: the pack errs towards declaring stability, never towards a
  false drift alarm. Raise `bins` to tighten it.
- `psi` and `ks_statistic` are new and are the recommended keys going forward.
- `drift_test` was declared in `pack_conf.json` and ignored by the code. It is
  now wired up, and an unknown value raises instead of being silently dropped.
- scipy, pandas and numpy are gone from the dependencies.
