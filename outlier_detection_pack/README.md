## Outlier Detection Pack

### Overview
Detects outliers in every numeric column using **streaming IQR or z-score
fences**, and reports a normality score per column and per dataset. Memory is
independent of the row count, so the pack runs on a 100 GiB source as it does
on a 10 MB one. Supports multi-table databases, chunked files and folders.

> ⚠️ **Breaking change in 3.0.0 — `normality_score` changed definition.**
> Historical values are **not comparable** with values produced by 2.x.
> See [Migrating from 2.x](#migrating-from-2x).

## Input 📥

### Configuration ⚙️

| Name                       | Type    | Required | Default | Description                                                                                                       |
| -------------------------- | ------- | -------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| `job.method`               | `str`   | no       | `iqr`   | Detector: `iqr` (Tukey fences on the quartiles) or `zscore` (fences on mean ± k·σ).                                |
| `job.iqr_multiplier`       | `float` | no       | `1.5`   | Tukey multiplier `k`: a value is an outlier outside `[Q1 - k·IQR, Q3 + k·IQR]`. Only used when `method` is `iqr`.  |
| `job.zscore_threshold`     | `float` | no       | `3.0`   | Number of standard deviations: outlier outside `[mean - k·σ, mean + k·σ]`. Only used when `method` is `zscore`.    |
| `job.exact`                | `bool`  | no       | `false` | Compute the quartiles exactly instead of from a 10 000-bucket histogram. Slower, needs scratch disk. See below.    |
| `job.normality_threshold`  | `float` | no       | `0.9`   | A column or dataset scoring below this raises a recommendation.                                                    |
| `job.example_rows`         | `int`   | no       | `10`    | Number of example outlier rows attached to `outliers_table`. Hard-capped at 1000 whatever the value.               |
| `job.id_columns`           | `list`  | no       | `[]`    | Columns used as identifiers: excluded from detection, but carried in the example rows so an outlier is traceable.  |
| `job.source.skiprows`      | `int`   | no       | `0`     | Number of rows to skip at the beginning of the file.                                                               |

#### Approximate by default

Quartiles come from a fixed-width histogram: two streaming passes and O(bins)
memory per column, whatever the row count. The absolute error on a quartile is
bounded by the bucket width, `(max - min) / 10000`. Set `job.exact` to `true`
to use exact order statistics instead.

Every metric derived from the fences carries a sibling `<key>_method` metric
naming how it was obtained — `histogram`, `exact`, or `exact` for `zscore`
(mean and standard deviation have no approximate variant here).

## Analysis 🕵️‍♂️

Two passes over the source, then one bounded pass for the evidence rows:

1. **Fences** — one `[lower, upper]` interval per numeric column, computed for
   every column in a single batched call.
2. **Counts** — the number of values outside its own fence for every column at
   once, plus the number of rows breaching at least one fence.
3. **Evidence** — at most `job.example_rows` example rows, bounded inside the
   query plan.

Passes 1 and 2 are `qalita_core.aggregation.streaming_outliers`, so this pack
and the platform agree on what an outlier is.

Columns that are entirely null get no fence, and neither do columns whose
spread is zero. That second case is a **known blind spot of the `iqr` method**:
a column that is constant except for a few extreme values has `Q1 == Q3`, the
Tukey fences collapse onto the quartile, and flagging everything that is not
exactly it would be worse than flagging nothing. Use `method: zscore` on such
columns — the standard deviation is not zero, so the extreme values are
reported. Non-numeric columns are not analysed at all.

### Metrics

| Name                             | Description                                                                                | Scope   | Type    |
| -------------------------------- | ------------------------------------------------------------------------------------------ | ------- | ------- |
| `outliers`                       | Values outside the column fence.                                                            | Column  | `int`   |
| `outliers_method`                | How the fence was obtained: `histogram`, `exact`.                                           | Column  | `str`   |
| `normality_score`                | `1 - outliers / non_null_values`. 1.0 means no outlier.                                     | Column  | `float` |
| `normality_score_method`         | How the score was obtained: `histogram`, `exact`.                                           | Column  | `str`   |
| `outlier_lower_bound`            | Lower fence. Absent when the column got none (all-null, zero spread). **New in 3.0.0.**     | Column  | `float` |
| `outlier_upper_bound`            | Upper fence. Absent when the column got none (all-null, zero spread). **New in 3.0.0.**     | Column  | `float` |
| `outliers`                       | **Redefined in 3.0.0** — rows breaching at least one fence (was: multivariate KNN count).   | Dataset | `int`   |
| `outlier_rows`                   | Same value under an unambiguous name. **New in 3.0.0.**                                     | Dataset | `int`   |
| `outliers_method`                | How the fences were obtained.                                                               | Dataset | `str`   |
| `outlier_method`                 | Detector used: `iqr` or `zscore`. **New in 3.0.0.**                                         | Dataset | `str`   |
| `normality_score_dataset`        | `1 - outlier_rows / rows`.                                                                  | Dataset | `float` |
| `normality_score_dataset_method` | How the score was obtained.                                                                 | Dataset | `str`   |
| `score`                          | `normality_score_dataset` as a string, for the platform score column.                       | Dataset | `str`   |
| `total_outliers_count`           | Sum of the per-column `outliers`. Unchanged definition.                                     | Dataset | `int`   |
| `n`                              | Row count. **New in 3.0.0.**                                                                | Dataset | `int`   |
| `outliers_table`                 | Bounded example rows: `index`, id columns, `OutlierAttribute`, `value`.                     | Dataset | `table` |

### Recommendations

Type `Outliers`, level `high` / `warning` / `info` from the share of rows
involved (>50% / >30% / otherwise):

- `Column '<c>' has <n> outliers.`
- `Column '<c>' has a normality score of <p>%.` — below `normality_threshold`.
- `The dataset '<d>' has a normality score of <p>%.` — below the threshold.
- `The dataset '<d>' has a total of <n> outliers over <k> checked columns. Up to <m> example rows are attached to the 'outliers_table' metric.`

### Outputs
- `metrics.json`, `recommendations.json`.
- Example outlier rows travel in the `outliers_table` metric, bounded by
  `job.example_rows` (hard cap 1000).

### Multi-table handling and scopes
Each logical object is one dataset. A chunked source is **not** several
datasets: every part of an object is scanned as a single frame, so the counts
no longer depend on how the source happened to be split.

A single-object source keeps the **source name** as its dataset scope, exactly
as before. A multi-object source now scopes each dataset on the loader's object
name (`<dialect>_<schema>_<table>`) instead of the raw `table_or_query` entry —
the mapping is recorded at load time rather than re-derived by zipping table
names against parquet paths, which is what used to drop parts 2..N of a
chunked table.

## Migrating from 2.x

2.x fitted a pyod KNN — a scikit-learn `NearestNeighbors` — per column, and a
second one over a one-hot encoded copy of the whole table. Both need a dense
in-memory matrix. The per-column fit trained on the first 100 000 rows, the
multivariate fit used **no sampling at all**, and above 1 000 000 rows the pack
scored only a `head()` slice of the source. On anything large it was a memory
wall, and on a chunked source it described the first chunk.

What changed, concretely:

- **`normality_score` is not comparable across the upgrade.** It was
  `mean(1 - knn_score / max_knn_score)` — a distance-derived quantity with no
  unit, sensitive to the KNN sample. It is now `1 - outliers / non_null`, the
  share of values inside the fence. Do not read a jump at 3.0.0 as a change in
  data quality. Historical series should be cut at the upgrade.
- **`outliers` at dataset scope is redefined.** It was the multivariate KNN
  count; it is now the number of rows breaching at least one column fence.
  Read `outlier_rows` instead — same value, unambiguous name.
- **The multivariate detector is dropped, not sampled.** A dense one-hot matrix
  of a 100 GiB table cannot be built, and fitting it on a sample would produce
  a score whose value depends on the draw, cannot be reproduced between runs
  and cannot be compared across chunkings. A row-level union of univariate
  fences is exact, streams, and is explainable — you can point at the column
  and the value that made the row an outlier. Genuinely multivariate detection
  (covariance-based) is a separate pack, not a hidden sample here.
- **`job.outlier_threshold` is gone.** It thresholded the KNN inlier score,
  which no longer exists. Use `job.iqr_multiplier` or `job.zscore_threshold`.
- **The Excel report is gone.** `{YYYYMMDD}_outlier_detection_report_*.xlsx`
  contained every outlier row, which is unbounded by construction — on a large
  source that file is the memory wall the rest of this rewrite removes. Bounded
  example rows are in the `outliers_table` metric instead.
- **Dependencies dropped**: `pyod`, `numpy`, `pandas`, `matplotlib`, `seaborn`,
  `statsmodels`, `xlsxwriter`. `scikit-learn` was imported by the pack but never
  declared — it only resolved transitively through `pyod`.
- **Only numeric columns are analysed.** 2.x one-hot encoded low-cardinality
  categoricals into the multivariate fit; with that fit gone, they are ignored.

### Tests

```bash
cd outlier_detection_pack && python -m pytest tests/ -v
```

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
