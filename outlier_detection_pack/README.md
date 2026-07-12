## Outlier Detection Pack

### Overview
Identifies univariate and multivariate outliers and computes normality scores per column and dataset using PyOD KNN. Supports multi-table databases and single DataFrames.

## Input 📥

### Configuration ⚙️

| Name                   | Type   | Required | Default | Description                                                                                                                      |
| ---------------------- | ------ | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `jobs.source.skiprows` | `int`  | no       | `0`     | The number of rows to skip at the beginning of the file.                                                                         |
| `jobs.normality_threshold`  | `int`  | no       | `0.9`   | The threshold for the normality score.  If there is a proportion of outliers bellow this threshold, it creates a recommendation. |
| `jobs.id_columns`           | `list` | no       | `[]`    | The list of columns to be used as an identifier.                                                                                 |
| `jobs.outlier_threshold`    | `int`  | no       | `0.5`   | The threshold for detecting outliers based on the inlier score `inlier_score = 1 - scores / (scores.max() + epsilon)`.           |

## Analysis 🕵️‍♂️

The pack assesses the data and computes the following metrics:

* **Univariate Outlier Detection**: Examines each numeric column independently to detect outliers. It computes a normality score indicating the proportion of inliers (data points that are not outliers) in each column.
* **Multivariate Outlier Detection**: Considers the entire dataset to detect outliers, providing a holistic view of data normality. It computes a normality score for the entire dataset, giving a sense of overall data consistency.
* **Normality Scoring**: Offers a score between 0 and 100% for each column and for the entire dataset. A score of 100% indicates no detected outliers, signifying highly normal data.
* **Actionable Recommendations**: Generates recommendations when a significant number of outliers are detected in a column or across the entire dataset. These recommendations are stratified into 'high', 'warning', and 'info' levels based on the severity of the detected outliers.

| Name                      | Description                                                 | Scope   | Type    |
| ------------------------- | ----------------------------------------------------------- | ------- | ------- |
| `normality_score_dataset` | The normality score for the entire dataset.                 | Dataset | `float` |
| `score`                   | The aggregated average normality score for each column.     | Dataset | `float` |
| `normality_score`         | The normality score for each column and the entire dataset. | Column  | `float` |
| `outliers`                | The number of outliers detected in each column.             | Column  | `int`   |

### Outputs
- `metrics.json`: per-column `normality_score`, `outliers`; per-dataset `normality_score_dataset`, `score`, and `outliers_table`.
- For file sources: `{YYYYMMDD}_outlier_detection_report_{dataset}.xlsx` per dataset with detailed outliers.

### Multi-table handling and scopes
- Each table is processed as a dataset; names derive from `table_or_query` or `{source_name}_{index}`. Scopes include `parent_scope` for databases.

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita/packs.
