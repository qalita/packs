## Great Expectations Pack

Runs a Great Expectations expectations suite on the dataset loaded via QALITA.

### Config
- `job.suite_name`: logical name of the suite
- `job.expectations`: list of expectations (type + kwargs), e.g.:
```
{
  "expectation_type": "expect_table_row_count_to_be_between",
  "kwargs": {"min_value": 1}
}
```

### Metrics
- `expectation_result` per expectation
- `score` = success ratio
