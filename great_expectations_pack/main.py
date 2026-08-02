"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Run a Great Expectations suite over the parquet staging, in DuckDB.

Two things were wrong with the previous implementation.

It did not run at all: it imported
``great_expectations.dataset.PandasDataset``, the V2 API that was removed in
Great Expectations 1.0, while the lockfile pins 1.9.0. Every job raised
``ModuleNotFoundError`` on line 3.

And the shape of it could not have worked on a large source anyway. It called
``pd.read_parquet`` on every part file, so the memory needed was the size of
the dataset, and it paired part files with configured table names using
``zip``, so on a chunked source parts 2..N were dropped or relabelled.

Now every expectation that has a SQL implementation runs inside DuckDB over a
view spanning ALL parts of a logical object. DuckDB streams and spills to disk,
so the memory used is bounded by DuckDB's buffer pool rather than by the row
count. The handful of expectations GX cannot express in SQL run on a bounded
uniform sample and say so, in ``expectation_result_method``.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile

import great_expectations as gx
from great_expectations.expectations.registry import get_expectation_impl

import duckdb_view
import gx_duckdb
import gx_results
from qalita_core import analytics
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("great_expectations_pack")

# Expectations GX 1.x cannot evaluate through its SQLAlchemy engine, verified
# against great-expectations 1.9.0 / duckdb-engine 0.17.0. Two causes:
#
#   - no SqlAlchemyExecutionEngine provider is registered for the metric at all
#     (the ordering and parsing families);
#   - the regex and LIKE helpers have no DuckDB branch, and the PostgreSQL one
#     cannot be borrowed because DuckDB's `~` is a full match where
#     PostgreSQL's is a partial match — see gx_duckdb.
#
# These are the only expectations evaluated on a sample. Everything else reads
# the whole dataset.
SAMPLE_ONLY_EXPECTATIONS = frozenset(
    {
        "expect_column_values_to_be_dateutil_parseable",
        "expect_column_values_to_be_decreasing",
        "expect_column_values_to_be_increasing",
        "expect_column_values_to_be_json_parseable",
        "expect_column_values_to_match_json_schema",
        "expect_column_values_to_match_like_pattern",
        "expect_column_values_to_match_like_pattern_list",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_match_regex_list",
        "expect_column_values_to_match_strftime_format",
        "expect_column_values_to_not_match_like_pattern",
        "expect_column_values_to_not_match_like_pattern_list",
        "expect_column_values_to_not_match_regex",
        "expect_column_values_to_not_match_regex_list",
    }
)

DEFAULT_SAMPLE_ROWS = 100_000
DEFAULT_EXAMPLE_LIMIT = 10


with Pack() as pack:
    if pack.source_config.get("type") == "database":
        table_or_query = pack.source_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type source, you must specify "
                "'table_or_query' in the config."
            )
        pack.load_data("source", table_or_query=table_or_query)
    else:
        pack.load_data("source")

    job = pack.pack_config.get("job", {}) or {}
    suite_name = job.get("suite_name", "qalita_default_suite")
    expectations = job.get("expectations", []) or []
    sample_rows = int(job.get("sample_rows", DEFAULT_SAMPLE_ROWS))
    example_limit = min(
        int(job.get("failed_rows_limit", DEFAULT_EXAMPLE_LIMIT)),
        gx_results.MAX_EXAMPLE_LIMIT,
    )

    tables = pack.tables("source")
    logger.info(
        "Suite '%s': %d expectation(s) over %d object(s)",
        suite_name,
        len(expectations),
        len(tables),
    )

    # A single-object source keeps the source name as its scope value, which is
    # what the platform already shows for this pack. With several objects the
    # object name is the only label that tells them apart.
    def _label(table: str) -> str:
        if len(tables) == 1:
            return pack.source_config.get("name") or table
        return table

    # GX reconnects on its own through SQLAlchemy, and a view created in an
    # in-memory DuckDB is invisible to any other connection — hence a file.
    workdir = tempfile.mkdtemp(prefix="qalita_gx_")
    database = os.path.join(workdir, "staging.duckdb")

    try:
        writer = duckdb_view.connect(database)
        try:
            views = duckdb_view.create_views(writer, pack.objects_source)
        finally:
            # Released before GX opens the same file read-only: DuckDB refuses
            # to attach one database twice with different access modes.
            writer.close()

        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_sql(
            name="qalita_duckdb",
            connection_string="duckdb://",
            kwargs=gx_duckdb.connection_kwargs(database),
        )

        for table in tables:
            label = _label(table)
            row_count = pack.get_row_count("source", table)
            batch = (
                datasource.add_table_asset(name=table, table_name=views[table])
                .add_batch_definition_whole_table(f"{table}_whole")
                .get_batch()
            )

            # Built on first use only: a suite made solely of SQL-capable
            # expectations must not pay for a sample it never reads.
            sampled_batch = None

            def _sample(table=table):
                frame = analytics.sample(
                    pack.scan("source", table),
                    n=sample_rows,
                    total_rows=row_count,
                )
                # The documented fallback for expectations GX cannot push down
                # to SQL: the frame is already capped by analytics.sample().
                return context.data_sources.pandas_default.read_dataframe(
                    frame.to_pandas()  # streaming-ok: capped by analytics.sample
                )

            total = 0
            passed = 0
            sampled = 0
            unavailable = 0

            for expectation in expectations:
                exp_type = expectation.get("expectation_type")
                kwargs = expectation.get("kwargs", {}) or {}
                if not exp_type:
                    continue

                try:
                    impl = get_expectation_impl(exp_type)
                except Exception as exc:  # noqa: BLE001 - reported as a metric
                    logger.warning(
                        "Unknown expectation '%s': %s", exp_type, exc
                    )
                    # Counted as a configured-but-not-passed expectation: a
                    # typo in a suite must lower the score, not disappear from
                    # the denominator and leave the run looking clean.
                    total += 1
                    unavailable += 1
                    pack.metrics.data.append(
                        {
                            "key": "expectation_result",
                            "value": {
                                "expectation": exp_type,
                                "success": False,
                                "error": f"unknown expectation: {exc}",
                            },
                            "scope": {"perimeter": "dataset", "value": label},
                        }
                    )
                    pack.metrics.data.append(
                        {
                            "key": "expectation_result_method",
                            "value": "unavailable",
                            "scope": {"perimeter": "dataset", "value": label},
                        }
                    )
                    pack.recommendations.data.append(
                        {
                            "content": (
                                f"'{exp_type}' is not a Great Expectations "
                                f"expectation, so it was not evaluated on "
                                f"'{label}'."
                            ),
                            "type": "Check Not Evaluated",
                            "scope": {"perimeter": "dataset", "value": label},
                            "level": "warning",
                        }
                    )
                    continue

                method = "duckdb"
                error = None
                result = None

                if exp_type not in SAMPLE_ONLY_EXPECTATIONS:
                    result = batch.validate(
                        impl(**kwargs),
                        result_format=gx_results.result_format(example_limit),
                    )
                    error = gx_results.raised_exception(result)

                # The SQL-capable list above is empirical, so a GX upgrade can
                # move an expectation out of it. Falling back keeps the check
                # answered instead of silently reported as a failure.
                if exp_type in SAMPLE_ONLY_EXPECTATIONS or error:
                    if error:
                        logger.info(
                            "'%s' has no DuckDB implementation (%s), "
                            "falling back to a %d-row sample",
                            exp_type,
                            error,
                            sample_rows,
                        )
                    if sampled_batch is None:
                        sampled_batch = _sample()
                    result = sampled_batch.validate(
                        impl(**kwargs),
                        result_format=gx_results.result_format(example_limit),
                    )
                    error = gx_results.raised_exception(result)
                    method = "sampled"

                total += 1
                if error:
                    method = "unavailable"
                    unavailable += 1
                    success = False
                else:
                    success = bool(result.success)
                    passed += 1 if success else 0
                    if method == "sampled":
                        sampled += 1

                value = {"expectation": exp_type, "success": success}
                value.update(
                    gx_results.summarize_result(result, example_limit)
                )
                if error:
                    value["error"] = error
                if method == "sampled":
                    value["sampled_rows"] = min(sample_rows, row_count)

                pack.metrics.data.append(
                    {
                        "key": "expectation_result",
                        "value": value,
                        "scope": {"perimeter": "dataset", "value": label},
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "expectation_result_method",
                        "value": method,
                        "scope": {"perimeter": "dataset", "value": label},
                    }
                )

                if error:
                    pack.recommendations.data.append(
                        {
                            "content": (
                                f"'{exp_type}' could not be evaluated on "
                                f"'{label}': {error}"
                            ),
                            "type": "Check Not Evaluated",
                            "scope": {"perimeter": "dataset", "value": label},
                            "level": "warning",
                        }
                    )
                elif not success:
                    pack.recommendations.data.append(
                        {
                            "content": (
                                f"'{exp_type}' failed on '{label}'"
                                + (
                                    " (evaluated on a sample of "
                                    f"{min(sample_rows, row_count)} rows)"
                                    if method == "sampled"
                                    else ""
                                )
                            ),
                            "type": "Expectation Failed",
                            "scope": {"perimeter": "dataset", "value": label},
                            "level": "high",
                        }
                    )

            score = 1.0 if total == 0 else passed / total
            scope = {"perimeter": "dataset", "value": label}
            pack.metrics.data.extend(
                [
                    {
                        "key": "score",
                        "value": str(round(score, 2)),
                        "scope": scope,
                    },
                    {
                        "key": "score_method",
                        "value": "sampled" if sampled else "exact",
                        "scope": scope,
                    },
                    {
                        "key": "expectations_total",
                        "value": total,
                        "scope": scope,
                    },
                    {
                        "key": "expectations_passed",
                        "value": passed,
                        "scope": scope,
                    },
                    {
                        "key": "expectations_failed",
                        "value": total - passed,
                        "scope": scope,
                    },
                    {
                        "key": "expectations_sampled",
                        "value": sampled,
                        "scope": scope,
                    },
                    {
                        "key": "expectations_unavailable",
                        "value": unavailable,
                        "scope": scope,
                    },
                    {
                        "key": "rows_analyzed",
                        "value": row_count,
                        "scope": scope,
                    },
                ]
            )

            if score < 1:
                pack.recommendations.data.append(
                    {
                        "content": (
                            f"The dataset '{label}' passed {passed}/{total} "
                            f"expectations, giving a score of "
                            f"{round(score * 100, 2)}%."
                        ),
                        "type": "Expectations Failed",
                        "scope": scope,
                        "level": determine_recommendation_level(score),
                    }
                )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    pack.metrics.save()
    pack.recommendations.save()
