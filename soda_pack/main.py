"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Run SodaCL checks over the parquet staging, in DuckDB.

The previous implementation drove the scan with ``scan.add_pandas_dataframe``.
That is soda-core's only in-memory-frame entry point, it comes from the
separate soda-core-pandas-dask plugin, and it needs a fully materialized pandas
frame before dask ever sees it — so the memory needed was the size of the
dataset, and a source larger than the worker could not be checked at all.

soda-core-duckdb runs the same SodaCL checks as SQL. DuckDB spills aggregates
and sorts to disk, so the checks now cover every row with bounded memory, and
soda-core-pandas-dask, dask and pandas all leave the dependency set.

It also fixes a mispairing. The old code zipped the configured table names with
the parquet paths; when the lengths disagreed — which is exactly what chunking
causes — its guard fell through to labelling each CHUNK as its own dataset, so
a single table split into four parts was reported as four datasets, each with
its own score. Datasets now come from ``pack.tables()`` and each view spans all
the parts of its object.
"""

from __future__ import annotations

import logging

import duckdb_view
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level
from soda.scan import Scan
from soda_metrics import DATA_SOURCE_NAME, column_aliases, metric_column

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("soda_pack")


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

    tables = pack.tables("source")
    logger.info("Scanning %d dataset(s): %s", len(tables), ", ".join(tables))

    for table in tables:
        # A single-object source keeps the source name as its scope value,
        # which is what the platform already shows for this pack. With several
        # objects the object name is the only label that tells them apart.
        dataset_label = (
            pack.source_config.get("name") or table
            if len(tables) == 1
            else table
        )

        aliases = column_aliases(pack.schema("source", table).keys())
        column_name_association = {
            alias: column for column, alias in aliases.items()
        }
        logger.info("[%s] columns: %s", dataset_label, list(aliases.values()))

        # One connection per dataset, holding exactly one view: the SodaCL
        # "include %" in checks.yaml then resolves to this dataset alone.
        connection = duckdb_view.connect()
        try:
            duckdb_view.create_view(
                connection,
                table,
                pack.objects_source[table],
                columns=aliases,
            )
            row_count = duckdb_view.view_row_count(connection, table)

            scan = Scan()
            scan.set_data_source_name(DATA_SOURCE_NAME)
            scan.add_duckdb_connection(
                duckdb_connection=connection,
                data_source_name=DATA_SOURCE_NAME,
            )
            scan.add_sodacl_yaml_files("checks.yaml")
            scan.execute()

            if scan.has_error_logs():
                logger.error(
                    "[%s] Soda reported errors:\n%s",
                    dataset_label,
                    scan.get_error_logs_text(),
                )

            results = scan.get_scan_results()
        finally:
            connection.close()

        checks = results["checks"]

        # ---------------------------- Metrics
        for metric in results["metrics"]:
            metric_name = metric["metricName"]
            column_slug = metric_column(metric["identity"], table, metric_name)
            if column_slug is None:
                scope = {"perimeter": "dataset", "value": dataset_label}
            else:
                scope = {
                    "perimeter": "column",
                    "value": column_name_association.get(
                        column_slug, column_slug
                    ),
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_label,
                    },
                }

            pack.metrics.data.append(
                {
                    "key": metric_name,
                    "value": metric["value"],
                    "scope": scope,
                }
            )

        total_checks = len(checks)
        total_pass_count = sum(
            1 for check in checks if check["outcome"] == "pass"
        )

        dataset_score = (
            total_pass_count / total_checks if total_checks > 0 else 0
        )
        logger.info(
            "[%s] %d/%d checks passed over %d rows (score %.2f)",
            dataset_label,
            total_pass_count,
            total_checks,
            row_count,
            dataset_score,
        )

        dataset_scope = {"perimeter": "dataset", "value": dataset_label}
        pack.metrics.data.extend(
            [
                {
                    "key": "score",
                    "value": round(dataset_score, 2),
                    "scope": dataset_scope,
                },
                {
                    # Every check ran in DuckDB over the whole view, so no
                    # figure here is an estimate. The UI labels it as such.
                    "key": "score_method",
                    "value": "exact",
                    "scope": dataset_scope,
                },
                {
                    "key": "check_passed",
                    "value": total_pass_count,
                    "scope": dataset_scope,
                },
                {
                    "key": "check_failed",
                    "value": (total_checks - total_pass_count),
                    "scope": dataset_scope,
                },
                {
                    "key": "rows_analyzed",
                    "value": row_count,
                    "scope": dataset_scope,
                },
            ]
        )

        # Per-column score
        column_pass_count: dict[str, int] = {}
        column_total_checks: dict[str, int] = {}
        for check in checks:
            column = check.get("column") or "dataset"
            column_pass_count[column] = column_pass_count.get(column, 0) + (
                1 if check["outcome"] == "pass" else 0
            )
            column_total_checks[column] = (
                column_total_checks.get(column, 0) + 1
            )

        for column, total in column_total_checks.items():
            pass_count = column_pass_count.get(column, 0)
            score = pass_count / total if total > 0 else 0
            column_name = (
                column.replace('"', "")
                if column != "dataset"
                else dataset_label
            )
            original_column_name = column_name_association.get(
                column_name, column_name
            )

            pack.metrics.data.append(
                {
                    "key": "check_completion_score",
                    "value": round(
                        score if column != "dataset" else dataset_score, 2
                    ),
                    "scope": {
                        "perimeter": (
                            "column" if column != "dataset" else "dataset"
                        ),
                        "value": original_column_name,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_label,
                        },
                    },
                }
            )

        # ---------------------------- Recommendations
        if dataset_score < 1:
            pack.recommendations.data.append(
                {
                    "content": (
                        f"The dataset '{dataset_label}' has PASSED "
                        f"{total_pass_count}/{total_checks} checks giving a "
                        f"score of {dataset_score * 100}%."
                    ),
                    "type": "Checks Failed",
                    "scope": dataset_scope,
                    "level": determine_recommendation_level(dataset_score),
                }
            )

        for check in checks:
            if check["outcome"] == "pass":
                continue
            if check["column"] is not None:
                pack.recommendations.data.append(
                    {
                        "content": check["definition"],
                        "type": check["name"],
                        "scope": {
                            "perimeter": "column",
                            "value": column_name_association.get(
                                check["column"], check["column"]
                            ),
                            "parent_scope": {
                                "perimeter": "dataset",
                                "value": dataset_label,
                            },
                        },
                        "level": "high",
                    }
                )
            else:
                pack.recommendations.data.append(
                    {
                        "content": check["definition"],
                        "type": "Checks Failed",
                        "scope": dataset_scope,
                        "level": "high",
                    }
                )

    pack.recommendations.save()
    pack.metrics.save()
