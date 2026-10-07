"""QALITA pack entry point: conformance of a source to the codoc data model.

Reads job config, finds each codoc table in the source, checks it against the
model shipped in ``codoc_model/model.json`` and pushes metrics,
recommendations, schemas and figures back to the platform.
"""

import logging

import polars as pl
from qalita_core.pack import Pack

from codoc_model.checks import check_table, missing_table
from codoc_model.reporting import (
    build_metrics,
    build_recommendations,
    build_schemas,
    declare_figures,
)
from codoc_model.sources import load_database_tables, load_file_tables
from codoc_model.spec import load_model

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STRICTNESS = ("auto", "strict", "lenient")
# Source types qalita_core opens through SQLAlchemy. A database source holds
# every codoc table, so each one is looked up by name.
DATABASE_TYPES = (
    "postgresql",
    "mysql",
    "oracle",
    "mssql",
    "sqlite",
    "database",
)


def _names(job: dict, key: str, allowed, what: str) -> list:
    """job[key] as a list of names, each checked against ``allowed``."""
    values = job.get(key) or []
    if isinstance(values, str):
        values = [values]
    unknown = sorted(set(values) - set(allowed))
    if unknown:
        raise ValueError(
            f"job.{key} names unknown {what} {unknown}; "
            f"known: {', '.join(sorted(allowed))}"
        )
    return list(values)


def read_job(job: dict, model) -> dict:
    tables = _names(job, "tables", model.tables, "codoc table(s)")
    excluded = _names(job, "excluded_tables", model.tables, "codoc table(s)")
    declared = job.get("table")
    if declared is not None and declared not in model.tables:
        raise ValueError(
            f"job.table names unknown codoc table {declared!r}; "
            f"known: {', '.join(sorted(model.tables))}"
        )
    strictness = job.get("type_strictness", "auto")
    if strictness not in STRICTNESS:
        raise ValueError(
            f"job.type_strictness must be one of {STRICTNESS}, got {strictness!r}"
        )
    return {
        "tables": tables,
        "excluded": excluded,
        "table": declared,
        "extensions": _names(
            job, "extensions", model.extensions, "extension(s)"
        ),
        "strictness": strictness,
    }


def tables_in_scope(model, config: dict) -> list:
    names = config["tables"] or list(model.tables)
    return [name for name in names if name not in config["excluded"]]


if __name__ == "__main__":
    with Pack() as pack:
        model = load_model()
        config = read_job(pack.pack_config.get("job", {}), model)
        dataset = pack.source_config.get("name", "codoc")
        is_database = pack.source_config.get("type") in DATABASE_TYPES
        strict = config["strictness"] == "strict" or (
            config["strictness"] == "auto" and is_database
        )
        scope = tables_in_scope(model, config)
        logger.info(
            "codoc model %s@%s, %d table(s) in scope, %s types",
            model.repository,
            model.commit[:7],
            len(scope),
            "strict" if strict else "lenient",
        )

        unmatched = []
        if is_database:
            loaded = load_database_tables(pack, scope)
        else:
            loaded, unmatched = load_file_tables(pack, scope, config["table"])
            if not loaded:
                raise ValueError(
                    "No object of this source matches a codoc table "
                    f"({', '.join(unmatched)}). Name each file after its "
                    "codoc table (dwh_patient.csv...) or set job.table."
                )
            # A single file is one table: the others are out of scope, not
            # missing.
            if not config["tables"] and len(loaded) + len(unmatched) == 1:
                scope = list(loaded)

        results = []
        for name in scope:
            table = model.tables[name]
            if name not in loaded:
                logger.info("%s: missing", name)
                results.append(missing_table(table))
                continue
            result = check_table(
                table,
                pl.scan_parquet(loaded[name]),
                extensions=config["extensions"],
                strict_types=strict,
            )
            logger.info(
                "%s: %d rows, score %.2f", name, result.rows, result.score
            )
            results.append(result)

        pack.metrics.data = build_metrics(results, dataset)
        pack.recommendations.data = build_recommendations(
            results, dataset, unmatched
        )
        pack.schemas.data = build_schemas(results, dataset)
        declare_figures(pack.figures, results, dataset)

        pack.metrics.save()
        pack.recommendations.save()
        pack.schemas.save()
        pack.figures.save()
