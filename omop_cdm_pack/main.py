"""QALITA pack entry point: OMOP CDM data quality assessment.

Wires the pipeline built in Tasks 1-11 (catalog -> loader -> runner ->
reporting) to the QALITA platform's Pack contract: read job config,
materialise the CDM tables from the configured source, run every
applicable check, and push metrics/recommendations back to the
platform.
"""

import logging
from dataclasses import replace

from qalita_core.pack import Pack

import omop_dqd.checks  # noqa: F401  (registers every check)
from omop_dqd.catalog import load_catalog
from omop_dqd.loader import load_cdm_tables
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with Pack() as pack:
    job = pack.pack_config.get("job", {})
    cdm_version = str(job.get("cdm_version", "5.4"))
    excluded_tables = job.get("excluded_tables", [])
    dataset_label = pack.source_config.get("name", "omop_cdm")

    context = load_cdm_tables(pack, cdm_version, excluded_tables)
    logger.info(
        "loaded %d CDM tables (vocabulary: %s)",
        len(context.available_tables),
        "yes" if context.has_vocabulary else "no",
    )

    catalog = load_catalog(cdm_version)
    overrides = job.get("threshold_overrides", {})
    if overrides:
        catalog = [
            (
                replace(check, threshold=float(overrides[check.check_name]))
                if check.check_name in overrides
                else check
            )
            for check in catalog
        ]

    results = run_checks(context, catalog)

    tally = {}
    for evaluated in results:
        tally[evaluated.result.status] = (
            tally.get(evaluated.result.status, 0) + 1
        )
    logger.info(
        "checks: %d passed, %d failed, %d not applicable, %d errored",
        tally.get(CheckStatus.PASS, 0),
        tally.get(CheckStatus.FAIL, 0),
        tally.get(CheckStatus.NOT_APPLICABLE, 0),
        tally.get(CheckStatus.ERROR, 0),
    )

    pack.metrics.data = build_metrics(results, dataset_label)
    pack.recommendations.data = build_recommendations(results, dataset_label)

    pack.metrics.save()
    pack.recommendations.save()
