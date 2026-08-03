"""QALITA pack entry point: OMOP CDM data quality assessment.

Wires the pipeline built in Tasks 1-11 (catalog -> loader -> runner ->
reporting) to the QALITA platform's Pack contract: read job config,
materialise the CDM tables from the configured source, run every
applicable check, and push metrics, recommendations and schemas back
to the platform.
"""

import logging
from dataclasses import replace

from qalita_core.pack import Pack

import omop_dqd.checks  # noqa: F401  (registers every check)
from omop_dqd.catalog import load_catalog
from omop_dqd.loader import load_cdm_tables
from omop_dqd.registry import registered_names
from omop_dqd.reporting import (
    build_metrics,
    build_recommendations,
    build_schemas,
)
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _validated_overrides(raw):
    """job.threshold_overrides, or a clear error explaining why not.

    Both failure modes are silent otherwise: a misspelled check name
    matches nothing and the intended threshold simply never applies,
    and a non-numeric value raises a bare ValueError from float() with
    no indication of which key produced it.
    """
    overrides = {}
    known = registered_names()
    for check_name, value in dict(raw).items():
        if check_name not in known:
            raise ValueError(
                f"threshold_overrides names unknown check "
                f"{check_name!r}; known checks: "
                f"{', '.join(sorted(known))}"
            )
        try:
            overrides[check_name] = float(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"threshold_overrides[{check_name!r}] must be a number, "
                f"got {value!r}"
            ) from None
    return overrides


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
    overrides = _validated_overrides(job.get("threshold_overrides", {}))
    if overrides:
        catalog = [
            (
                replace(check, threshold=overrides[check.check_name])
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
    # Without schemas the platform has no table/column tree for the
    # table- and column-scoped metrics above to attach to -- see
    # reporting.build_schemas.
    pack.schemas.data = build_schemas(results, dataset_label)

    pack.metrics.save()
    pack.recommendations.save()
    pack.schemas.save()
