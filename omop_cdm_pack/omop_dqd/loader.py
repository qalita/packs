"""Materialisation of the CDM tables the catalog needs.

qalita_core.pack.Pack.load_data(trigger, table_or_query=...) returns a
flat list of parquet paths. Passed a list of table names it returns
every table's paths concatenated with no way to tell which path
belongs to which table -- the naming convention on those paths is not
a reliable inverse. So this module calls load_data once per table and
builds the {TABLE: [paths]} mapping itself.
"""

import logging
from typing import Iterable, List, Set

from omop_dqd.catalog import load_catalog
from omop_dqd.context import VOCABULARY_TABLES, CdmContext

logger = logging.getLogger(__name__)


def catalog_table_names(cdm_version: str) -> List[str]:
    """Every CDM table the catalog references, plus the vocabulary.

    PERSON, DEATH and VISIT_OCCURRENCE are joined by field-level checks
    (plausibleAfterBirth, plausibleBeforeDeath, withinVisitDates...)
    even on CDM versions/catalog slices where no check targets them
    directly as its own cdmTableName, so they are always included.
    """
    names: Set[str] = {
        check.cdm_table_name for check in load_catalog(cdm_version)
    }
    names.update(VOCABULARY_TABLES)
    names.update({"PERSON", "DEATH", "VISIT_OCCURRENCE"})
    return sorted(names)


def load_cdm_tables(
    pack, cdm_version: str, excluded_tables: Iterable[str] = ()
) -> CdmContext:
    """Load every available CDM table into a CdmContext.

    Calls ``pack.load_data("source", table_or_query=<table>)`` once per
    table so each table's parquet paths stay attributable to it. A
    table absent from the source raises inside qalita_core; that is
    expected -- it simply means the table is not part of this CDM
    instance -- so it is caught, logged, and the loader moves on. The
    ``cdmTable`` check is what reports the absence to the user.
    """
    excluded = {name.upper() for name in excluded_tables}
    table_paths = {}
    for table_name in catalog_table_names(cdm_version):
        if table_name in excluded:
            logger.info("skipping excluded table %s", table_name)
            continue
        try:
            paths = pack.load_data("source", table_or_query=table_name)
        except Exception as exc:  # noqa: BLE001 - absence is expected
            logger.info("table %s unavailable in source: %s", table_name, exc)
            continue
        if paths:
            table_paths[table_name] = paths
    return CdmContext.from_paths(table_paths)
