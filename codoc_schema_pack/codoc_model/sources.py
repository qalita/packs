"""Which source object holds which codoc table.

qalita_core names every loaded object ``<source type>_<slug of the name>``
(``postgresql_public_dwh_patient``, ``file_dwh_patient``...), so a codoc table
is recognised by its name being the object key's trailing ``_``-separated
part. The longest table name wins: ``file_dwh_thesaurus_data`` is
``dwh_thesaurus_data``, not ``dwh_data``.

Loading itself is qalita_core's (>= 2.1.3): a ``folder`` source reads every
file of a directory as one object each, and ``Pack.cleanup`` removes the
staged parts of every ``load_data`` call without touching source files.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

FILE_PREFIX = "file_"


@dataclass
class Loaded:
    """Codoc tables found in a source, and what could not be used."""

    tables: Dict[str, List[str]] = field(default_factory=dict)
    # Objects read but named after no codoc table in scope.
    unmatched: List[str] = field(default_factory=list)
    # Objects qalita_core skipped because they could not be read.
    skipped: List[Dict[str, str]] = field(default_factory=list)


def match_table(object_key: str, table_names: Iterable[str]) -> Optional[str]:
    key = object_key.lower()
    candidates = [
        name for name in table_names if key == name or key.endswith("_" + name)
    ]
    return max(candidates, key=len) if candidates else None


def _database_table_names(source_config: dict) -> Optional[Dict[str, str]]:
    """Lower-cased table name -> name as the database spells it, or None.

    Listing first means an absent table is skipped quietly instead of costing
    a failed ``SELECT`` (and its traceback in the job log), and an Oracle or
    mixed-case name is found without guessing.
    """
    try:
        from qalita_core.data_source_opener import get_data_source
        from sqlalchemy import inspect

        engine = get_data_source(source_config).engine
        schema = (source_config.get("config") or {}).get("schema")
        names = inspect(engine).get_table_names(schema=schema)
    except Exception as exc:  # noqa: BLE001 - listing is an optimisation
        logger.info("could not list tables, probing each one: %s", exc)
        return None
    return {name.lower(): name for name in names}


def load_database_tables(pack, table_names: List[str]) -> Loaded:
    """Codoc tables of a database source.

    ``load_data`` runs once per table: one unreadable table must not abort
    the others, and its absence is a finding, not an error.
    """
    listed = _database_table_names(pack.source_config)
    loaded = Loaded()
    for name in table_names:
        if listed is None:
            stored = name
        elif name in listed:
            stored = listed[name]
        else:
            continue
        try:
            paths = pack.load_data("source", table_or_query=stored)
        except Exception as exc:  # noqa: BLE001 - absence is expected
            logger.info("table %s unavailable: %s", stored, exc)
            continue
        if paths:
            loaded.tables[name] = list(paths)
    return loaded


def _display_name(object_key: str) -> str:
    """``file_notes`` -> ``notes``: the name the user gave the file."""
    if object_key.startswith(FILE_PREFIX):
        return object_key[len(FILE_PREFIX) :]
    return object_key


def load_file_tables(
    pack, table_names: List[str], declared_table: Optional[str] = None
) -> Loaded:
    """Codoc tables of a file or folder source.

    A ``folder`` source holds one object per file, each matched to a codoc
    table by its name; restrict what it reads with the source's
    ``table_or_query``. A single file can be named anything: ``declared_table``
    (job.table) says which codoc table it holds, and wins over its name.
    """
    config = pack.source_config.get("config") or {}
    if pack.source_config.get("type") != "folder" and os.path.isdir(
        config.get("path") or ""
    ):
        # qalita_core would read the directory's first file only, silently.
        raise ValueError(
            f"{config['path']} is a directory: declare the source with "
            "type 'folder' so that every codoc table file in it is read."
        )

    pack.load_data("source")
    objects = dict(pack.objects_source)
    loaded = Loaded(
        skipped=[
            dict(item)
            for item in getattr(pack, "skipped_source_objects", None) or []
        ]
    )
    if declared_table and len(objects) == 1:
        loaded.tables[declared_table] = list(next(iter(objects.values())))
        return loaded

    for key, parts in objects.items():
        table = match_table(key, table_names)
        if table is None:
            loaded.unmatched.append(_display_name(key))
            logger.warning("%s matches no codoc table; ignored", key)
        elif table in loaded.tables:
            logger.warning("%s is a second %s; ignored", key, table)
        else:
            loaded.tables[table] = list(parts)
    return loaded
