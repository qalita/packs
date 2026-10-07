"""Which source object holds which codoc table.

qalita_core names every loaded object ``<source type>_<slug of the name>``
(``postgresql_public_dwh_patient``, ``file_dwh_patient``...), so a codoc table
is recognised by its name being the object key's trailing ``_``-separated
part. The longest table name wins: ``file_dwh_thesaurus_data`` is
``dwh_thesaurus_data``, not ``dwh_data``.

Two qalita_core 2.1 gaps are worked around here rather than inherited:

* a directory is not a multi-table source -- ``FolderSource`` is not
  implemented and ``FileSource`` on a directory loads its first file only --
  so each file of a directory is loaded on its own;
* ``Pack.cleanup`` deletes whatever ``paths_source`` holds at exit, which is
  only the *last* ``load_data`` call's parts (earlier tables leak) and, for a
  parquet file source, the user's own file. :func:`cleanup_list` hands it the
  staged parts of every table and never a source file.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

DATA_FILE_SUFFIXES = (".csv", ".xlsx", ".parquet", ".pq")


def match_table(object_key: str, table_names: Iterable[str]) -> Optional[str]:
    key = object_key.lower()
    candidates = [
        name for name in table_names if key == name or key.endswith("_" + name)
    ]
    return max(candidates, key=len) if candidates else None


def cleanup_list(loaded: Dict[str, List[str]], source_files=()) -> List[str]:
    """Staged parquet parts of every table, minus the source's own files."""
    protected = {os.path.realpath(path) for path in source_files}
    return [
        path
        for parts in loaded.values()
        for path in parts
        if os.path.realpath(path) not in protected
    ]


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


def load_database_tables(pack, table_names: List[str]) -> Dict[str, List[str]]:
    """``{codoc table: parquet parts}`` for a database source.

    ``load_data`` runs once per table: one unreadable table must not abort
    the others, and its absence is a finding, not an error.
    """
    listed = _database_table_names(pack.source_config)
    loaded: Dict[str, List[str]] = {}
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
            loaded[name] = list(paths)
    pack.paths_source = cleanup_list(loaded)
    return loaded


def _load_one_file(pack, path: str) -> List[str]:
    """Load a single file through qalita_core, whatever the source says."""
    original = pack.source_config
    config = dict(original.get("config") or {}, path=path)
    pack.source_config = dict(original, type="file", config=config)
    try:
        return list(pack.load_data("source"))
    finally:
        pack.source_config = original


def load_file_tables(
    pack, table_names: List[str], declared_table: Optional[str] = None
) -> Tuple[Dict[str, List[str]], List[str]]:
    """``({codoc table: parquet parts}, [unmatched file names])``.

    A single file can be named anything: ``declared_table`` (job.table) says
    which codoc table it holds, and wins over its name. In a directory each
    file is matched by name; unmatched files are not even loaded.
    """
    path = Path((pack.source_config.get("config") or {}).get("path", ""))
    if path.is_dir():
        files = sorted(
            str(p)
            for p in path.iterdir()
            if p.is_file() and p.suffix.lower() in DATA_FILE_SUFFIXES
        )
    else:
        files = [str(path)]

    loaded: Dict[str, List[str]] = {}
    unmatched = []
    for file in files:
        stem = Path(file).stem
        if declared_table and len(files) == 1:
            table = declared_table
        else:
            table = match_table(stem, table_names)
        if table is None:
            unmatched.append(Path(file).name)
            logger.warning("%s matches no codoc table; ignored", file)
        elif table in loaded:
            logger.warning("%s is a second %s; ignored", file, table)
        else:
            loaded[table] = _load_one_file(pack, file)
    pack.paths_source = cleanup_list(loaded, files)
    return loaded, unmatched
