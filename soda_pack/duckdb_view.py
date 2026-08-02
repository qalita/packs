"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

DuckDB views over the parquet staging produced by ``Pack.load_data``.

Third-party check engines only accept either a whole in-memory frame or a SQL
relation. The frame path caps the pack at whatever fits in RAM; the SQL path
does not, because DuckDB executes filters, aggregates, joins and sorts
out-of-core. Pointing the engine at a view therefore lets its checks run over
the complete dataset within a fixed memory budget.

The second reason this module exists is correctness. A view is built from the
COMPLETE part list of a logical object, so a source chunked into N parquet
files stays ONE dataset. The idiom it replaces — ``zip(table_names, paths)`` —
either dropped parts 2..N or relabelled each part as its own dataset.

This file is duplicated verbatim in every pack that needs it. Packs are
packaged and released independently, so a shared import would couple their
release cycles for forty lines of SQL string building.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import duckdb

__all__ = [
    "connect",
    "create_view",
    "create_views",
    "quote_identifier",
    "quote_literal",
    "view_row_count",
]


def connect(database: str = ":memory:", *, read_only: bool = False) -> Any:
    """Open a DuckDB connection.

    ``:memory:`` is enough when the checks run in this process. A file path is
    needed when another library reconnects on its own — a view created in an
    in-memory database is invisible to any other connection.
    """
    return duckdb.connect(database=database, read_only=read_only)


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier. Object names come from source metadata, not
    from a trusted allow-list, so they are never interpolated raw."""
    return '"' + str(name).replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Quote a SQL string literal (used for file paths)."""
    return "'" + str(value).replace("'", "''") + "'"


def create_view(
    connection: Any,
    view: str,
    parts: Sequence[str],
    *,
    columns: Mapping[str, str] | None = None,
) -> str:
    """Expose every parquet part of one logical object as a single view.

    ``read_parquet`` is given the WHOLE part list rather than one file: DuckDB
    then treats the chunks as one relation and answers a check over all of
    them, which is the property the previous per-chunk handling lost.

    Args:
        connection: an open DuckDB connection.
        view: name of the view to create.
        parts: every parquet file belonging to the object. Must not be empty.
        columns: optional ``{source_column: alias}`` projection, used when the
            check engine needs slugified column names.

    Returns:
        The view name, for chaining.
    """
    files = [str(part) for part in parts]
    if not files:
        raise ValueError(f"no parquet part to expose as view {view!r}")

    if columns:
        projection = ", ".join(
            f"{quote_identifier(source)} AS {quote_identifier(alias)}"
            for source, alias in columns.items()
        )
    else:
        projection = "*"

    file_list = ", ".join(quote_literal(path) for path in files)
    connection.execute(
        f"CREATE OR REPLACE VIEW {quote_identifier(view)} AS "
        f"SELECT {projection} FROM read_parquet([{file_list}])"
    )
    return view


def create_views(
    connection: Any,
    objects: Mapping[str, Sequence[str]],
    *,
    columns: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, str]:
    """Create one view per logical object of ``Pack.objects_source``.

    Args:
        connection: an open DuckDB connection.
        objects: ``{object_name: [parquet parts]}``.
        columns: optional ``{object_name: {source_column: alias}}``.

    Returns:
        ``{object_name: view_name}``.
    """
    created: dict[str, str] = {}
    for name, parts in objects.items():
        aliases = (columns or {}).get(name)
        created[name] = create_view(connection, name, parts, columns=aliases)
    return created


def view_row_count(connection: Any, view: str) -> int:
    """Row count of a view, evaluated by DuckDB without materializing rows."""
    result = connection.execute(
        f"SELECT count(*) FROM {quote_identifier(view)}"
    ).fetchone()
    return int(result[0]) if result else 0
