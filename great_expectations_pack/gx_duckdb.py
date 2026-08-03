"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Glue that lets Great Expectations 1.x drive a DuckDB database.

One adaptation is needed. It is a property of duckdb-engine, not of this pack,
and without it GX reports every expectation as failed rather than as
unsupported — which is worse than not running at all.

DuckDB has no real cursors: duckdb-engine's "cursor" IS the connection, so its
pending result is discarded the moment SQLAlchemy rolls that connection back.
``SqlAlchemyExecutionEngine.execute_query`` fetches OUTSIDE its
``with engine.connect()`` block, so every metric came back as an empty row set
and every expectation died with "list index out of range". Buffering the rows
at execute time makes the later fetch survive. It costs nothing extra: these
are the rows GX was about to fetch into Python anyway, and every metric query
GX bundles is a single-row aggregate.

A tempting second adaptation was tried and REJECTED, so that nobody adds it
back: GX picks its regex SQL by testing whether the dialect derives from
PGDialect, which duckdb-engine's does. Making that test succeed does make the
regex expectations run — and return wrong answers. PostgreSQL's ``~`` is a
PARTIAL match while DuckDB's ``~`` is a FULL match, so ``'a0' ~ '^a'`` is true
in PostgreSQL and false in DuckDB. Every regex expectation then reports 100%
unexpected values. The regex and LIKE families are handled by sampling in
main.py instead, which is at least honest about what it measured.
"""

from __future__ import annotations

from typing import Any, Callable

import duckdb
import duckdb_engine

__all__ = [
    "BufferedCursor",
    "BufferedConnection",
    "connection_kwargs",
    "make_creator",
]


class BufferedCursor(duckdb_engine.CursorWrapper):
    """A duckdb-engine cursor whose rows outlive the connection.

    ``_rows`` is filled eagerly by :meth:`execute` because filling it lazily
    would already be too late: by the time the caller fetches, the connection
    has been returned to the pool and rolled back.
    """

    _rows: list | None = None

    def execute(self, statement, parameters=None, context=None):
        super().execute(statement, parameters, context)
        try:
            fetchall = duckdb_engine.CursorWrapper.__getattr__(
                self, "fetchall"
            )
            self._rows = list(fetchall())
        except Exception:  # noqa: BLE001 - see comment below
            # DDL and other statements without a result set. There is nothing
            # for the caller to fetch, and raising here would mask the real
            # error the caller is about to report.
            self._rows = []

    def fetchall(self):
        rows = self._rows or []
        self._rows = []
        return rows

    def fetchone(self):
        rows = self._rows or []
        return rows.pop(0) if rows else None

    def fetchmany(self, size=None):
        rows = self._rows or []
        count = size or 1
        head, self._rows = rows[:count], rows[count:]
        return head


class BufferedConnection(duckdb_engine.ConnectionWrapper):
    """duckdb-engine connection that hands out :class:`BufferedCursor`."""

    def cursor(self) -> BufferedCursor:
        # The parent stores the DuckDB handle under a name-mangled attribute;
        # reaching for it is the only way to build a cursor of our own class.
        return BufferedCursor(self._ConnectionWrapper__c, self)


def make_creator(database: str) -> Callable[[], BufferedConnection]:
    """A SQLAlchemy ``creator`` opening ``database`` read-only.

    Read-only is not just hygiene: it stops GX from trying to persist anything
    into the staging database, and it lets several pooled connections share the
    same DuckDB instance.
    """

    def creator() -> BufferedConnection:
        return BufferedConnection(
            duckdb.connect(database=database, read_only=True)
        )

    return creator


def connection_kwargs(database: str) -> dict[str, Any]:
    """``kwargs`` for ``context.data_sources.add_sql``.

    GX forwards these to ``sqlalchemy.create_engine``, which is the only hook
    that lets us substitute the connection class without patching GX itself.
    """
    return {"creator": make_creator(database)}
