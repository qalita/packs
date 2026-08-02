"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

The property these tests exist for: a view must see EVERY part file.

The bug being locked out is not hypothetical. The pack used to pair configured
table names with parquet paths through ``zip``; when the lengths disagreed —
which is what chunking causes — its guard fell through to labelling each CHUNK
as its own dataset, so one table split in four was reported as four datasets,
each scored on a quarter of the rows.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb_view  # noqa: E402

pl = pytest.importorskip("polars")

ROWS_PER_PART = 7
PARTS = 4


def _write_parts(directory, name, parts=PARTS, rows=ROWS_PER_PART):
    """Write ``parts`` parquet files, each with distinct ids."""
    paths = []
    for index in range(parts):
        path = os.path.join(directory, f"{name}_part_{index + 1}.parquet")
        pl.DataFrame(
            {
                "id": list(range(index * rows, index * rows + rows)),
                "Order Date": [
                    "2024-01-0%d" % (i % 9 + 1) for i in range(rows)
                ],
            }
        ).write_parquet(path)
        paths.append(path)
    return paths


@pytest.fixture()
def parts(tmp_path):
    return _write_parts(str(tmp_path), "file_orders")


def test_view_spans_every_part(parts):
    connection = duckdb_view.connect()
    try:
        duckdb_view.create_view(connection, "orders", parts)
        assert duckdb_view.view_row_count(connection, "orders") == (
            PARTS * ROWS_PER_PART
        )
        # Not merely the right count: the ids of the LAST part must be there,
        # which is exactly what a first-part-only view would miss.
        ids = connection.execute(
            'SELECT min(id), max(id), count(DISTINCT id) FROM "orders"'
        ).fetchone()
        assert ids == (0, PARTS * ROWS_PER_PART - 1, PARTS * ROWS_PER_PART)
    finally:
        connection.close()


def test_view_over_one_part_is_not_the_whole_dataset(parts):
    """Guards the guard: the assertion above would pass trivially if a
    single-part view also counted every row."""
    connection = duckdb_view.connect()
    try:
        duckdb_view.create_view(connection, "first_only", parts[:1])
        assert duckdb_view.view_row_count(connection, "first_only") == (
            ROWS_PER_PART
        )
    finally:
        connection.close()


def test_create_views_keeps_objects_apart(tmp_path):
    objects = {
        "file_orders": _write_parts(str(tmp_path), "file_orders", parts=3),
        "file_customers": _write_parts(
            str(tmp_path), "file_customers", parts=2
        ),
    }
    connection = duckdb_view.connect()
    try:
        views = duckdb_view.create_views(connection, objects)
        assert set(views) == {"file_orders", "file_customers"}
        assert duckdb_view.view_row_count(
            connection, views["file_orders"]
        ) == (3 * ROWS_PER_PART)
        assert duckdb_view.view_row_count(
            connection, views["file_customers"]
        ) == (2 * ROWS_PER_PART)
    finally:
        connection.close()


def test_column_aliases_are_applied(parts):
    connection = duckdb_view.connect()
    try:
        duckdb_view.create_view(
            connection,
            "orders",
            parts,
            columns={"id": "id", "Order Date": "order_date"},
        )
        columns = [
            row[0]
            for row in connection.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'orders' ORDER BY ordinal_position"
            ).fetchall()
        ]
        assert columns == ["id", "order_date"]
        assert duckdb_view.view_row_count(connection, "orders") == (
            PARTS * ROWS_PER_PART
        )
    finally:
        connection.close()


def test_empty_part_list_is_refused():
    connection = duckdb_view.connect()
    try:
        with pytest.raises(ValueError):
            duckdb_view.create_view(connection, "empty", [])
    finally:
        connection.close()


def test_quoting_escapes_hostile_names(tmp_path):
    """Object names come from source metadata, not from an allow-list."""
    assert duckdb_view.quote_identifier('we"ird') == '"we""ird"'
    assert duckdb_view.quote_literal("/tmp/o'brien.parquet") == (
        "'/tmp/o''brien.parquet'"
    )

    paths = _write_parts(str(tmp_path), "file_orders", parts=2)
    connection = duckdb_view.connect()
    try:
        duckdb_view.create_view(connection, 'we"ird', paths)
        assert duckdb_view.view_row_count(connection, 'we"ird') == (
            2 * ROWS_PER_PART
        )
    finally:
        connection.close()


def test_view_survives_a_reopened_database(tmp_path, parts):
    """Not needed by the scan itself, which shares this process's connection,
    but it is the property that lets the staging database be inspected."""
    database = str(tmp_path / "staging.duckdb")
    writer = duckdb_view.connect(database)
    try:
        duckdb_view.create_view(writer, "orders", parts)
    finally:
        writer.close()

    reader = duckdb_view.connect(database, read_only=True)
    try:
        assert duckdb_view.view_row_count(reader, "orders") == (
            PARTS * ROWS_PER_PART
        )
    finally:
        reader.close()
