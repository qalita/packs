"""Tests for the footer-only schema scanner.

The point of the pack is that nothing it emits requires reading a data page, so
the tests build a real parquet file and assert the metrics come out of
``collect_schema()`` alone.
"""

import datetime as dt

import polars as pl
import pytest

import main


@pytest.fixture(scope="module")
def parquet_schema(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "people.parquet"
    pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ratio": [0.1, 0.2, 0.3],
            "seen_at": [
                dt.datetime(2024, 1, 1),
                dt.datetime(2024, 1, 2),
                dt.datetime(2024, 1, 3),
            ],
            "active": [True, False, True],
        }
    ).write_parquet(path)
    return dict(pl.scan_parquet(path).collect_schema())


def test_type_name_drops_dtype_parameters():
    assert main.type_name(pl.Int64) == "Int64"
    assert main.type_name(pl.String) == "String"
    assert main.type_name(pl.Datetime("us")) == "Datetime"
    assert main.type_name(pl.Datetime("ns")) == "Datetime"
    assert main.type_name(pl.List(pl.Int64)) == "List"


def test_schema_entries_cover_every_column_in_order(parquet_schema):
    entries = main.schema_entries("people", parquet_schema)
    assert [e["value"] for e in entries] == [
        "id",
        "name",
        "ratio",
        "seen_at",
        "active",
    ]
    assert {e["key"] for e in entries} == {"column"}
    assert entries[0]["scope"] == {
        "perimeter": "column",
        "value": "id",
        "parent_scope": {"perimeter": "dataset", "value": "people"},
    }


def test_metric_keys_are_preserved(parquet_schema):
    metrics = main.schema_metrics("people", parquet_schema, 3)
    keys = {m["key"] for m in metrics}
    assert {
        "column_count",
        "column_list_hash",
        "column_order_hash",
        "column_type",
        "column_types_hash",
    } <= keys


def test_counts_come_from_the_schema(parquet_schema):
    metrics = {
        m["key"]: m["value"]
        for m in main.schema_metrics("people", parquet_schema, 3)
        if m["scope"]["perimeter"] == "dataset"
    }
    assert metrics["column_count"] == 5
    assert metrics["row_count"] == 3
    assert metrics["types_numeric"] == 2
    assert metrics["types_text"] == 1
    assert metrics["types_temporal"] == 1


def test_column_types_are_polars_names(parquet_schema):
    types = {
        m["scope"]["value"]: m["value"]
        for m in main.schema_metrics("people", parquet_schema, 3)
        if m["key"] == "column_type"
    }
    assert types == {
        "id": "Int64",
        "name": "String",
        "ratio": "Float64",
        "seen_at": "Datetime",
        "active": "Boolean",
    }


def test_type_badge_mirrors_column_type(parquet_schema):
    metrics = main.schema_metrics("people", parquet_schema, 3)
    column_types = {
        m["scope"]["value"]: m["value"]
        for m in metrics
        if m["key"] == "column_type"
    }
    badges = {
        m["scope"]["value"]: m["value"] for m in metrics if m["key"] == "type"
    }
    assert badges == column_types


def test_order_hash_changes_with_order_but_list_hash_does_not():
    a = {"x": pl.Int64, "y": pl.String}
    b = {"y": pl.String, "x": pl.Int64}
    ma = {m["key"]: m["value"] for m in main.schema_metrics("d", a, 0)}
    mb = {m["key"]: m["value"] for m in main.schema_metrics("d", b, 0)}
    assert ma["column_list_hash"] == mb["column_list_hash"]
    assert ma["column_order_hash"] != mb["column_order_hash"]


def test_types_hash_changes_when_a_dtype_changes():
    before = {"x": pl.Int64}
    after = {"x": pl.Float64}
    hb = {m["key"]: m["value"] for m in main.schema_metrics("d", before, 0)}
    ha = {m["key"]: m["value"] for m in main.schema_metrics("d", after, 0)}
    assert hb["column_types_hash"] != ha["column_types_hash"]


def test_types_hash_is_stable_across_datetime_precisions():
    us = {"t": pl.Datetime("us")}
    ns = {"t": pl.Datetime("ns")}
    hus = {m["key"]: m["value"] for m in main.schema_metrics("d", us, 0)}
    hns = {m["key"]: m["value"] for m in main.schema_metrics("d", ns, 0)}
    assert hus["column_types_hash"] == hns["column_types_hash"]


class FakePack:
    def __init__(self, tables, name):
        self._tables = tables
        self.source_config = {"name": name}

    def tables(self, trigger):
        return list(self._tables)


def test_single_object_source_stays_one_dataset():
    assert main.dataset_labels(FakePack(["csv_people_part"], "people")) == {
        "csv_people_part": "people"
    }
    assert main.dataset_labels(FakePack(["db_a", "db_b"], "wh")) == {
        "db_a": "db_a",
        "db_b": "db_b",
    }


def test_no_profiling_or_html_round_trip_remains():
    source = open(main.__file__, encoding="utf-8").read()
    assert "import ydata_profiling" not in source
    assert "from ydata_profiling" not in source
    assert "ProfileReport(" not in source
    assert "read_html(" not in source
    assert "import pandas" not in source


def test_pyproject_no_longer_pulls_the_profiling_stack():
    from pathlib import Path

    pyproject = (
        Path(main.__file__).resolve().parent / "pyproject.toml"
    ).read_text(encoding="utf-8")
    for dropped in ("ydata-profiling", "lxml", "html5lib", "beautifulsoup4"):
        assert dropped not in pyproject
    assert "polars" in pyproject
