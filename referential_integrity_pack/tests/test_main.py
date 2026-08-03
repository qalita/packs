"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import json

import polars as pl
import pytest

import main
from conftest import write_parts


def _by_key(pack, key):
    return [item for item in pack.metrics.data if item["key"] == key]


def _relation(parent_table="dim_customers", child_table="fact_orders"):
    return {
        "parent": {
            "source": "source",
            "table": parent_table,
            "key": ["customer_id"],
        },
        "child": {
            "source": "source",
            "table": child_table,
            "key": ["customer_id"],
        },
    }


def _customers(ids):
    return pl.DataFrame({"customer_id": ids, "name": [f"c{i}" for i in ids]})


def _orders(ids):
    return pl.DataFrame(
        {"order_id": list(range(len(ids))), "customer_id": ids}
    )


# --------------------------------------------------------------------------
# the bugs being fixed
# --------------------------------------------------------------------------


def test_multi_part_child_is_fully_checked(make_pack, tmp_path):
    """Regression: every part of the child is checked, not just part 1.

    Part 1 of the child references only valid customers, so a pack reading
    ``parquet[0]`` reports zero orphans. Parts 2 and 3 each hold 50 orphans.
    """
    parent = write_parts(
        tmp_path / "p",
        "sqlite_dim_customers",
        [
            _customers(list(range(1, 101))),
            _customers(list(range(101, 201))),
            _customers(list(range(201, 301))),
        ],
    )
    child = write_parts(
        tmp_path / "c",
        "sqlite_fact_orders",
        [
            _orders(list(range(1, 101))),
            _orders(list(range(50, 100)) + list(range(9000, 9050))),
            _orders(list(range(50, 100)) + list(range(9100, 9150))),
        ],
    )

    pack = make_pack(
        {"job": {"relations": [_relation()]}},
        {"sqlite_dim_customers": parent, "sqlite_fact_orders": child},
    )
    main.run(pack)

    assert _by_key(pack, "missing_foreign_keys")[0]["value"] == 100
    assert _by_key(pack, "checked_foreign_keys")[0]["value"] == 300
    assert _by_key(pack, "score")[0]["value"] == "0.67"


def test_relations_resolve_the_declared_tables(make_pack, tmp_path):
    """Regression: the relation's parent/child tables are actually used.

    The previous code took ``pack.df_source`` wholesale, which unioned every
    loaded object. That union contains the orphan ids in the ``fact_orders``
    rows themselves, so it reported a different — and meaningless — count.
    """
    parent = write_parts(
        tmp_path / "p", "sqlite_dim_customers", [_customers([1, 2, 3])]
    )
    child = write_parts(
        tmp_path / "c", "sqlite_fact_orders", [_orders([1, 2, 3, 77, 88])]
    )

    pack = make_pack(
        {"job": {"relations": [_relation()]}},
        {"sqlite_dim_customers": parent, "sqlite_fact_orders": child},
    )
    main.run(pack)

    assert _by_key(pack, "missing_foreign_keys")[0]["value"] == 2
    assert _by_key(pack, "checked_foreign_keys")[0]["value"] == 5


def test_every_declared_table_is_loaded_once(make_pack, tmp_path):
    parent = write_parts(
        tmp_path / "p", "sqlite_dim_customers", [_customers([1])]
    )
    child = write_parts(tmp_path / "c", "sqlite_fact_orders", [_orders([1])])
    second = write_parts(tmp_path / "s", "sqlite_fact_returns", [_orders([1])])

    pack = make_pack(
        {
            "job": {
                "relations": [
                    _relation(),
                    _relation(child_table="fact_returns"),
                ]
            }
        },
        {
            "sqlite_dim_customers": parent,
            "sqlite_fact_orders": child,
            "sqlite_fact_returns": second,
        },
    )
    main.run(pack)

    assert pack.loaded_calls == [
        ("source", "dim_customers"),
        ("source", "fact_orders"),
        ("source", "fact_returns"),
    ]


# --------------------------------------------------------------------------
# counting
# --------------------------------------------------------------------------


def test_null_child_keys_are_orphans_and_null_parents_are_ignored():
    parent = pl.DataFrame({"k": [1, 2, None]}).lazy()
    child = pl.DataFrame({"k": [1, 2, None, 3]}).lazy()
    orphans, total, rows = main._fk_counts(parent, child, ["k"], ["k"], 10)
    assert (orphans, total) == (2, 4)
    assert rows.height == 2


def test_composite_keys_with_different_names():
    parent = pl.DataFrame({"country": ["fr", "fr"], "code": [1, 2]}).lazy()
    child = pl.DataFrame(
        {"c_country": ["fr", "fr", "be"], "c_code": [1, 3, 1]}
    ).lazy()
    orphans, total, _ = main._fk_counts(
        parent, child, ["country", "code"], ["c_country", "c_code"], 10
    )
    assert (orphans, total) == (2, 3)


def test_duplicate_parent_keys_do_not_multiply_the_child_count():
    parent = pl.DataFrame({"k": [1, 1, 1, 2]}).lazy()
    child = pl.DataFrame({"k": [1, 1, 2]}).lazy()
    orphans, total, _ = main._fk_counts(parent, child, ["k"], ["k"], 0)
    assert (orphans, total) == (0, 3)


# --------------------------------------------------------------------------
# bounded evidence
# --------------------------------------------------------------------------


def test_orphan_examples_are_bounded(make_pack, tmp_path):
    parent = write_parts(
        tmp_path / "p", "sqlite_dim_customers", [_customers([1])]
    )
    child = write_parts(
        tmp_path / "c",
        "sqlite_fact_orders",
        [_orders(list(range(500, 1000)))],
    )

    pack = make_pack(
        {"job": {"relations": [_relation()], "examples": 4}},
        {"sqlite_dim_customers": parent, "sqlite_fact_orders": child},
    )
    main.run(pack)

    payload = _by_key(pack, "missing_foreign_keys_examples")[0]["value"]
    rows = json.loads(payload)
    assert len(rows) == 4
    assert set(rows[0]) == {"customer_id"}


def test_example_limit_is_capped(make_pack, tmp_path):
    assert main._example_limit({"examples": 10**9}) == main.MAX_EXAMPLE_ROWS
    assert main._example_limit({"examples": -3}) == 0
    assert main._example_limit({}) == main.DEFAULT_EXAMPLE_ROWS


# --------------------------------------------------------------------------
# object resolution
# --------------------------------------------------------------------------


def test_object_resolution_matches_the_slugified_suffix():
    objects = {
        "postgresql_public_dim_customers": [],
        "postgresql_public_fact_orders": [],
    }
    assert (
        main._resolve_object(objects, "public.dim_customers")
        == "postgresql_public_dim_customers"
    )
    assert (
        main._resolve_object(objects, "fact_orders")
        == "postgresql_public_fact_orders"
    )


def test_a_single_object_source_needs_no_table_name():
    objects = {"csv_customers": []}
    assert main._resolve_object(objects, "anything") == "csv_customers"


def test_an_unresolvable_table_raises():
    objects = {"a_x": [], "b_y": []}
    with pytest.raises(KeyError, match="cannot resolve table"):
        main._resolve_object(objects, "zzz")


def test_relation_tables_are_distinct_and_ordered():
    relations = [_relation(), _relation(child_table="fact_returns")]
    assert main._relation_tables(relations, "source") == [
        "dim_customers",
        "fact_orders",
        "fact_returns",
    ]
    assert main._relation_tables(relations, "target") == []
