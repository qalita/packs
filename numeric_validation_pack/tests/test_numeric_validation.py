"""Numeric validation pack: range statistics batched into a single pass."""

from pathlib import Path

import polars as pl
from qalita_core import analytics

import main


def metrics_by_key(pack):
    out = {}
    for entry in pack.metrics.data:
        out.setdefault(entry["key"], []).append(entry)
    return out


def scoped(entries):
    return {entry["scope"]["value"]: entry["value"] for entry in entries}


def test_resolve_range_covers_the_documented_types():
    assert main.resolve_range({"type": "latitude"}) == (-90, 90)
    assert main.resolve_range({"type": "longitude"}) == (-180, 180)
    assert main.resolve_range({"type": "percentage"}) == (0, 100)
    assert main.resolve_range({"type": "non_negative"}) == (0, None)
    assert main.resolve_range({"type": "non_negative", "max_value": 10}) == (
        0,
        10,
    )
    assert main.resolve_range({"min_value": 1, "max_value": 2}) == (1, 2)


def test_plan_checks_drops_missing_and_non_numeric_columns(capsys):
    schema = {"age": pl.Int64, "label": pl.String}
    checks = main.plan_checks(
        [
            {"column": "age", "min_value": 0},
            {"column": "label", "min_value": 0},
            {"column": "ghost", "min_value": 0},
        ],
        schema,
    )
    assert [check["column"] for check in checks] == ["age"]
    out = capsys.readouterr().out
    assert "not found" in out and "not numeric" in out


def test_every_rule_and_column_fits_in_one_pass(parquet_parts, monkeypatch):
    calls = []
    original = analytics.agg

    def counting(lf, exprs):
        calls.append(len(exprs))
        return original(lf, exprs)

    monkeypatch.setattr(analytics, "agg", counting)

    lf = pl.scan_parquet(parquet_parts)
    checks = [
        {
            "column": "age",
            "type": None,
            "min_value": 0,
            "max_value": 150,
        },
        {"column": "price", "type": None, "min_value": 0, "max_value": None},
    ]
    main.measure(lf, checks, ["id", "age", "price", "latitude"])
    assert len(calls) == 1


def test_measure_matches_the_pandas_semantics(parquet_parts):
    lf = pl.scan_parquet(parquet_parts)
    checks = [
        {"column": "age", "type": None, "min_value": 0, "max_value": 150},
        {"column": "price", "type": None, "min_value": 0, "max_value": None},
    ]
    results, negatives = main.measure(lf, checks, ["price"])

    age = results[0]
    # the null is excluded, like dropna()
    assert age["total"] == 5
    assert (age["below_min"], age["above_max"]) == (1, 1)
    assert age["in_range_percent"] == 0.6
    assert (age["min_value_observed"], age["max_value_observed"]) == (
        -5.0,
        200.0,
    )
    assert (age["sum_value"], age["mean_value"]) == (330.0, 66.0)

    price = results[1]
    # NaN is excluded too, which polars does not do on its own
    assert price["total"] == 5
    assert (price["below_min"], price["above_max"]) == (1, 0)
    assert price["sum_value"] == 112.5
    assert price["mean_value"] == 22.5

    assert negatives[0] == {
        "column": "price",
        "total": 5,
        "negative_count": 1,
        "negative_percent": 0.2,
    }


def test_run_emits_the_historical_metric_keys(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)

    assert scoped(metrics["number_below_min_value"]) == {
        "age": 1,
        "price": 1,
        "latitude": 1,
    }
    assert scoped(metrics["number_above_max_value"]) == {
        "age": 1,
        "latitude": 1,
    }
    assert scoped(metrics["number_in_range_percent"]) == {
        "age": "0.6",
        "price": "0.8",
        "latitude": "0.6667",
    }
    assert scoped(metrics["min_value"])["age"] == "-5.0"
    assert scoped(metrics["max_value"])["age"] == "200.0"
    assert scoped(metrics["sum_value"])["price"] == "112.5"
    assert scoped(metrics["mean_value"])["price"] == "22.5"

    assert metrics["invalid_latitude"][0]["value"] == 2
    assert metrics["valid_latitude_percent"][0]["value"] == "0.6667"

    assert scoped(metrics["negative_values"]) == {
        "id": 0,
        "age": 1,
        "price": 1,
        "latitude": 1,
    }
    assert scoped(metrics["negative_values_percent"])["latitude"] == "0.1667"

    assert metrics["score"][0]["value"] == "0.69"
    assert metrics["score"][0]["scope"] == {
        "perimeter": "dataset",
        "value": "src",
    }


def test_run_emits_bounded_examples(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)

    out_of_range = {
        entry["scope"]["value"]: entry["value"]
        for entry in metrics["out_of_range_examples"]
    }
    assert out_of_range["age"] == [{"id": 1, "age": -5}, {"id": 3, "age": 200}]
    assert out_of_range["price"] == [{"id": 2, "price": -3.0}]

    negatives = {
        entry["scope"]["value"]: entry["value"]
        for entry in metrics["negative_values_examples"]
    }
    assert negatives["latitude"] == [{"id": 3, "latitude": -100.0}]


def test_examples_respect_the_configured_limit(pack):
    pack.pack_config["job"]["example_rows"] = 1
    main.run(pack)
    metrics = metrics_by_key(pack)
    age = [
        entry
        for entry in metrics["out_of_range_examples"]
        if entry["scope"]["value"] == "age"
    ][0]
    assert len(age["value"]) == 1


def test_examples_can_be_turned_off(pack):
    pack.pack_config["job"]["examples"] = False
    main.run(pack)
    assert "out_of_range_examples" not in metrics_by_key(pack)


def test_figures_count_one_check_per_column(pack):
    main.run(pack)
    figures = {
        figure["key"]: figure for figure in pack.figures.data["figures"]
    }
    outcome = {row[0]: row[1] for row in figures["checks_outcome"]["rows"]}
    assert outcome == {"pass": 0, "fail": 3}
    assert figures["violations_by_column"]["rows"] == [
        ["age", 2],
        ["price", 1],
        ["latitude", 2],
    ]


def test_pack_does_not_import_pandas_or_numpy():
    source = Path(main.__file__).read_text(encoding="utf-8")
    assert "import pandas" not in source
    assert "import numpy" not in source


def test_no_unbounded_materialization():
    source = Path(main.__file__).read_text(encoding="utf-8")
    for forbidden in ("read_parquet", "to_pandas", ".collect()"):
        assert forbidden not in source
