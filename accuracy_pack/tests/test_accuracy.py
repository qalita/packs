"""Accuracy pack: decimal profiling, geo ranges, and the shape of the scan."""

import math
from pathlib import Path

import polars as pl
import pytest
from qalita_core import analytics

import main


def metrics_by_key(pack):
    out = {}
    for entry in pack.metrics.data:
        out.setdefault(entry["key"], []).append(entry)
    return out


def test_decimals_expr_matches_python_str():
    """The lazy expression reproduces len(str(x).split('.')[1])."""
    values = [1.0, 1.5, 1.25, 1.234567890123, 1e20, 1e-7, 0.1 + 0.2, 42.0]
    frame = pl.DataFrame({"v": values})
    computed = frame.select(main.decimals_expr("v")).to_series().to_list()
    expected = [
        len(str(v).split(".")[1]) if "." in str(v) else 0 for v in values
    ]
    assert computed == expected


def test_decimals_expr_ignores_null_and_nan():
    frame = pl.DataFrame({"v": [1.25, None, math.nan]})
    assert frame.select(main.decimals_expr("v")).to_series().to_list() == [
        2,
        None,
        None,
    ]


def test_profile_reads_the_source_twice_whatever_the_width(
    parquet_parts, monkeypatch
):
    """Two passes by design, not two passes per column."""
    calls = []
    original = analytics.agg

    def counting(lf, exprs):
        calls.append(len(exprs))
        return original(lf, exprs)

    monkeypatch.setattr(analytics, "agg", counting)

    lf = pl.scan_parquet(parquet_parts)
    main.profile(lf, ["amount", "latitude"], [("latitude", "latitude")])
    assert len(calls) == 2


def test_profile_counts_decimal_buckets(parquet_parts):
    lf = pl.scan_parquet(parquet_parts)
    decimals, coordinates = main.profile(
        lf, ["amount", "latitude"], [("latitude", "latitude")]
    )

    # null and NaN are excluded, exactly as dropna() used to do.
    assert decimals["amount"]["valid_points"] == 4
    assert decimals["amount"]["max_decimals"] == 2
    assert decimals["amount"]["counts"] == {0: 0, 1: 3, 2: 1}

    assert coordinates["latitude"] == {
        "kind": "latitude",
        "invalid": 2,
        "valid_points": 6,
    }


def test_most_common_breaks_ties_on_the_smallest_value():
    assert main.most_common({0: 5, 1: 5, 2: 1}) == (0, 5)
    assert main.most_common({0: 1, 3: 7}) == (3, 7)


def test_geo_checks_detects_by_name_and_dtype():
    schema = {
        "latitude": pl.Float64,
        "lng": pl.Int64,
        "lat_label": pl.String,
        "other": pl.Float64,
    }
    assert main.geo_checks(schema) == [
        ("latitude", "latitude"),
        ("lng", "longitude"),
    ]


def test_run_emits_the_historical_metric_keys(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)

    proportion = {
        entry["scope"]["value"]: entry["value"]
        for entry in metrics["proportion_score"]
    }
    assert proportion == {"amount": "0.75", "latitude": "1.0"}

    assert metrics["decimal_precision"][0]["scope"]["value"] == "amount"
    assert metrics["decimal_precision"][0]["value"] == "2"

    # mean of the per-column proportions, then the point-weighted mean
    assert metrics["score"][0]["value"] == "0.88"
    assert metrics["float_score"][0]["value"] == "0.9"

    assert metrics["invalid_latitude"][0]["value"] == 2
    assert metrics["valid_latitude_percent"][0]["value"] == "0.6667"

    # every scope hangs under the logical object name, not "src_1"
    assert metrics["proportion_score"][0]["scope"]["parent_scope"] == {
        "perimeter": "dataset",
        "value": "orders",
    }


def test_run_emits_bounded_examples(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)

    uneven = metrics["uneven_decimals_examples"][0]
    assert uneven["scope"]["value"] == "amount"
    # only 2.25 differs from the modal decimal count
    assert uneven["value"] == [{"id": 2, "amount": 2.25}]

    latitudes = metrics["invalid_latitude_examples"][0]["value"]
    assert latitudes == [
        {"id": 2, "latitude": 95.0},
        {"id": 3, "latitude": -100.0},
    ]


def test_examples_respect_the_configured_limit(pack):
    pack.pack_config["job"]["example_rows"] = 1
    main.run(pack)
    metrics = metrics_by_key(pack)
    assert len(metrics["invalid_latitude_examples"][0]["value"]) == 1


def test_examples_can_be_turned_off(pack):
    pack.pack_config["job"]["examples"] = False
    main.run(pack)
    metrics = metrics_by_key(pack)
    assert "uneven_decimals_examples" not in metrics
    assert "invalid_latitude_examples" not in metrics


def test_run_recommends_uneven_rounding_and_bad_coordinates(pack):
    main.run(pack)
    types = [entry["type"] for entry in pack.recommendations.data]
    assert types.count("Unevenly Rounded Data") == 2  # column + dataset
    assert "Invalid Latitude" in types


def test_pack_does_not_import_pandas_or_numpy():
    source = Path(main.__file__).read_text(encoding="utf-8")
    assert "import pandas" not in source
    assert "import numpy" not in source


def test_no_unbounded_materialization():
    """No read_parquet / to_pandas / bare collect survives in the pack."""
    source = Path(main.__file__).read_text(encoding="utf-8")
    for forbidden in ("read_parquet", "to_pandas", ".collect()"):
        assert forbidden not in source


@pytest.mark.parametrize("column", ["amount", "latitude"])
def test_defined_folds_nan_into_null(parquet_parts, column):
    lf = pl.scan_parquet(parquet_parts)
    counted = analytics.agg(lf, {"n": main.defined(column).count()})["n"]
    assert counted == (4 if column == "amount" else 6)
