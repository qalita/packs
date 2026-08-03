"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -
"""

import json

import polars as pl
import pytest

import main
from conftest import frame, write_parts


def _metrics(pack):
    return {
        (item["key"], json.dumps(item["scope"], sort_keys=True)): item["value"]
        for item in pack.metrics.data
    }


def _by_key(pack, key):
    return [item for item in pack.metrics.data if item["key"] == key]


def _conf(**job):
    base = {
        "drift_test": "ks",
        "bins": 10,
        "alpha": 0.05,
        "psi_threshold": 0.2,
        "examples": 10,
        "example_columns": 5,
    }
    base.update(job)
    return {"job": base}


# --------------------------------------------------------------------------
# statistics
# --------------------------------------------------------------------------


def test_psi_is_zero_for_identical_distributions():
    shares = [0.1] * 10
    assert main._psi(shares, shares) == pytest.approx(0.0, abs=1e-9)


def test_psi_grows_with_the_shift():
    reference = [0.5, 0.5]
    close = [0.45, 0.55]
    far = [0.05, 0.95]
    assert main._psi(reference, far) > main._psi(reference, close) > 0


def test_binned_ks_is_the_largest_cdf_gap():
    reference = [0.5, 0.5, 0.0]
    current = [0.0, 0.5, 0.5]
    assert main._binned_ks(reference, current) == pytest.approx(0.5)


def test_ks_p_value_is_one_when_there_is_no_gap():
    assert main._ks_p_value(0.0, 1000, 1000) == pytest.approx(1.0)


def test_ks_p_value_falls_as_the_gap_grows():
    small = main._ks_p_value(0.02, 5000, 5000)
    large = main._ks_p_value(0.5, 5000, 5000)
    assert large < 0.001 < small <= 1.0


# --------------------------------------------------------------------------
# binning
# --------------------------------------------------------------------------


def test_bin_edges_span_the_reference_range():
    lf = pl.DataFrame({"amount": list(range(1000))}).lazy()
    edges = main._bin_edges(lf, ["amount"], 10, exact=True)
    assert edges["amount"][0] == pytest.approx(0.0)
    assert edges["amount"][-1] == pytest.approx(999.0)
    assert edges["amount"] == sorted(edges["amount"])


def test_bin_edges_skip_a_constant_column():
    lf = pl.DataFrame({"amount": [7] * 100}).lazy()
    assert main._bin_edges(lf, ["amount"], 10, exact=True) == {}


def test_bin_counts_fold_out_of_range_values_into_the_extreme_bins():
    edges = {"amount": [0.0, 10.0, 20.0]}
    lf = pl.DataFrame({"amount": [-5, 5, 15, 999]}).lazy()
    assert main._bin_counts(lf, edges) == {"amount": [2, 2]}


# --------------------------------------------------------------------------
# the bug being fixed: every part of a chunked source is analysed
# --------------------------------------------------------------------------


def test_multi_part_source_is_fully_analysed(make_pack, tmp_path):
    """Regression: the drift must come from every part, not from part 1.

    The first part is identical on both sides, so a pack reading ``paths[0]``
    reports no drift at all. Parts 2 and 3 of the current dataset sit far
    outside the reference range.
    """
    reference = write_parts(
        tmp_path / "ref",
        "file_reference",
        [frame(list(range(1000))) for _ in range(3)],
    )
    current = write_parts(
        tmp_path / "cur",
        "file_current",
        [
            frame(list(range(1000))),
            frame(list(range(5000, 6000))),
            frame(list(range(5000, 6000))),
        ],
    )

    pack = make_pack(
        _conf(), {"file_reference": reference}, {"file_current": current}
    )
    main.run(pack)

    metrics = _metrics(pack)
    assert int(_by_key(pack, "columns_compared")[0]["value"]) == 1
    assert int(_by_key(pack, "drifted_columns")[0]["value"]) == 1

    psi = float(_by_key(pack, "psi")[0]["value"])
    ks = float(_by_key(pack, "ks_statistic")[0]["value"])
    p_value = float(_by_key(pack, "p_value")[0]["value"])
    # Two thirds of the current mass left the reference support. The binned KS
    # only samples the gap at the 9 inner edges, so it lands just under the
    # exact 2/3 — the documented lower-bound behaviour.
    assert psi > 1.0
    assert ks == pytest.approx(0.6, abs=0.02)
    assert p_value < 0.001
    assert _by_key(pack, "score")[0]["value"] == "0.0"
    assert metrics[
        (
            "p_value_method",
            json.dumps(
                {
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": "reference",
                    },
                    "perimeter": "column",
                    "value": "amount",
                },
                sort_keys=True,
            ),
        )
    ] == ("binned_ks_asymptotic")


def test_identical_multi_part_datasets_do_not_drift(make_pack, tmp_path):
    parts = [frame(list(range(1000))) for _ in range(3)]
    reference = write_parts(tmp_path / "ref", "file_reference", parts)
    current = write_parts(tmp_path / "cur", "file_current", parts)

    pack = make_pack(
        _conf(), {"file_reference": reference}, {"file_current": current}
    )
    main.run(pack)

    assert float(_by_key(pack, "psi")[0]["value"]) == pytest.approx(
        0.0, abs=1e-6
    )
    assert _by_key(pack, "score")[0]["value"] == "1.0"
    assert int(_by_key(pack, "drifted_columns")[0]["value"]) == 0


# --------------------------------------------------------------------------
# the drift_test knob, which the previous version declared and ignored
# --------------------------------------------------------------------------


def test_drift_test_knob_selects_the_decision_rule(make_pack, tmp_path):
    """A shift big enough for KS at 1e4 rows, but under the PSI threshold."""
    reference = write_parts(
        tmp_path / "ref", "file_reference", [frame(list(range(10000)))]
    )
    current = write_parts(
        tmp_path / "cur", "file_current", [frame(list(range(200, 10200)))]
    )

    ks_pack = make_pack(
        _conf(drift_test="ks"),
        {"file_reference": reference},
        {"file_current": current},
    )
    main.run(ks_pack)

    psi_pack = make_pack(
        _conf(drift_test="psi", psi_threshold=5.0),
        {"file_reference": reference},
        {"file_current": current},
    )
    main.run(psi_pack)

    assert int(_by_key(ks_pack, "drifted_columns")[0]["value"]) == 1
    assert int(_by_key(psi_pack, "drifted_columns")[0]["value"]) == 0
    assert _by_key(ks_pack, "drift_test")[0]["value"] == "ks"
    assert _by_key(psi_pack, "drift_test")[0]["value"] == "psi"


def test_unknown_drift_test_is_rejected(make_pack, tmp_path):
    reference = write_parts(
        tmp_path / "ref", "file_reference", [frame([1, 2, 3])]
    )
    pack = make_pack(
        _conf(drift_test="chi2"),
        {"file_reference": reference},
        {"file_current": reference},
    )
    with pytest.raises(ValueError, match="unknown drift_test"):
        main.run(pack)


# --------------------------------------------------------------------------
# bounded evidence
# --------------------------------------------------------------------------


def test_example_rows_are_bounded(make_pack, tmp_path):
    reference = write_parts(
        tmp_path / "ref", "file_reference", [frame(list(range(1000)))]
    )
    current = write_parts(
        tmp_path / "cur", "file_current", [frame([5000] * 1000)]
    )

    pack = make_pack(
        _conf(examples=3),
        {"file_reference": reference},
        {"file_current": current},
    )
    main.run(pack)

    examples = _by_key(pack, "drift_example_rows")
    assert examples, "a drifted column must ship bounded evidence"
    rows = json.loads(examples[0]["value"])
    assert len(rows) == 3
    assert set(rows[0]) == {"amount"}


def test_example_rows_can_be_turned_off(make_pack, tmp_path):
    reference = write_parts(
        tmp_path / "ref", "file_reference", [frame(list(range(1000)))]
    )
    current = write_parts(
        tmp_path / "cur", "file_current", [frame([5000] * 1000)]
    )

    pack = make_pack(
        _conf(examples=0),
        {"file_reference": reference},
        {"file_current": current},
    )
    main.run(pack)
    assert _by_key(pack, "drift_example_rows") == []


# --------------------------------------------------------------------------
# table pairing
# --------------------------------------------------------------------------


def test_tables_are_paired_by_name_when_both_sides_share_them(
    make_pack, tmp_path
):
    a_ref = write_parts(tmp_path / "r", "orders", [frame([1, 2, 3])])
    b_ref = write_parts(tmp_path / "r", "invoices", [frame([4, 5, 6])])
    a_cur = write_parts(tmp_path / "c", "orders", [frame([1, 2, 3])])
    b_cur = write_parts(tmp_path / "c", "invoices", [frame([4, 5, 6])])

    pack = make_pack(
        _conf(),
        {"orders": a_ref, "invoices": b_ref},
        {"invoices": b_cur, "orders": a_cur},
    )
    assert main._pair_tables(pack) == [
        ("orders", "orders", "orders"),
        ("invoices", "invoices", "invoices"),
    ]


def test_tables_are_paired_in_load_order_when_names_differ(
    make_pack, tmp_path
):
    ref = write_parts(tmp_path / "r", "db_2024_q1", [frame([1])])
    cur = write_parts(tmp_path / "c", "db_2024_q2", [frame([2])])
    pack = make_pack(_conf(), {"db_2024_q1": ref}, {"db_2024_q2": cur})
    assert main._pair_tables(pack) == [
        ("db_2024_q1", "db_2024_q1", "db_2024_q2")
    ]


def test_only_numeric_columns_common_to_both_sides_are_compared():
    reference = {"a": pl.Int64, "b": pl.String, "c": pl.Float64}
    current = {"a": pl.Float64, "b": pl.String, "d": pl.Int64}
    assert main._comparable_columns(reference, current) == ["a"]
