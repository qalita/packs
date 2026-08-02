"""Pattern validation pack: anchoring semantics, one pass, bounded fallback."""

import re
from pathlib import Path

import polars as pl
import pytest
from qalita_core import analytics

import main

SAMPLES = ["a1", "b1", "xa", "xb", "", "ab", "zz"]


def metrics_by_key(pack):
    out = {}
    for entry in pack.metrics.data:
        out.setdefault(entry["key"], []).append(entry)
    return out


def matched(pattern, samples=SAMPLES):
    return (
        pl.DataFrame({"s": samples})
        .select(pl.col("s").str.contains(pattern))
        .to_series()
        .to_list()
    )


@pytest.mark.parametrize(
    "pattern",
    ["a|b", "^a|b", "foo|bar", r"\d+|x", r"^[A-Za-z]+$", "a", "(a|b)1"],
)
def test_anchored_wrapper_reproduces_re_match(pattern):
    """'^(?:...)' is match(); '^...' is not, once an alternation is involved."""
    expected = [bool(re.match(pattern, s)) for s in SAMPLES]
    assert matched(main.anchored(pattern)) == expected


def test_the_non_capturing_group_is_what_makes_it_correct():
    # '^a|b' anchors only the left branch: "xb" would be accepted.
    assert matched("^a|b") != [bool(re.match("a|b", s)) for s in SAMPLES]
    assert matched(main.anchored("a|b")) == [
        bool(re.match("a|b", s)) for s in SAMPLES
    ]


def test_builtin_patterns_all_compile_in_the_engine():
    for name, pattern in main.BUILTIN_PATTERNS.items():
        assert main.polars_supports(pattern), name


@pytest.mark.parametrize(
    "pattern", [r"(\w)\1", r"(?=.*x).*", r"(?<!a)b", r"(a)\1{2}"]
)
def test_polars_rejects_backreferences_and_lookaround(pattern):
    assert main.polars_supports(pattern) is False


def test_every_check_fits_in_one_pass(parquet_parts, monkeypatch):
    calls = []
    original = analytics.agg

    def counting(lf, exprs):
        calls.append(len(exprs))
        return original(lf, exprs)

    monkeypatch.setattr(analytics, "agg", counting)

    lf = pl.scan_parquet(parquet_parts)
    checks = [
        {
            "column": column,
            "pattern_name": name,
            "pattern": main.BUILTIN_PATTERNS[name],
            "supported": True,
        }
        for column, name in (
            ("email", "email"),
            ("user_id", "uuid"),
            ("ip_address", "ipv4"),
        )
    ]
    results, rows = main.measure(lf, checks)
    assert len(calls) == 1
    assert rows == 6
    assert [check["invalid_count"] for check in results] == [2, 2, 2]
    # nulls are out of the denominator, empty strings are valid
    assert [check["total"] for check in results] == [5, 5, 5]


def test_run_emits_the_historical_metric_keys(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)

    assert metrics["invalid_email_format_found"][0]["value"] == 2
    assert metrics["invalid_email_format_percent"][0]["value"] == "0.4"
    assert metrics["valid_email_percent"][0]["value"] == "0.6"
    assert metrics["invalid_uuid_format_found"][0]["value"] == 2
    assert metrics["invalid_ip4_address_format_found"][0]["value"] == 2
    assert metrics["score"][0]["value"] == "0.6"

    assert metrics["invalid_email_format_found"][0]["scope"] == {
        "perimeter": "column",
        "value": "email",
        "parent_scope": {"perimeter": "dataset", "value": "users"},
    }


def test_every_metric_carries_its_method(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)
    assert metrics["invalid_email_format_found_method"][0]["value"] == "exact"
    assert metrics["score_method"][0]["value"] == "exact"


def test_run_emits_bounded_examples(pack):
    main.run(pack)
    metrics = metrics_by_key(pack)
    examples = {
        entry["scope"]["value"]: entry["value"]
        for entry in metrics["invalid_format_examples"]
    }
    assert examples["email"] == [
        {"id": 2, "email": "bad"},
        {"id": 6, "email": "a@b"},
    ]


def test_examples_respect_the_configured_limit(pack):
    pack.pack_config["job"]["example_rows"] = 1
    main.run(pack)
    metrics = metrics_by_key(pack)
    for entry in metrics["invalid_format_examples"]:
        assert len(entry["value"]) == 1


def test_backreference_pattern_falls_back_to_a_labelled_sample(pack):
    """A pattern the engine cannot compile must not kill the job."""
    pack.pack_config["job"]["patterns"] = [
        {"column": "code", "type": "regex", "regex": r"(\w)\1"}
    ]
    main.run(pack)
    metrics = metrics_by_key(pack)

    # "ab" and "de" do not start with a doubled character
    assert metrics["text_not_matching_regex_found"][0]["value"] == 2
    assert metrics["texts_not_matching_regex_percent"][0]["value"] == "0.3333"
    assert (
        metrics["text_not_matching_regex_found_method"][0]["value"]
        == "sampled_python_regex"
    )
    assert metrics["score_method"][0]["value"] == "sampled_python_regex"
    assert metrics["invalid_format_examples"][0]["value"] == [
        {"id": 2, "code": "ab"},
        {"id": 5, "code": "de"},
    ]


def test_autodetection_runs_when_no_rule_is_configured(pack):
    pack.pack_config["job"]["patterns"] = []
    main.run(pack)
    metrics = metrics_by_key(pack)
    assert metrics["invalid_email_format_found"][0]["scope"]["value"] == (
        "email"
    )
    assert metrics["invalid_ip4_address_format_found"][0]["scope"][
        "value"
    ] == ("ip_address")
    # user_id is not auto-detected as a UUID column: the name carries no hint
    assert "invalid_uuid_format_found" not in metrics


def test_unknown_pattern_type_is_skipped(pack, capsys):
    pack.pack_config["job"]["patterns"] = [
        {"column": "email", "type": "not_a_pattern"}
    ]
    main.run(pack)
    assert "Unknown pattern type" in capsys.readouterr().out
    assert metrics_by_key(pack)["score"][0]["value"] == "1.0"


def test_pack_does_not_import_pandas_or_numpy():
    source = Path(main.__file__).read_text(encoding="utf-8")
    assert "import pandas" not in source
    assert "import numpy" not in source


def test_no_unbounded_materialization():
    source = Path(main.__file__).read_text(encoding="utf-8")
    for forbidden in ("read_parquet", "to_pandas", ".collect()"):
        assert forbidden not in source
