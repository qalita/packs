"""End-to-end tests: the full pipeline, and the loader's per-table wiring.

Bypasses qalita_core.pack.Pack for the pipeline tests (it needs platform
config that does not exist in this test environment) and drives
catalog -> runner -> reporting directly. loader.py is exercised
separately against a stub standing in for Pack, since it is the one
piece of genuinely new logic this task adds.
"""

import os
import runpy
import sys
import types

import pytest

import omop_dqd.checks  # noqa: F401  (registers every check)
from omop_dqd.catalog import load_catalog
from omop_dqd.loader import catalog_table_names, load_cdm_tables
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks

from tests.fixtures import PERSON, write_mini_cdm


def mini_cdm_person_columns():
    """PERSON's real columns in the fixture, as "PERSON.<field>"."""
    return {f"PERSON.{name}" for name in PERSON.columns}


MAIN_PY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py"
)


def test_full_pipeline_produces_metrics(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    metrics = build_metrics(results, "mini_cdm")

    keys = {m["key"] for m in metrics}
    assert "score" in keys
    dataset_scores = [
        m
        for m in metrics
        if m["key"] == "score" and m["scope"]["perimeter"] == "dataset"
    ]
    assert len(dataset_scores) == 1
    assert 0.0 <= float(dataset_scores[0]["value"]) <= 1.0


def test_full_pipeline_finds_the_planted_violations(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    failed = {
        (r.instance.check_name, r.instance.qualified_field)
        for r in results
        if r.result.status == CheckStatus.FAIL
    }
    # the duplicate primary key planted in the fixture
    assert (
        "isPrimaryKey",
        "CONDITION_OCCURRENCE.condition_occurrence_id",
    ) in failed


def test_full_pipeline_produces_recommendations(mini_cdm):
    results = run_checks(mini_cdm, load_catalog("5.4"))
    recommendations = build_recommendations(results, "mini_cdm")
    assert recommendations
    assert all(r["type"] == "OMOP CDM" for r in recommendations)


def test_missing_vocabulary_yields_not_applicable_not_failures(
    mini_cdm_no_vocabulary,
):
    results = run_checks(mini_cdm_no_vocabulary, load_catalog("5.4"))
    vocabulary_checks = [
        r
        for r in results
        if r.instance.check_name
        in {"fkDomain", "fkClass", "isStandardValidConcept"}
    ]
    assert vocabulary_checks
    assert all(
        r.result.status == CheckStatus.NOT_APPLICABLE
        for r in vocabulary_checks
    )


def test_both_cdm_versions_run(mini_cdm):
    for version in ("5.3", "5.4"):
        results = run_checks(mini_cdm, load_catalog(version))
        assert results


# --- loader.py: per-table loading against a stub Pack -------------------
#
# The real qalita_core.pack.Pack needs platform config files (source_conf
# .json, ~/.qalita/.worker...) that do not exist in this test environment,
# so it cannot be driven directly here. This stub reproduces only the two
# behaviours load_cdm_tables actually depends on:
#   - load_data(trigger, table_or_query=<one table name>) returns that
#     table's list of parquet paths;
#   - a table absent from the source raises, as it does inside the real
#     qalita_core data source opener.


class _StubPack:
    def __init__(self, table_paths):
        self._table_paths = table_paths
        self.calls = []

    def load_data(self, trigger, table_or_query=None):
        assert trigger == "source"
        self.calls.append(table_or_query)
        try:
            return self._table_paths[table_or_query]
        except KeyError as exc:
            raise RuntimeError(f"no such table: {table_or_query}") from exc


def test_loader_loads_available_tables(tmp_path):
    table_paths = write_mini_cdm(str(tmp_path))
    pack = _StubPack(table_paths)

    context = load_cdm_tables(pack, "5.4")

    assert context.has_table("PERSON")
    assert context.has_table("CONDITION_OCCURRENCE")
    assert context.has_vocabulary


def test_loader_skips_tables_absent_from_the_source(tmp_path):
    table_paths = write_mini_cdm(str(tmp_path))
    del table_paths["DEATH"]
    pack = _StubPack(table_paths)

    context = load_cdm_tables(pack, "5.4")

    assert not context.has_table("DEATH")
    assert context.has_table("PERSON")


def test_loader_respects_excluded_tables(tmp_path):
    table_paths = write_mini_cdm(str(tmp_path))
    pack = _StubPack(table_paths)

    context = load_cdm_tables(pack, "5.4", excluded_tables=["concept"])

    assert not context.has_table("CONCEPT")
    # excluding CONCEPT should not have even attempted to load it
    assert "CONCEPT" not in pack.calls
    assert context.has_table("PERSON")


def test_loader_calls_load_data_once_per_table_never_with_a_list(tmp_path):
    table_paths = write_mini_cdm(str(tmp_path))
    pack = _StubPack(table_paths)

    load_cdm_tables(pack, "5.4")

    assert pack.calls, "loader never called load_data"
    assert all(isinstance(name, str) for name in pack.calls)
    assert len(pack.calls) == len(set(pack.calls))
    assert set(pack.calls) == set(catalog_table_names("5.4"))


# --- main.py ---------------------------------------------------------
#
# The pipeline tests above deliberately bypass qalita_core.pack.Pack,
# so main.py's own wiring -- reading the job config, the
# threshold_overrides branch, and saving all three platform assets --
# is not covered by any of them. These run the real main.py through
# runpy against a stubbed Pack, the same way the loader tests above
# stub load_data.


class _StubAsset:
    def __init__(self, type_):
        self.type = type_
        self.data = []
        self.saved = False

    def save(self):
        # Deliberately does NOT write <type>.json: the point is to
        # assert main.py saves, not to litter the cwd.
        self.saved = True


class _StubPlatformPack(_StubPack):
    """_StubPack, plus the config and asset surface main.py uses."""

    def __init__(self, table_paths, job=None, source_name="mini_cdm"):
        super().__init__(table_paths)
        self.pack_config = {"job": dict(job or {})}
        self.source_config = {"name": source_name}
        self.metrics = _StubAsset("metrics")
        self.recommendations = _StubAsset("recommendations")
        self.schemas = _StubAsset("schemas")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def _run_main(monkeypatch, pack):
    """Execute main.py with qalita_core.pack.Pack replaced by `pack`."""
    module = types.ModuleType("qalita_core.pack")
    module.Pack = lambda *args, **kwargs: pack
    package = types.ModuleType("qalita_core")
    package.pack = module
    monkeypatch.setitem(sys.modules, "qalita_core", package)
    monkeypatch.setitem(sys.modules, "qalita_core.pack", module)
    runpy.run_path(MAIN_PY, run_name="__main__")
    return pack


def test_main_runs_the_pipeline_and_saves_all_three_assets(
    tmp_path, monkeypatch
):
    pack = _StubPlatformPack(write_mini_cdm(str(tmp_path)))
    _run_main(monkeypatch, pack)

    assert pack.metrics.saved
    assert pack.recommendations.saved
    assert pack.schemas.saved

    keys = {m["key"] for m in pack.metrics.data}
    assert "score" in keys
    assert pack.recommendations.data

    # schemas must carry the tree the metrics hang off
    perimeters = {s["scope"]["perimeter"] for s in pack.schemas.data}
    assert perimeters == {"table", "column"}
    table_scopes = {
        s["scope"]["value"]
        for s in pack.schemas.data
        if s["scope"]["perimeter"] == "table"
    }
    assert "PERSON" in table_scopes
    assert "CONDITION_OCCURRENCE.condition_start_date" in {
        s["scope"]["value"]
        for s in pack.schemas.data
        if s["scope"]["perimeter"] == "column"
    }
    # Every metric scoped inside a PRESENT table must have a home in
    # the tree -- including one on a column the source turned out to
    # lack, whose cdmField failure is exactly the finding to surface.
    # Metrics on an entirely absent table are deliberately not in the
    # tree (see reporting.build_schemas): there is nothing to browse
    # there, and the absence is reported at dataset level instead.
    schema_scopes = {s["scope"]["value"] for s in pack.schemas.data}
    for metric in pack.metrics.data:
        scope = metric["scope"]
        if scope["perimeter"] not in ("table", "column"):
            continue
        if scope["value"].split(".")[0] not in table_scopes:
            continue
        assert scope["value"] in schema_scopes, metric

    # ... and a missing column of a present table really is in there.
    assert "PERSON.care_site_id" not in mini_cdm_person_columns()
    assert "PERSON.care_site_id" in schema_scopes


def test_main_applies_threshold_overrides(tmp_path, monkeypatch):
    """A 100% threshold on isRequired must stop it ever failing.

    evaluate() fails a check when its violated percentage exceeds the
    threshold, so 100 can never be exceeded -- the fixture's planted
    NULL condition_concept_id stops being reported.
    """
    table_paths = write_mini_cdm(str(tmp_path))
    baseline = _run_main(monkeypatch, _StubPlatformPack(dict(table_paths)))
    assert any(
        key.startswith("isRequired_")
        for key in {m["key"] for m in baseline.metrics.data}
    )

    overridden = _run_main(
        monkeypatch,
        _StubPlatformPack(
            dict(table_paths),
            job={"threshold_overrides": {"isRequired": 100}},
        ),
    )
    assert not any(
        key.startswith("isRequired_")
        for key in {m["key"] for m in overridden.metrics.data}
    )


def test_main_rejects_an_unknown_check_in_threshold_overrides(
    tmp_path, monkeypatch
):
    pack = _StubPlatformPack(
        write_mini_cdm(str(tmp_path)),
        job={"threshold_overrides": {"isRequiredd": 5}},
    )
    with pytest.raises(ValueError, match="unknown check 'isRequiredd'"):
        _run_main(monkeypatch, pack)
    assert not pack.metrics.saved


def test_main_rejects_a_non_numeric_threshold_override(tmp_path, monkeypatch):
    pack = _StubPlatformPack(
        write_mini_cdm(str(tmp_path)),
        job={"threshold_overrides": {"isRequired": "loose"}},
    )
    with pytest.raises(ValueError, match="must be a number"):
        _run_main(monkeypatch, pack)
    assert not pack.metrics.saved
