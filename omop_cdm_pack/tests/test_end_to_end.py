"""End-to-end tests: the full pipeline, and the loader's per-table wiring.

Bypasses qalita_core.pack.Pack for the pipeline tests (it needs platform
config that does not exist in this test environment) and drives
catalog -> runner -> reporting directly. loader.py is exercised
separately against a stub standing in for Pack, since it is the one
piece of genuinely new logic this task adds.
"""

import omop_dqd.checks  # noqa: F401  (registers every check)
from omop_dqd.catalog import load_catalog
from omop_dqd.loader import catalog_table_names, load_cdm_tables
from omop_dqd.reporting import build_metrics, build_recommendations
from omop_dqd.results import CheckStatus
from omop_dqd.runner import run_checks

from tests.fixtures import write_mini_cdm


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
