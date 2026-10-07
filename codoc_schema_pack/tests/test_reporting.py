import polars as pl
import pytest

from codoc_model.checks import check_table, missing_table
from codoc_model.reporting import (
    build_metrics,
    build_recommendations,
    build_schemas,
    overall_score,
)
from codoc_model.spec import load_model

MODEL = load_model()


@pytest.fixture(scope="module")
def results():
    site = MODEL.tables["dwh_thesaurus_site"]
    frame = pl.DataFrame(
        {
            "site_num": [1, 1],
            "site_code": ["A", None],
            "site_str": ["x", "y"],
            "instance_id": [1, 2],
            "extra": [1, 2],
        }
    ).lazy()
    return [
        check_table(site, frame),
        missing_table(MODEL.tables["hospital_instance"]),
    ]


def test_overall_score_weighs_tables_equally(results):
    assert overall_score(results) == pytest.approx(results[0].score / 2)


def test_metrics_values_and_scopes(results):
    metrics = build_metrics(results, "src")
    by_key = {(m["key"], m["scope"]["value"]): m["value"] for m in metrics}
    assert by_key[("tables_missing", "src")] == 1
    assert (
        by_key[("column_missing_count", "src")] == 2
    )  # upload_id, update_date
    assert by_key[("null_violations", "dwh_thesaurus_site.site_code")] == 1
    assert (
        by_key[("primary_key_violations", "dwh_thesaurus_site.site_num")] == 1
    )
    assert by_key[("score", "hospital_instance")] == "0.0"
    assert isinstance(by_key[("score", "src")], str)
    keys = [(m["key"], str(m["scope"])) for m in metrics]
    assert len(keys) == len(set(keys)), "key+scope must identify a metric"


def test_recommendations_group_columns_per_table(results):
    recs = build_recommendations(results, "src", ["file_notes"])
    contents = [r["content"] for r in recs]
    assert any("hospital_instance is missing" in c for c in contents)
    [missing] = [c for c in contents if "lacks" in c]
    assert "optional: upload_id, update_date" in missing
    assert any("extra" in c for c in contents)
    assert any("file_notes" in c for c in contents)
    assert {r["level"] for r in recs} <= {"high", "warning", "info"}


def test_schemas_cover_present_tables_and_model_columns(results):
    schemas = build_schemas(results, "src")
    values = [s["value"] for s in schemas]
    assert "dwh_thesaurus_site" in values
    assert "hospital_instance" not in values
    assert "dwh_thesaurus_site.update_date" in values  # missing, still a node
    assert "dwh_thesaurus_site.extra" in values
