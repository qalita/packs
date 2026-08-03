"""Metric mapping of the profiling pack, on a generated parquet fixture.

The fixture is written as TWO part files on purpose: the previous
implementation profiled ``head(500_000)``, which on a chunked source reads only
the first parts. Asserting the statistics of the whole fixture is what makes
that regression impossible to reintroduce silently.
"""

import datetime as dt

import polars as pl
import pytest

import main


ROWS = 10

FIXTURE = {
    "id": list(range(1, ROWS + 1)),
    "score": [0.0, 1.0, 2.0, 3.0, -1.0, None, 5.0, 6.0, 7.0, None],
    "label": ["a", "b", "a", "c", "", None, "a", "bb", "ccc", None],
    "when": [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(ROWS)],
}


@pytest.fixture(scope="module")
def parts(tmp_path_factory):
    """The fixture split across two parquet parts, as a chunked source is."""
    directory = tmp_path_factory.mktemp("data")
    frame = pl.DataFrame(FIXTURE)
    paths = []
    for index, (offset, size) in enumerate([(0, 6), (6, 4)], start=1):
        path = directory / f"source_fixture_part_{index}.parquet"
        frame.slice(offset, size).write_parquet(path)
        paths.append(str(path))
    return paths


@pytest.fixture(scope="module")
def lazy(parts):
    return pl.scan_parquet(parts)


@pytest.fixture(scope="module")
def schema(lazy):
    return dict(lazy.collect_schema())


@pytest.fixture(scope="module")
def approximate(lazy, schema):
    options = main.read_options({})
    return main.profile_dataset(lazy, schema, options), options


@pytest.fixture(scope="module")
def exact(lazy, schema):
    options = main.read_options({"exact": True})
    return main.profile_dataset(lazy, schema, options), options


def index_by_column(metrics):
    """{(column, key): value} for column-scoped metrics."""
    return {
        (item["scope"]["value"], item["key"]): item["value"]
        for item in metrics
        if item["scope"]["perimeter"] == "column"
    }


def index_by_dataset(metrics):
    return {
        item["key"]: item["value"]
        for item in metrics
        if item["scope"]["perimeter"] == "dataset"
    }


def test_options_default_to_approximate():
    assert main.read_options({}) == {
        "exact": False,
        "top_k": main.DEFAULT_TOP_K,
        "high_cardinality": main.DEFAULT_HIGH_CARDINALITY,
    }
    assert main.read_options({"exact": True})["exact"] is True
    assert main.read_options({"job": {"exact": True}})["exact"] is True


def test_every_part_is_profiled(approximate):
    prof, _ = approximate
    assert {name: column["n"] for name, column in prof.items()} == {
        "id": ROWS,
        "score": ROWS,
        "label": ROWS,
        "when": ROWS,
    }
    # Row 10 lives in the second part: a head()-sampled profile would miss it.
    assert prof["id"]["max"] == ROWS


def test_column_metric_keys(approximate):
    prof, _ = approximate
    metrics = index_by_column(main.column_metrics(prof, "fixture"))

    common = {
        "type",
        "dtype",
        "n",
        "count",
        "n_missing",
        "p_missing",
        "completeness_score",
        "n_distinct",
        "p_distinct",
        "n_distinct_method",
        "n_unique",
        "p_unique",
        "is_unique",
        "is_unique_method",
    }
    for column in FIXTURE:
        emitted = {key for (name, key) in metrics if name == column}
        assert common <= emitted, column

    numeric = {
        "min",
        "max",
        "range",
        "sum",
        "mean",
        "std",
        "variance",
        "sample_stddev",
        "population_stddev",
        "sample_variance",
        "population_variance",
        "cv",
        "n_zeros",
        "p_zeros",
        "n_negative",
        "p_negative",
        "skewness",
        "kurtosis",
        "iqr",
        "n_infinite",
        "p_infinite",
        "5%",
        "25%",
        "50%",
        "75%",
        "95%",
        "percentile_10",
        "percentile_25",
        "percentile_75",
        "percentile_90",
    }
    assert numeric <= {key for (name, key) in metrics if name == "score"}

    text = {
        "min_length",
        "max_length",
        "mean_length",
        "median_length",
        "n_characters",
        "n_empty",
        "p_empty",
    }
    assert text <= {key for (name, key) in metrics if name == "label"}

    assert metrics[("when", "min")] == "2024-01-01"
    assert metrics[("when", "max")] == "2024-01-10"


def test_exact_column_values(approximate):
    prof, _ = approximate
    metrics = index_by_column(main.column_metrics(prof, "fixture"))

    assert metrics[("score", "n")] == "10"
    assert metrics[("score", "count")] == "8"
    assert metrics[("score", "n_missing")] == "2"
    assert metrics[("score", "p_missing")] == "0.2"
    assert metrics[("score", "completeness_score")] == "0.8"
    assert metrics[("score", "min")] == "-1"
    assert metrics[("score", "max")] == "7"
    assert metrics[("score", "range")] == "8"
    assert metrics[("score", "sum")] == "23"
    assert metrics[("score", "mean")] == "2.875"
    assert metrics[("score", "n_zeros")] == "1"
    assert metrics[("score", "n_negative")] == "1"
    assert metrics[("score", "n_infinite")] == "0"
    assert metrics[("score", "type")] == "Numeric"

    assert metrics[("label", "type")] == "Text"
    assert metrics[("label", "count")] == "8"
    assert metrics[("label", "n_empty")] == "1"
    assert metrics[("label", "min_length")] == "0"
    assert metrics[("label", "max_length")] == "3"
    assert metrics[("label", "n_characters")] == "10"

    # id is 1..10 across both parts: unique and complete. Monotonicity is no
    # longer reported — diff() cannot stream, and row order across part files
    # describes how the loader staged the data rather than the source.
    assert metrics[("id", "n_distinct")] == "10"
    assert metrics[("id", "is_unique")] == "1"
    assert metrics[("id", "completeness_score")] == "1"
    assert ("id", "monotonic_increase") not in metrics
    assert ("id", "ordering") not in metrics


def test_approximate_statistics_declare_their_method(approximate, exact):
    approximate_prof, _ = approximate
    metrics = index_by_column(main.column_metrics(approximate_prof, "fixture"))
    assert metrics[("id", "n_distinct_method")] == "hyperloglog"
    assert metrics[("id", "n_unique_method")] == "hyperloglog"
    assert metrics[("id", "is_unique_method")] == "hyperloglog"
    assert metrics[("score", "50%_method")] == "histogram"
    assert metrics[("score", "percentile_25_method")] == "histogram"
    assert metrics[("label", "median_length_method")] == "histogram"

    exact_prof, _ = exact
    exact_metrics = index_by_column(main.column_metrics(exact_prof, "fixture"))
    assert exact_metrics[("id", "n_distinct_method")] == "exact"
    assert exact_metrics[("score", "50%_method")] == "exact"
    assert exact_metrics[("score", "50%")] == "3"
    assert exact_metrics[("label", "median_length_method")] == "exact"


def test_dataset_metrics(approximate, schema):
    prof, _ = approximate
    summary = main.dataset_summary(prof, schema)
    metrics = index_by_dataset(
        main.dataset_metrics(summary, "fixture", database=None)
    )

    assert metrics["n"] == "10"
    assert metrics["n_var"] == "4"
    # 2 missing in `score` + 2 in `label`, over 10 x 4 cells.
    assert metrics["n_cells_missing"] == "4"
    assert metrics["p_cells_missing"] == "0.1"
    assert metrics["n_vars_with_missing"] == "2"
    assert metrics["n_vars_all_missing"] == "0"
    assert metrics["score"] == "0.9"
    assert metrics["types_numeric"] == "2"
    assert metrics["types_text"] == "1"
    assert metrics["types_datetime"] == "1"
    assert int(metrics["memory_size"]) > 0


def test_database_scope_carries_the_parent(approximate, schema):
    prof, _ = approximate
    summary = main.dataset_summary(prof, schema)
    entries = main.dataset_metrics(summary, "orders", database="warehouse")
    parent = entries[0]["scope"]["parent_scope"]
    assert parent == {"perimeter": "database", "value": "warehouse"}


def test_recommendations(approximate):
    prof, options = approximate
    summary = main.dataset_summary(prof, {name: pl.Int64 for name in FIXTURE})
    found = main.build_recommendations(prof, summary, "fixture", options)
    by_type = {}
    for item in found:
        by_type.setdefault(item["type"], set()).add(item["scope"]["value"])

    assert by_type["Missing"] == {"score", "label"}
    assert "id" in by_type["Unique"]
    for item in found:
        assert item["level"] in ("info", "warning", "high")
        assert item["scope"]["perimeter"] in ("column", "dataset")


def test_schemas(approximate):
    prof, _ = approximate
    entries = main.build_schemas(prof, "fixture")
    columns = [item["value"] for item in entries if item["key"] == "column"]
    datasets = [item["value"] for item in entries if item["key"] == "dataset"]
    assert columns == list(FIXTURE)
    assert datasets == ["fixture"]


def test_figures_are_bounded(approximate, schema):
    from qalita_core.figures import FiguresAsset

    prof, _ = approximate
    figures = FiguresAsset()
    figures.declare_measure("p_cells_missing")
    figures.declare_measure("p_missing")
    figures.declare_measure("n_columns")
    figures.declare_measure("count")

    summary = main.dataset_summary(prof, schema)
    main.add_figures(figures, prof, "fixture")

    keys = {figure["key"] for figure in figures.data["figures"]}
    assert "missing_by_column" in keys
    assert "column_types" in keys
    assert "top_values_label" in keys
    for figure in figures.data["figures"]:
        assert len(figure["rows"]) <= main.DEFAULT_TOP_K
        assert figure["truncated"] is False


def test_unbounded_keys_never_come_back(approximate, schema):
    """The keys the pack used to strip must not reappear as metrics.

    Each of them carries one entry per distinct value, word or character, which
    is exactly the unbounded payload the streaming rewrite removes.
    """
    prof, _ = approximate
    summary = main.dataset_summary(prof, schema)
    metrics = main.column_metrics(prof, "fixture") + main.dataset_metrics(
        summary, "fixture"
    )
    emitted = {item["key"] for item in metrics}
    assert not emitted & {
        "histogram",
        "histogram_length",
        "value_counts_without_nan",
        "value_counts_index_sorted",
        "word_counts",
        "character_counts",
        "script_counts",
        "script_char_counts",
        "first_rows",
        "category_alias_values",
    }
    for item in metrics:
        assert isinstance(item["value"], str)


def test_dataset_name_falls_back_to_the_source_name():
    class FakePack:
        source_config = {"name": "clients"}

    assert main.dataset_name_for(FakePack(), "file_clients", 1) == "clients"
    assert main.dataset_name_for(FakePack(), "db_orders", 3) == "db_orders"
