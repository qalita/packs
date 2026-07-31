import polars as pl
import pytest


def test_known_tables_are_available(mini_cdm):
    assert mini_cdm.has_table("PERSON")
    assert mini_cdm.has_table("CONDITION_OCCURRENCE")


def test_table_lookup_is_case_insensitive(mini_cdm):
    assert mini_cdm.has_table("person")
    assert mini_cdm.has_table("Person")


def test_absent_table_is_reported_missing(mini_cdm):
    assert not mini_cdm.has_table("DRUG_EXPOSURE")


def test_table_returns_a_lazyframe(mini_cdm):
    frame = mini_cdm.table("PERSON")
    assert isinstance(frame, pl.LazyFrame)
    assert frame.select(pl.len()).collect().item() == 4


def test_requesting_a_missing_table_raises(mini_cdm):
    with pytest.raises(KeyError, match="DRUG_EXPOSURE"):
        mini_cdm.table("DRUG_EXPOSURE")


def test_columns_are_lowercased(mini_cdm):
    assert "person_id" in mini_cdm.columns("PERSON")
    assert "year_of_birth" in mini_cdm.columns("PERSON")


def test_dtypes_are_exposed_without_reading_rows(mini_cdm):
    dtypes = mini_cdm.dtypes("PERSON")
    assert dtypes["person_id"] == pl.Int64


def test_vocabulary_is_detected_when_present(mini_cdm):
    assert mini_cdm.has_vocabulary


def test_vocabulary_absence_is_detected(mini_cdm_no_vocabulary):
    assert not mini_cdm_no_vocabulary.has_vocabulary
    assert not mini_cdm_no_vocabulary.has_table("CONCEPT")


def test_available_tables_excludes_missing_ones(mini_cdm):
    assert "PERSON" in mini_cdm.available_tables
    assert "DRUG_EXPOSURE" not in mini_cdm.available_tables


def test_has_column_is_case_insensitive_on_both_arguments(mini_cdm):
    assert mini_cdm.has_column("person", "PERSON_ID")
    assert not mini_cdm.has_column("PERSON", "no_such_column")


def test_has_column_is_false_for_a_missing_table(mini_cdm):
    assert not mini_cdm.has_column("DRUG_EXPOSURE", "person_id")
