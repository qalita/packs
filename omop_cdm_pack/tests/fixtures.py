"""Synthetic mini OMOP CDM used across the test suite."""

import os

import polars as pl

# PERSON: 4 people. person 4 has a NULL gender_concept_id.
PERSON = pl.DataFrame(
    {
        "person_id": [1, 2, 3, 4],
        "gender_concept_id": [8507, 8532, 8507, None],
        "year_of_birth": [1980, 1990, 2000, 1970],
        "month_of_birth": [1, 6, 12, None],
        "day_of_birth": [15, 1, 31, None],
        "birth_datetime": [
            "1980-01-15",
            "1990-06-01",
            "2000-12-31",
            "1970-01-01",
        ],
        "race_concept_id": [8527, 8527, 8516, 8527],
        "ethnicity_concept_id": [38003564] * 4,
    },
    schema_overrides={
        "person_id": pl.Int64,
        "gender_concept_id": pl.Int64,
        "birth_datetime": pl.Utf8,
    },
).with_columns(pl.col("birth_datetime").str.to_date())

# DEATH: person 3 died in 2020.
DEATH = pl.DataFrame(
    {
        "person_id": [3],
        "death_date": ["2020-01-01"],
        "death_type_concept_id": [32817],
    }
).with_columns(pl.col("death_date").str.to_date())

# VISIT_OCCURRENCE: 3 visits.
VISIT_OCCURRENCE = pl.DataFrame(
    {
        "visit_occurrence_id": [10, 11, 12],
        "person_id": [1, 2, 3],
        "visit_concept_id": [9201, 9202, 9201],
        "visit_start_date": ["2015-01-01", "2016-01-01", "2017-01-01"],
        "visit_end_date": ["2015-01-05", "2016-01-05", "2017-01-05"],
    }
).with_columns(
    pl.col("visit_start_date").str.to_date(),
    pl.col("visit_end_date").str.to_date(),
)

# CONDITION_OCCURRENCE: 6 rows, with deliberate violations.
#   row 1 (id 100) clean
#   row 2 (id 101) start after end               -> plausibleStartBeforeEnd
#   row 3 (id 102) date before birth             -> plausibleAfterBirth
#   row 4 (id 103) date after death (person 3)   -> plausibleBeforeDeath
#   row 5 (id 104) NULL condition_concept_id     -> isRequired, completeness
#   row 6 (id 104) duplicate id                  -> isPrimaryKey
#                  concept 99999 unknown         -> isForeignKey
CONDITION_OCCURRENCE = pl.DataFrame(
    {
        "condition_occurrence_id": [100, 101, 102, 103, 104, 104],
        "person_id": [1, 1, 1, 3, 2, 2],
        "condition_concept_id": [
            201826,
            201826,
            201826,
            201826,
            None,
            99999,
        ],
        "condition_start_date": [
            "2015-01-02",
            "2015-06-01",
            "1970-01-01",
            "2021-01-01",
            "2016-01-02",
            "2016-01-03",
        ],
        "condition_end_date": [
            "2015-01-03",
            "2015-05-01",
            "1970-01-02",
            "2021-01-02",
            "2016-01-03",
            "2016-01-04",
        ],
        "condition_source_value": ["A", "B", "C", "D", None, "F"],
        "visit_occurrence_id": [10, 10, 10, 12, 11, 11],
    },
    schema_overrides={"condition_concept_id": pl.Int64},
).with_columns(
    pl.col("condition_start_date").str.to_date(),
    pl.col("condition_end_date").str.to_date(),
)

# CONCEPT: minimal vocabulary. 99999 is deliberately absent.
CONCEPT = pl.DataFrame(
    {
        "concept_id": [201826, 8507, 8532, 9201, 9202, 4181412],
        "concept_name": [
            "Type 2 diabetes",
            "MALE",
            "FEMALE",
            "Inpatient visit",
            "Outpatient visit",
            "Deprecated concept",
        ],
        "domain_id": [
            "Condition",
            "Gender",
            "Gender",
            "Visit",
            "Visit",
            "Condition",
        ],
        "concept_class_id": [
            "Clinical Finding",
            "Gender",
            "Gender",
            "Visit",
            "Visit",
            "Clinical Finding",
        ],
        "standard_concept": ["S", "S", "S", "S", "S", None],
        "invalid_reason": [None, None, None, None, None, "D"],
    },
    schema_overrides={
        "standard_concept": pl.Utf8,
        "invalid_reason": pl.Utf8,
    },
)

CONCEPT_ANCESTOR = pl.DataFrame(
    {
        "ancestor_concept_id": [201826],
        "descendant_concept_id": [201826],
        "min_levels_of_separation": [0],
        "max_levels_of_separation": [0],
    }
)

TABLES = {
    "PERSON": PERSON,
    "DEATH": DEATH,
    "VISIT_OCCURRENCE": VISIT_OCCURRENCE,
    "CONDITION_OCCURRENCE": CONDITION_OCCURRENCE,
    "CONCEPT": CONCEPT,
    "CONCEPT_ANCESTOR": CONCEPT_ANCESTOR,
}

VOCABULARY_TABLE_NAMES = ("CONCEPT", "CONCEPT_ANCESTOR")


def write_mini_cdm(directory, include_vocabulary=True):
    """Write the mini CDM as parquet. Returns {TABLE_NAME: [paths]}."""
    os.makedirs(directory, exist_ok=True)
    table_paths = {}
    for name, frame in TABLES.items():
        if not include_vocabulary and name in VOCABULARY_TABLE_NAMES:
            continue
        path = os.path.join(directory, f"{name.lower()}_part_0.parquet")
        frame.write_parquet(path)
        table_paths[name] = [path]
    return table_paths
