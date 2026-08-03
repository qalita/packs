"""Derived from OHDSI DataQualityDashboard, Apache License 2.0.

Reimplements in Polars the SQL templates of inst/sql/sql_server/.
See the NOTICE file at the pack root.

Importing this package registers every check implementation.
"""

from omop_dqd.checks import (  # noqa: F401
    concept_level,
    field_level,
    table_level,
)
