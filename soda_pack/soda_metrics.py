"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Naming rules shared between the DuckDB view and the Soda scan results.

The view renames columns so SodaCL can address them, and Soda then reports its
metrics under those renamed columns. Both halves of that round trip live here
so they cannot drift apart, and so they can be tested without a scan.
"""

from __future__ import annotations

from typing import Iterable

from qalita_core.utils import slugify

__all__ = ["DATA_SOURCE_NAME", "column_aliases", "metric_column"]

# Soda builds metric identities as
# "metric-<data source>-<dataset>-<column>-<metric>". A constant, dash-free
# data source name keeps that string parseable whatever the source is called.
DATA_SOURCE_NAME = "qalita"


def column_aliases(columns: Iterable[str]) -> dict[str, str]:
    """Map every source column to the slug SodaCL will address it by.

    SodaCL check definitions cannot quote arbitrary identifiers, so a column
    named ``Order Date`` has to become ``order_date``. Slugs that collide keep
    the original name rather than shadowing another column — the pandas
    ``rename`` this replaces silently produced two columns with one name.
    """
    aliases: dict[str, str] = {}
    taken: set[str] = set()
    for column in columns:
        alias = slugify(column) or column
        if alias in taken:
            alias = column
        taken.add(alias)
        aliases[column] = alias
    return aliases


def metric_column(
    identity: str,
    dataset: str,
    metric_name: str,
    data_source: str = DATA_SOURCE_NAME,
) -> str | None:
    """Column a Soda metric identity refers to, or None if dataset-scoped.

    Peeling the known prefix and suffix off is what makes this survive names
    containing a dash; splitting on "-" and taking element 3, as the pack used
    to, mislabelled every dataset whose name held one.
    """
    prefix = f"metric-{data_source}-{dataset}-"
    suffix = f"-{metric_name}"
    if not identity.startswith(prefix) or not identity.endswith(suffix):
        return None
    middle = identity[len(prefix) : -len(suffix)]
    return middle or None
