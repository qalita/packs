"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Column naming has to survive a round trip: the view renames columns so SodaCL
can address them, and the metric identities Soda returns are then parsed back
into the ORIGINAL names the platform shows. Getting either half wrong silently
attributes a metric to the wrong column.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import soda_metrics  # noqa: E402


def test_aliases_slugify_addressable_names():
    aliases = soda_metrics.column_aliases(["id", "Order Date", "Prénom"])
    assert aliases == {
        "id": "id",
        "Order Date": "order_date",
        "Prénom": "prenom",
    }


def test_colliding_slugs_keep_their_own_column():
    # "Order Date" and "order-date" both slugify to "order_date". The pandas
    # rename this replaces produced two columns called "order_date"; here the
    # second keeps its original name so no column is shadowed.
    aliases = soda_metrics.column_aliases(["Order Date", "order-date"])
    assert aliases["Order Date"] == "order_date"
    assert aliases["order-date"] == "order-date"
    assert len(set(aliases.values())) == 2


def test_dataset_scoped_metric_has_no_column():
    identity = "metric-qalita-file_orders-row_count"
    assert (
        soda_metrics.metric_column(identity, "file_orders", "row_count")
        is None
    )


def test_column_scoped_metric_names_its_column():
    identity = "metric-qalita-file_orders-order_date-missing_count"
    assert (
        soda_metrics.metric_column(identity, "file_orders", "missing_count")
        == "order_date"
    )


def test_dashes_in_the_column_do_not_shift_the_parse():
    # Splitting the identity on "-" and taking element 3, as the pack used to,
    # returns "order" here instead of the whole column name.
    identity = "metric-qalita-file_orders-order-date-missing_count"
    assert (
        soda_metrics.metric_column(identity, "file_orders", "missing_count")
        == "order-date"
    )


def test_unrecognised_identity_is_not_guessed():
    assert (
        soda_metrics.metric_column(
            "metric-other-somewhere_else-row_count",
            "file_orders",
            "row_count",
        )
        is None
    )
