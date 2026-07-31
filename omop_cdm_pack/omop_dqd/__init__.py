"""OMOP CDM data quality checks, ported from the OHDSI DataQualityDashboard.

See the NOTICE file at the pack root for attribution and licensing.
"""

import os

VENDOR_CSV_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "vendor", "csv"
)

SUPPORTED_CDM_VERSIONS = ("5.3", "5.4")
