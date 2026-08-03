"""Test fixtures for the accuracy pack.

The pack is a script: ``main.py`` sits next to this directory and is imported
by name, which is why the pack root goes on ``sys.path`` here.
"""

import math
import sys
from pathlib import Path

import polars as pl
import pytest

PACK_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACK_ROOT))

from qalita_core.pack import Pack  # noqa: E402


@pytest.fixture
def parquet_parts(tmp_path):
    """A two-part parquet object, so chunked sources are actually exercised."""
    first = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "amount": [1.5, 2.25, 3.5],
            "latitude": [10.0, 95.0, -100.0],
            "label": ["a", "b", "c"],
        }
    )
    second = pl.DataFrame(
        {
            "id": [4, 5, 6],
            "amount": [4.5, None, math.nan],
            "latitude": [45.0, 1.0, 2.0],
            "label": ["d", "e", "f"],
        }
    )
    paths = []
    for index, frame in enumerate((first, second), start=1):
        path = tmp_path / f"src_orders_part_{index}.parquet"
        frame.write_parquet(path)
        paths.append(str(path))
    return paths


@pytest.fixture
def pack(parquet_parts, tmp_path, monkeypatch):
    """A Pack whose source is already 'loaded' with the fixture parts."""
    monkeypatch.chdir(tmp_path)
    instance = Pack()
    instance.source_config = {"name": "src", "type": "file"}
    instance.pack_config = {"job": {"id_columns": ["id"], "example_rows": 5}}
    instance.objects_source = {"orders": list(parquet_parts)}
    return instance
