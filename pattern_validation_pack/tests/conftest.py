"""Test fixtures for the pattern validation pack.

The pack is a script: ``main.py`` sits next to this directory and is imported
by name, which is why the pack root goes on ``sys.path`` here.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

PACK_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACK_ROOT))

from qalita_core.pack import Pack  # noqa: E402

VALID_UUID = "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def parquet_parts(tmp_path):
    """A two-part parquet object, so chunked sources are actually exercised."""
    first = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "email": ["a@b.co", "bad", None],
            "user_id": [VALID_UUID, "nope", None],
            "ip_address": ["10.0.0.1", "999.1.1.1", None],
            "code": ["aa", "ab", "cc"],
        }
    )
    second = pl.DataFrame(
        {
            "id": [4, 5, 6],
            "email": ["", "x@y.zz", "a@b"],
            "user_id": [VALID_UUID, "", "1234"],
            "ip_address": ["1.2.3.4", "", "abc"],
            "code": ["dd", "de", "ff"],
        }
    )
    paths = []
    for index, frame in enumerate((first, second), start=1):
        path = tmp_path / f"src_users_part_{index}.parquet"
        frame.write_parquet(path)
        paths.append(str(path))
    return paths


@pytest.fixture
def pack(parquet_parts, tmp_path, monkeypatch):
    """A Pack whose source is already 'loaded' with the fixture parts."""
    monkeypatch.chdir(tmp_path)
    instance = Pack()
    instance.source_config = {"name": "src", "type": "file"}
    instance.pack_config = {
        "job": {
            "id_columns": ["id"],
            "example_rows": 5,
            "patterns": [
                {"column": "email", "type": "email"},
                {"column": "user_id", "type": "uuid"},
                {"column": "ip_address", "type": "ipv4"},
            ],
        }
    }
    instance.objects_source = {"users": list(parquet_parts)}
    return instance
