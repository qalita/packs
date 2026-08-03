"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Test fixtures for data_drift_pack.
"""

import json
import os
import sys

import polars as pl
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qalita_core.pack import Pack  # noqa: E402


def write_parts(directory, name, frames):
    """Write one logical object as several parquet parts.

    Several parts is the normal shape of a chunked load, and the whole point of
    these tests: a pack that reads ``paths[0]`` sees only the first one.
    """
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, frame in enumerate(frames, start=1):
        path = directory / f"{name}_part_{index}.parquet"
        frame.write_parquet(path)
        paths.append(str(path))
    return paths


@pytest.fixture
def make_pack(tmp_path, monkeypatch):
    def _make(pack_conf, source_objects, target_objects=None):
        monkeypatch.chdir(tmp_path)
        conf = tmp_path / "conf"
        conf.mkdir(exist_ok=True)
        (conf / "pack_conf.json").write_text(json.dumps(pack_conf))
        (conf / "source_conf.json").write_text(
            json.dumps({"name": "reference", "type": "file", "config": {}})
        )
        (conf / "target_conf.json").write_text(
            json.dumps({"name": "current", "type": "file", "config": {}})
        )

        pack = Pack(
            configs={
                "pack_conf": str(conf / "pack_conf.json"),
                "source_conf": str(conf / "source_conf.json"),
                "target_conf": str(conf / "target_conf.json"),
                "agent_file": str(conf / ".worker"),
            }
        )
        pack.objects_source = dict(source_objects)
        pack.paths_source = [
            path for parts in source_objects.values() for path in parts
        ]
        pack.objects_target = dict(target_objects or {})
        pack.paths_target = [
            path for parts in (target_objects or {}).values() for path in parts
        ]

        def _already_loaded(trigger, table_or_query=None):
            return (
                pack.paths_source if trigger == "source" else pack.paths_target
            )

        pack.load_data = _already_loaded
        return pack

    return _make


def frame(values, column="amount"):
    return pl.DataFrame({column: values})
