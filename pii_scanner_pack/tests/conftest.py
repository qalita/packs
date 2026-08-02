"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Test fixtures for pii_scanner_pack.
"""

import json
import os
import sys

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
    def _make(pack_conf, source_objects):
        monkeypatch.chdir(tmp_path)
        conf = tmp_path / "conf"
        conf.mkdir(exist_ok=True)
        (conf / "pack_conf.json").write_text(json.dumps(pack_conf))
        (conf / "source_conf.json").write_text(
            json.dumps({"name": "customers", "type": "file", "config": {}})
        )
        (conf / "target_conf.json").write_text(json.dumps({}))

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
        pack.load_data = lambda trigger, table_or_query=None: pack.paths_source
        return pack

    return _make
