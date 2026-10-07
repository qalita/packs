import pytest

from codoc_model.sources import load_file_tables, match_table
from codoc_model.spec import load_model

TABLES = list(load_model().tables)


def test_match_table_prefers_the_longest_name():
    assert (
        match_table("file_dwh_thesaurus_data", TABLES) == "dwh_thesaurus_data"
    )
    assert match_table("file_dwh_data", TABLES) == "dwh_data"
    assert (
        match_table("postgresql_public_dwh_patient", TABLES) == "dwh_patient"
    )
    assert match_table("dwh_patient_stay", TABLES) == "dwh_patient_stay"
    assert match_table("HOSPITAL_INSTANCE", TABLES) == "hospital_instance"
    assert match_table("patients", TABLES) is None


class FakePack:
    """What qalita_core >= 2.1.3 leaves on a Pack after load_data."""

    def __init__(self, source_type, path, objects, skipped=()):
        self.source_config = {
            "type": source_type,
            "name": "src",
            "config": {"path": path},
        }
        self._objects = objects
        self._skipped = list(skipped)
        self.objects_source = {}
        self.skipped_source_objects = []
        self.load_calls = 0

    def load_data(self, trigger):
        self.load_calls += 1
        self.objects_source = dict(self._objects)
        self.skipped_source_objects = list(self._skipped)
        return [p for parts in self._objects.values() for p in parts]


def test_folder_objects_map_to_tables_in_one_load(tmp_path):
    pack = FakePack(
        "folder",
        str(tmp_path),
        {
            "file_dwh_patient": ["p1", "p2"],
            "file_hospital_instance": ["h1"],
            "file_notes": ["n1"],
        },
        skipped=[{"object": "broken.csv", "error": "ComputeError"}],
    )
    loaded = load_file_tables(pack, TABLES)
    assert pack.load_calls == 1
    assert loaded.tables == {
        "dwh_patient": ["p1", "p2"],
        "hospital_instance": ["h1"],
    }
    assert loaded.unmatched == ["notes"]
    assert loaded.skipped == [
        {"object": "broken.csv", "error": "ComputeError"}
    ]


def test_declared_table_wins_for_a_single_file(tmp_path):
    source = tmp_path / "export_2025.csv"
    source.write_text("x")
    pack = FakePack("file", str(source), {"file_export_2025": ["a"]})
    loaded = load_file_tables(pack, TABLES, "dwh_patient_mvt")
    assert loaded.tables == {"dwh_patient_mvt": ["a"]}
    assert loaded.unmatched == []


def test_file_source_on_a_directory_points_to_folder(tmp_path):
    # qalita_core would read the directory's first file only, silently.
    pack = FakePack("file", str(tmp_path), {"file_dwh_patient": ["a"]})
    with pytest.raises(ValueError, match="type 'folder'"):
        load_file_tables(pack, TABLES)
    assert pack.load_calls == 0
