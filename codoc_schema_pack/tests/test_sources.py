import os

from codoc_model.sources import cleanup_list, load_file_tables, match_table
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
    """Records which file each load_data call was pointed at."""

    def __init__(self, path):
        self.source_config = {
            "type": "folder",
            "name": "src",
            "config": {"path": path},
        }
        self.paths_source = []
        self.loaded_files = []

    def load_data(self, trigger):
        path = self.source_config["config"]["path"]
        self.loaded_files.append(os.path.basename(path))
        return [path + ".staged.parquet"]


def test_directory_files_are_loaded_one_by_one(tmp_path):
    for name in (
        "dwh_patient.csv",
        "hospital_instance.parquet",
        "notes.csv",
        "readme.md",
    ):
        (tmp_path / name).write_text("x")
    pack = FakePack(str(tmp_path))
    loaded, unmatched = load_file_tables(pack, TABLES)
    assert sorted(loaded) == ["dwh_patient", "hospital_instance"]
    assert unmatched == ["notes.csv"]
    assert sorted(pack.loaded_files) == [
        "dwh_patient.csv",
        "hospital_instance.parquet",
    ]
    assert pack.source_config["type"] == "folder"  # restored after each load


def test_declared_table_wins_for_a_single_file(tmp_path):
    (tmp_path / "export_2025.csv").write_text("x")
    pack = FakePack(str(tmp_path / "export_2025.csv"))
    loaded, unmatched = load_file_tables(pack, TABLES, "dwh_patient_mvt")
    assert list(loaded) == ["dwh_patient_mvt"]
    assert unmatched == []


def test_cleanup_never_lists_a_source_file(tmp_path):
    source = str(tmp_path / "dwh_patient.parquet")
    staged = str(tmp_path / "parquet" / "file_dwh_data_part_1.parquet")
    loaded = {"dwh_patient": [source], "dwh_data": [staged]}
    assert cleanup_list(loaded, [source]) == [staged]
