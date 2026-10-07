"""Regenerate ``model.json`` from a checkout of the codoc data model docs.

Development tool, not run by the pack. Usage::

    git clone https://github.com/codoc-health/codoc-data-model-docs /tmp/codoc
    python codoc_model/build_model.py /tmp/codoc > codoc_model/model.json

Only the structural facts of each column are kept (name, datatype, length,
required, primary key, foreign key). The prose of the upstream docs -- user
guide and ETL conventions -- is deliberately left out: the pack does not need
it, and the reader is pointed to the upstream site instead.

Upstream quirks handled here:

* blank lines separate column groups inside a table CSV;
* a few rows close a quoted field too early (``"... (e.g. *a;b*"),varchar``),
  so a row is rebuilt from its last six fields, which never contain prose;
* ``dwh_data`` is split over ``csv/dwh_data/``: the core table plus the PMSI
  and drug *extension* field groups, which a warehouse adds only when it
  carries that kind of data.
"""

import csv
import json
import re
import subprocess
import sys
from pathlib import Path

REPOSITORY = "https://github.com/codoc-health/codoc-data-model-docs"
DATATYPE = re.compile(r"^\s*([a-z]+)\s*(?:\((\d+)\))?\s*$")
EXTENSIONS = {"dwh_data_pmsi": "pmsi", "dwh_data_drugs": "drugs"}


def _yes(value: str) -> bool:
    return value.strip().lower() == "yes"


def parse_columns(path: Path) -> list:
    """Every column of one upstream CSV, in file order."""
    columns = []
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    for row in rows[1:]:
        if not any(cell.strip() for cell in row):
            continue
        name = row[0].strip()
        # Datatype, Required, Primary Key, Foreign Key, FK Table are the last
        # five fields and never hold prose, so they survive a broken quote in
        # the free-text fields before them.
        datatype, required, primary_key, foreign_key, fk_table = (
            cell.strip() for cell in row[-5:]
        )
        match = DATATYPE.match(datatype)
        if not match:
            raise ValueError(f"{path.name}: {name}: unparsable {datatype!r}")
        columns.append(
            {
                "name": name,
                "datatype": match.group(1),
                "length": int(match.group(2)) if match.group(2) else None,
                "required": _yes(required),
                "primary_key": _yes(primary_key),
                "foreign_key": fk_table if _yes(foreign_key) else None,
            }
        )
    return columns


def table_domains(docs: Path) -> dict:
    """Table name -> documentation section (clinical_data, vocabularies...)."""
    return {
        page.stem: page.parent.name
        for page in (docs / "tables").glob("*/*.md")
    }


def build(checkout: Path) -> dict:
    docs = checkout / "docs"
    domains = table_domains(docs)
    tables = {}
    for path in sorted((docs / "csv").rglob("*.csv")):
        if path.stem in EXTENSIONS:
            continue
        tables[path.stem] = {
            "domain": domains.get(path.stem),
            "columns": parse_columns(path),
            "extensions": {},
        }
    for stem, extension in EXTENSIONS.items():
        path = docs / "csv" / "dwh_data" / f"{stem}.csv"
        tables["dwh_data"]["extensions"][extension] = parse_columns(path)

    commit = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return {
        "source": {"repository": REPOSITORY, "commit": commit},
        "tables": tables,
    }


if __name__ == "__main__":
    json.dump(build(Path(sys.argv[1])), sys.stdout, indent=2)
    sys.stdout.write("\n")
