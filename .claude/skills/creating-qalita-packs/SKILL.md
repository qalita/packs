---
name: creating-qalita-packs
description: Use when creating a new QALITA pack in this repo, adding files to a pack, or asking how packs are structured, configured, versioned, or published
---

# Creating QALITA Packs

A pack is a standalone folder `<name>_pack/` at repo root, run by the QALITA agent via `run.sh` → `python main.py`. CI publishes **every root dir containing `pack_conf.json`** with `qalita pack push` (`.github/workflows/publish.yml`), so a half-finished pack dir on `main` gets published.

## Required files

| File | How to produce |
|---|---|
| `properties.yaml` | Write from template below |
| `pack_conf.json` | Write from template below |
| `main.py` | Pack logic (pattern below) |
| `pyproject.toml` | Template below, then `uv lock` to generate `uv.lock` |
| `run.sh` | Copy **verbatim** from `scripts/run.sh` + `chmod +x`. Never edit per-pack: CI overwrites it with `scripts/run.sh` at publish |
| `README.md` | Sections: Overview, Configuration, Usage, Outputs, Contribute (copy structure from an existing pack) |
| `LICENSE` | Copy from `duplicates_finder_pack/LICENSE` (QALITA proprietary text — yes, despite AGENTS.md saying Apache 2.0; match existing packs) |
| `icon.png` | Required (`properties.yaml` references it); copy a placeholder from another pack until design provides one |

## properties.yaml

```yaml
compatible_sources:        # subset of: mysql, postgresql, sqlite, oracle, file, csv, excel, folder
- file
- csv
description: One-sentence description shown on the platform.
icon: icon.png
name: null_rate            # bare name, NO _pack suffix (dir = <name>_pack)
tags: [Data Quality]       # free-form domain tags
type: completeness         # quality dimension: accuracy|completeness|consistency|interoperability|reasonability|schema|timeliness|uniqueness|validity
url: https://github.com/qalita-io/packs/tree/main/null_rate_pack
version: 1.0.0             # THE published version; bumped by scripts/bump_pack_versions.sh
visibility: public
```

`properties.yaml version` is the only version that matters to the platform. `pyproject.toml version` is not bumped (existing packs are stale at 0.1.0/1.0.0 — leave it).

## pack_conf.json

```json
{
    "job": { "source": { "skiprows": 0 } },
    "charts": { "overview": [
        { "metric_key": "score", "chart_type": "text", "display_title": true, "justify": true },
        { "metric_key": "score", "chart_type": "bar_chart", "display_title": true, "justify": false }
    ] }
}
```

`job` holds pack-specific defaults read via `pack.pack_config["job"]`. Chart entries use exactly these four keys; `chart_type` is `text` or `bar_chart`. Emit a `score` metric (0–1, stringified) — it is the conventional headline.

## pyproject.toml

```toml
[project]
name = "null-rate-pack"
version = "1.0.0"
description = "Pack null-rate computes per-column null rates"
authors = [{name = "QALITA SAS", email = "contact@qalita.io"}]
license = {text = "Proprietary"}
readme = "README.md"
requires-python = ">=3.10,<4.0"
dependencies = ["qalita-core>=1.5.0", "pyarrow>=19.0.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`requires-python` and `dependencies` are load-bearing: `run.sh` picks the interpreter from `requires-python`, then `uv lock` + install.

## main.py pattern

Module-level code inside `with Pack() as pack:` (from `qalita_core.pack`):

```python
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level
import pandas as pd

with Pack() as pack:
    if pack.source_config.get("type") == "database":
        toq = pack.source_config.get("config", {}).get("table_or_query")
        if not toq:
            raise ValueError("database source requires 'table_or_query' in config")
        pack.load_data("source", table_or_query=toq)
    else:
        pack.load_data("source")

    # pack.df_source may be: a DataFrame, a list of DataFrames,
    # or parquet path string(s) (chunked big-data mode) — handle all three.
    # Normalize to (dataset_label, df) items; read paths with pd.read_parquet.

    pack.metrics.data.append({
        "key": "score", "value": str(round(score, 2)),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    })
    # column-scoped metrics add parent_scope:
    # {"perimeter": "column", "value": col,
    #  "parent_scope": {"perimeter": "dataset", "value": dataset_label}}

    pack.recommendations.data.append({
        "content": "human-readable finding",
        "type": "ShortCategory",
        "scope": {"perimeter": "dataset", "value": dataset_label},
        "level": determine_recommendation_level(rate),  # rate = badness 0-1
    })

    pack.metrics.save()             # writes metrics.json
    pack.recommendations.save()     # writes recommendations.json
```

Conventions: fractional metric values stringified (`str(round(x, 2))`), counts stay `int`. Packs comparing two datasets also use `pack.load_data("target")` / `pack.df_target`. See `duplicates_finder_pack/main.py` for a full example including chunked-parquet aggregation via `qalita_core.aggregation`.

## Verify and ship

1. `cd <pack>_pack && uv lock` — commit `uv.lock`.
2. Test: `cd tests && ./test_one_pack.sh <name>_pack [dataset]` (datasets live in `tests/data/`, needs a `source_conf.json` per dataset). Outputs land in `tests/data/<dataset>/output/`.
3. Format/lint: `black .` then `pylint .` in the pack dir.
4. Add the pack to the Architecture tree in `AGENTS.md`.
5. Commit `feat: add <name>_pack` — merging to `main` publishes it via CI.

## Common mistakes

- Editing `run.sh` inside a pack — CI replaces it with `scripts/run.sh`; put fixes in `scripts/run.sh` for all packs.
- Putting `_pack` in `properties.yaml name` — the name is bare; the directory adds `_pack`.
- Bumping `pyproject.toml` version instead of `properties.yaml` — only `properties.yaml` is published.
- Using `license = {text = "Apache-2.0"}` — existing packs declare `Proprietary` with the QALITA LICENSE file.
- Numeric metric `value` as float — the platform expects stringified fractions.
