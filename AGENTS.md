# AGENTS.md — Qalita Public Packs

Instructions for AI agents working on this repository.

## Project

**Qalita Packs** — Open source collection of data quality analysis packs for the Qalita platform.

- **Organization** : `qalita`
- **License** : Apache 2.0
- **Visibility** : Public
- **Runtime** : Python >= 3.10

## Tech Stack

| Component | Technologies |
|-----------|-------------|
| **Runtime** | Python >= 3.10 |
| **Data processing** | pandas, numpy |
| **Quality frameworks** | Great Expectations, Soda Core, dbt Core |
| **Healthcare** | FHIR standards |
| **Core dependency** | qalita_core (PyPI) |
| **Linting** | Black, Pylint, Flake8 |

## Dependencies

Each pack has its own `pyproject.toml` or `requirements.txt`. Common dependencies:
- `qalita_core>=0.1.0`
- `pandas>=2.0`, `numpy>=1.24`
- Pack-specific: `great-expectations`, `soda-core`, `dbt-core`, `fhirclient`

## Build/Lint/Test Commands

```bash
# Install qalita_core (dependency for all packs)
pip install qalita_core

# Run tests (if available)
cd <pack_directory> && python -m pytest tests/ -v

# Lint a specific pack
cd <pack_directory> && black . --check
cd <pack_directory> && pylint .

# Format code
cd <pack_directory> && black .

# Version management
./scripts/bump_pack_versions.sh
./scripts/push_all_packs.sh
```

## Code Conventions

- Each pack is a standalone folder at root level
- Packs use `qalita_core` as dependency for data access
- **License** : Include Apache 2.0 header in all new files
- **Formatter** : Black
- **Linting** : Pylint, Flake8
- **Tests** : pytest (when applicable)
- **Naming** : `<name>_pack/` for pack directories
- **Imports** : Use `qalita_core` abstractions for data sources

## Architecture

```
packs/
├── profiling_pack/              # Data profiling
├── duplicates_finder_pack/      # Duplicate detection
├── outlier_detection_pack/      # Outlier detection
├── numeric_validation_pack/     # Numeric validation
├── text_validation_pack/        # Text validation
├── pattern_validation_pack/     # Pattern validation
├── schema_scanner_pack/         # Schema scanning
├── pii_scanner_pack/            # PII detection
├── referential_integrity_pack/  # Referential integrity
├── accepted_values_pack/        # Accepted values
├── accuracy_pack/               # Data accuracy
├── data_compare_pack/           # Dataset comparison
├── data_drift_pack/             # Drift detection
├── timeliness_pack/             # Data freshness
├── fhir_compliance_pack/        # FHIR compliance
├── great_expectations_pack/     # Great Expectations integration
├── soda_pack/                   # Soda integration
├── dbt_checks_pack/             # dbt integration
├── scripts/                     # Utility scripts
│   ├── bump_pack_versions.sh
│   └── push_all_packs.sh
└── tests/                       # Tests
```

## Git Workflow

- **Tags** : Strict semver `X.Y.Z` (⚠️ NO `v` prefix)
- **Commits** : English, conventional commits (`feat:`, `fix:`, `chore:`)
- **Branches** : `main` (prod), feature branches for development

## Creating a New Pack

**REQUIRED SKILL:** Before creating a new pack (or modifying pack structure/config/versioning), install the `qalita-pack-creation` plugin from the [`qalita/skills`](https://github.com/qalita/skills) marketplace — it documents required files, config templates, the `main.py` pattern, versioning, and publishing.

```
/plugin marketplace add qalita/skills
/plugin install qalita-pack-creation@qalita-skills
```

## Rules

- ❌ Do not modify existing pack structure without understanding user impact
- ✅ New packs must follow existing pack structure
- ✅ Document inputs/outputs of each pack
- ✅ Include Apache 2.0 license header in new files
