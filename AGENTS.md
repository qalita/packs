# AGENTS.md — Qalita Public Packs

Instructions for AI agents working on this repository.

## Project

**Qalita Packs** — Open source collection of data quality analysis packs for the Qalita platform.

- **Organization** : `qalita`
- **License** : Proprietary (QALITA Software License Agreement)
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
- **License** : New files are proprietary QALITA by default; files deriving from
  third-party Apache-2.0 material (e.g. vendored OHDSI content) carry that
  attribution and are listed in the pack's `NOTICE`
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
├── omop_cdm_pack/               # OMOP CDM quality (OHDSI DQD port)
├── great_expectations_pack/     # Great Expectations integration
├── soda_pack/                   # Soda integration
├── dbt_checks_pack/             # dbt integration
├── scripts/                     # Utility scripts
│   ├── bump_pack_versions.sh
│   └── push_all_packs.sh
└── tests/                       # Tests
```

## Figures — expliquer les métriques

`metrics.json` porte les chiffres. `figures.json` porte ce qui les explique.

Un pack déclare une **intention**, jamais un type de graphe : la plateforme
choisit la forme, les couleurs et les seuils tracés.

```python
pack.figures.declare_measure(
    "p_missing", unit="ratio", direction="lower_is_better", target=0.05
)
pack.figures.add(
    "missing_by_column",
    intent="breakdown",          # ce qui contribue au chiffre
    of="p_cells_missing",        # la métrique que cette figure décompose
    frame=df,
    dims=["column"],
    measures=["p_missing"],
    scope={"perimeter": "dataset", "value": name},
)
pack.figures.save()
```

Intentions : `breakdown`, `composition`, `distribution`, `trend`, `comparison`,
`matrix`, `flow`.

Voir `profiling_pack/main.py` (`breakdown` + `composition` hors statut) et
`numeric_validation_pack/main.py` (`composition` de statuts pass/fail +
`breakdown`) pour deux exemples réels et complets.

Règles :
- **Agrégats uniquement.** Jamais de ligne de données source — c'est une contrainte
  de conformité, pas une préférence. `add()` rejette un `dims` vide, un `measures`
  vide, et un tuple de dimensions dupliqué (signe d'un frame non agrégé).
- **Ces garde-fous ne couvrent pas tout.** Un agrégat clé par un identifiant
  unique — `dims=["patient_id"], measures=["age"]` — n'a par construction aucun
  tuple dupliqué : il passe les trois contrôles et part en clair, 5000 lignes
  patient tronquées. Rien ne remplace la règle : ne passez à `add` qu'un frame
  que vous avez vous-même agrégé. Sur de la donnée de santé, ne pas s'y tromper.
- **Plafond 5 000 lignes.** Au-delà, la figure est tronquée et signalée. Pour replier
  la queue proprement, utiliser `qalita_core.figures.top_n(frame, by, n, other=False,
  label="Autres", dim=None)` — et seulement sur une mesure additive : sommer des
  ratios donne un chiffre faux (`other` est `False` par défaut pour cette raison).
  `dim` nomme la colonne qui reçoit le libellé de repli ; elle n'est déduite que si
  une seule colonne hors `by` existe, sinon `top_n` lève plutôt que de deviner.
- **`save()` vérifie tout.** Toute mesure citée par `of=` ou par `measures=` doit
  avoir été déclarée via `declare_measure` — sinon `save()` lève et le run du pack
  échoue. Ce n'est pas une préférence de style : sans déclaration, la figure ne
  sera jamais reliée à son chiffre.
- **`of` crée le drill-down.** Une métrique avec figures devient cliquable.
- `trend` est le temps **dans la donnée**. L'historique entre les runs appartient à
  la plateforme, pas au pack.
- **pandas et polars divergent sur `top_n`.** Une colonne de dimension entière
  repliée ressort en `["1", "2", "Autres"]` sous polars mais `[1, 2, "Autres"]`
  sous pandas (`dim` mélange forcément des types avec le libellé). C'est voulu
  et couvert par les tests ; le JSON encaisse les deux, mais ne pas être surpris.

Le champ `charts` de `pack_conf.json` est **gelé** : il fonctionne toujours, mais
aucun nouveau type ne lui sera ajouté. Les nouveaux packs utilisent `figures.json`.

### Le piège du chunking

`qalita_core` découpe tout CSV au-delà de `chunk_rows` (défaut 100 000). Un pack
qui boucle par chunk et appelle `figures.add` (ou accumule) à chaque itération
sans agréger produit une ligne par *exécution* de règle plutôt que par colonne —
tuples de dimensions dupliqués, crash dans `add()`. C'est arrivé sur
`numeric_validation_pack`. Deux parades légitimes :
- **Accumuler dans un dict clé par la dimension**, à travers toute la boucle,
  puis n'appeler `add()` qu'une fois à la fin (`numeric_validation_pack`,
  `check_outcomes = {}` avant la boucle, `check_outcomes.setdefault(column, ...)`).
- **Concaténer les chunks avant d'itérer** (`profiling_pack`, qui charge tout le
  parquet en un seul DataFrame avant de calculer les agrégats).

### Le piège du LazyFrame

`pack.scan_data()` — la voie recommandée pour le 100 Go+ (`duplicates_finder_pack`,
`data_compare_pack`, `profiling_pack`, `outlier_detection_pack`,
`referential_integrity_pack` l'utilisent déjà) — renvoie un `pl.LazyFrame`. Le
passer tel quel à `figures.add()` ou `top_n()` lève un `TypeError` explicite
(« frame est un plan différé ») plutôt qu'un plantage confus : `figures.py` ne
matérialise jamais votre plan à votre place. Appelez `.collect()` (ou
`.collect(engine="streaming")`) sur votre **agrégat** — pas sur la source
complète — avant de le passer. La bibliothèque refuse de le faire pour vous :
collecter dans `add()` matérialiserait tout le plan dans le worker et annulerait
l'intérêt du plafond de lignes, qui existe justement pour éviter ça.

### Le piège du uv.lock

Bumper le plancher `qalita-core>=X.Y.Z` dans `pyproject.toml` ne change rien tant
que `uv.lock` n'est pas régénéré : `uv sync` résout depuis le lock, pas depuis le
plancher déclaré. Les deux packs pilotes avaient un lock figé sur une vieille
version sans `FiguresAsset` — le pack installait quand même, et cassait à
l'exécution plutôt qu'à l'installation. Après avoir bumpé la dépendance :
`cd <pack>_pack && uv lock`.

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
- ✅ New files are proprietary QALITA by default; attribute and list in `NOTICE`
  any file deriving from third-party Apache-2.0 material
