# AGENTS.md — Qalita Public Packs

Ce fichier fournit des instructions aux agents IA pour travailler sur ce dépôt.

## Projet

**Qalita Packs** — Collection open source de packs d'analyse de qualité de données pour la plateforme Qalita.

- **Organisation GitHub** : `qalita-io`
- **Licence** : Apache 2.0
- **Visibilité** : Public

## Architecture

```
packs/
├── profiling_pack/              # Profilage des données
├── duplicates_finder_pack/      # Détection de doublons
├── outlier_detection_pack/      # Détection d'outliers
├── numeric_validation_pack/     # Validation numérique
├── text_validation_pack/        # Validation de texte
├── pattern_validation_pack/     # Validation de patterns
├── schema_scanner_pack/         # Scan de schéma
├── pii_scanner_pack/            # Détection de données personnelles
├── referential_integrity_pack/  # Intégrité référentielle
├── accepted_values_pack/        # Valeurs acceptées
├── accuracy_pack/               # Exactitude des données
├── data_compare_pack/           # Comparaison de datasets
├── data_drift_pack/             # Détection de drift
├── timeliness_pack/             # Fraîcheur des données
├── fhir_compliance_pack/        # Conformité FHIR
├── great_expectations_pack/     # Intégration Great Expectations
├── soda_pack/                   # Intégration Soda
├── dbt_checks_pack/             # Intégration dbt
├── scripts/                     # Scripts utilitaires
└── tests/                       # Tests
```

## Conventions de code

- Chaque pack est un dossier autonome à la racine
- Les packs utilisent `qalita_core` comme dépendance pour l'accès aux données
- **Licence** : Inclure l'en-tête Apache 2.0 dans tout nouveau fichier
- **Scripts** : `bump_pack_versions.sh` et `push_all_packs.sh` pour la gestion des versions

## Git workflow

- **Tags** : Semver strict `X.Y.Z` (⚠️ PAS de préfixe `v`)
- **Commits** : Messages en anglais, conventionnels (`feat:`, `fix:`, `chore:`)

## Règles

- ❌ Ne pas modifier la structure d'un pack existant sans comprendre l'impact sur les utilisateurs
- ✅ Tout nouveau pack doit suivre la structure des packs existants
- ✅ Documenter les inputs/outputs de chaque pack
