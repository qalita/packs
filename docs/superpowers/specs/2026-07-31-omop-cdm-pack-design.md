# Design — `omop_cdm_pack`

**Date** : 2026-07-31
**Statut** : validé, prêt pour plan d'implémentation

Portage du [OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard) (DQD)
en un pack QALITA évaluant la qualité d'une instance OMOP CDM.

## Contexte

DQD est un package R sous Apache 2.0 qui exécute ~2 800 checks de qualité contre une
instance OMOP CDM. Sa valeur ne réside pas dans le code R : elle réside dans deux
artefacts qui sont des **fichiers de données portables**.

1. Les métadonnées CSV (`inst/csv/`) — ~550 lignes × 70 colonnes déclarant, pour chaque
   champ de chaque table CDM, quels checks s'appliquent et avec quel seuil.
2. Les 30 templates SQL paramétrés (`inst/sql/sql_server/`) — chacun retourne exactement
   trois nombres : `num_violated_rows`, `pct_violated_rows`, `num_denominator_rows`.

Le moteur R n'est qu'une boucle : lire le CSV, rendre le template, exécuter, comparer au
seuil. C'est cette boucle que l'on réécrit.

## Décisions de cadrage

| Question | Décision | Motif |
|---|---|---|
| Périmètre | Les 27 check types de DQD (conformance + completeness + plausibility) | Demandé explicitement |
| Moteur d'exécution | **Polars**, en lazy/streaming — pas de pushdown SQL | Demandé ; aligne le pack sur le reste du catalogue QALITA |
| Forme de la source | Schéma CDM entier ; le pack déduit la liste des tables des CSV de métadonnées | Les checks inter-tables (`plausibleAfterBirth`, `withinVisitDates`, `isForeignKey`) sont une part majeure de DQD |
| Vocabulaire OMOP | **Optionnel** ; si `CONCEPT`/`CONCEPT_ANCESTOR` sont absents, les checks concernés sont `Not Applicable` et non `Fail` | Sémantique de DQD lui-même ; évite d'imposer ~10M lignes pour un run trivial |
| Versions CDM | 5.3 et 5.4 | Couvre le parc installé ; 5.2 est marginal |

### Compromis assumé

DQD natif pousse l'agrégation en base et ne déplace donc jamais les données. En Polars il
faut matérialiser les tables CDM en parquet. C'est excellent jusqu'à quelques centaines de
millions de lignes et coûteux en I/O sur un CDM à l'échelle Sentinel. Le lazy/streaming
systématique (§5) repousse cette limite mais ne la supprime pas.

## 1. Ce qu'on reprend, ce qu'on réécrit

| Artefact OHDSI | Sort |
|---|---|
| `inst/csv/*_Table_Level.csv`, `*_Field_Level.csv`, `*_Concept_Level.csv` | **Repris tels quels**, zéro modification |
| `inst/csv/*_Check_Descriptions.csv` | **Repris tel quel** — libellés, catégories Kahn, sévérité |
| `inst/sql/sql_server/*.sql` (30 fichiers) | **Réécrits** en 27 fonctions Polars |
| Code R (`R/`), application Shiny | **Ignorés** — la restitution est le métier de la plateforme QALITA |

Les checks étant paramétrés, 27 fonctions suffisent à produire les ~2 800 checks instanciés.

## 2. Structure du pack

```
omop_cdm_pack/
├── main.py                 # orchestration seule
├── pack_conf.json
├── properties.yaml
├── pyproject.toml
├── run.sh
├── icon.png
├── README.md
├── LICENSE                 # Apache 2.0
├── NOTICE                  # attribution OHDSI (obligation de licence)
└── omop_dqd/
    ├── vendor/             # matériel OHDSI sous Apache 2.0, jamais modifié
    │   ├── csv/            # CSV OHDSI intacts (5.3 et 5.4)
    │   ├── LICENSE-APACHE-2.0.txt
    │   └── README.md       # commit amont vendorisé
    ├── catalog.py          # CSV → List[CheckInstance]
    ├── context.py          # LazyFrame par table, détection du vocabulaire
    ├── checks/
    │   ├── table_level.py
    │   ├── field_level.py
    │   └── concept_level.py
    ├── registry.py         # checkName → fonction
    └── evaluate.py         # seuil → pass/fail/na, agrégation Kahn
```

`main.py` ne contient que l'orchestration. Toute la logique de check est dans `omop_dqd/`,
testable sans plateforme ni source de données.

## 3. Le catalogue

`catalog.py` reproduit l'instanciation de DQD : pour chaque ligne de `Field_Level.csv`,
chaque colonne `<checkName>` valant `Yes` produit un `CheckInstance`.

```python
@dataclass(frozen=True)
class CheckInstance:
    check_name: str          # "plausibleAfterBirth"
    check_level: str         # "TABLE" | "FIELD" | "CONCEPT"
    cdm_table_name: str
    cdm_field_name: str | None
    threshold: float         # % de lignes en violation toléré
    params: dict             # fkTableName, plausibleValueLow, standardConceptFieldName, …
```

550 lignes de CSV → 2 757 instances (CDM 5.4) et 2 163 (CDM 5.3). Portage mécanique, entièrement testable sans données.

## 4. Les 27 checks en Polars

| Famille | Checks | Traduction |
|---|---|---|
| Schéma seul | `cdmTable`, `cdmField`, `cdmDatatype` | Inspection du schéma parquet, aucune lecture de données |
| Colonne seule | `isRequired`, `measureValueCompleteness`, `plausibleValueLow/High`, `sourceValueCompleteness`, `plausibleStartBeforeEnd` | Agrégats `pl.col(...)`, streaming pur |
| Unicité | `isPrimaryKey` | `len() - n_unique()` |
| Anti-jointure | `isForeignKey`, `measurePersonCompleteness` | Pattern déjà éprouvé dans `referential_integrity_pack` |
| Jointure PERSON/DEATH | `plausibleAfterBirth`, `plausibleBeforeDeath`, `plausibleDuringLife` | Join streaming sur `person_id` |
| Jointure inter-tables | `plausibleTemporalAfter`, `withinVisitDates` | Join sur la table/champ nommés dans le CSV |
| Vocabulaire | `fkDomain`, `fkClass`, `isStandardValidConcept`, `standardConceptRecordCompleteness`, `sourceConceptRecordCompleteness`, `plausibleGender`, `plausibleUnitConceptIds` | Join `CONCEPT`/`CONCEPT_ANCESTOR` ; `Not Applicable` si absent |
| Fenêtrage | `measureObservationPeriodOverlap`, `measureConditionEraCompleteness` | `sort` + `shift` par groupe |

La liste exacte des check types sera figée depuis `Check_Descriptions.csv` à
l'implémentation, qui fait foi.

Toutes les fonctions partagent un contrat unique, calqué sur celui du SQL d'origine pour
rendre la comparaison ligne à ligne triviale :

```python
@register("plausibleAfterBirth")
def plausible_after_birth(ctx: CdmContext, chk: CheckInstance) -> CheckResult: ...
```

`CheckResult` porte `num_violated_rows`, `num_denominator_rows` et un statut
`PASS | FAIL | NOT_APPLICABLE | ERROR`. `pct_violated_rows` est dérivé, jamais stocké.

Point délicat unique : `measureObservationPeriodOverlap`, qui exige une comparaison
inter-lignes ordonnée. C'est un idiome Polars standard (`sort` + `shift` sur groupe).

## 5. Exécution

Naïvement, ~2 800 checks signifient ~2 800 scans du parquet. On groupe donc les checks **par
table** et on collecte tous les agrégats d'une même table en une passe, avec
`pl.collect_all()` pour paralléliser entre tables. C'est l'équivalent Polars du batching
`UNION ALL` de DQD, et c'est ce qui rend l'approche viable en volume.

Une erreur sur un check n'interrompt jamais le run : le check est marqué `ERROR` et
l'exécution continue.

## 6. Sortie QALITA

Émettre ~2 800 métriques brutes serait inexploitable. On agrège :

- `score` — dataset ; % de checks passés, pondéré par sévérité (`fatal` > `convention` > `characterization`)
- `conformance_score`, `completeness_score`, `plausibility_score` — dataset ; les trois axes du framework Kahn
- `score` — scope `{perimeter: "table", value: "CONDITION_OCCURRENCE"}`
- `pct_violated_rows` — **uniquement pour les checks en échec**, scope `{perimeter: "column", value: "CONDITION_OCCURRENCE.condition_concept_id"}`
- `recommendations` — une par check `fatal` en échec

Les recommandations sont alimentées par les colonnes `userGuidance` et `etlConventions` des
CSV OHDSI. Ces colonnes contiennent des conseils de remédiation rédigés par la communauté
OMOP, que le dashboard DQD n'affiche pas. C'est le principal apport du pack au-delà du
portage.

`pack_conf.json` expose : version CDM (`5.3`/`5.4`), liste de tables à **exclure** (par
défaut toutes les tables CDM présentes dans la source sont évaluées), surcharge de seuils
par `checkName`, activation du vocabulaire, et la configuration `charts` habituelle.

## 7. Vérification

- **Tests unitaires** — mini-CDM synthétique en parquet (~20 lignes/table) avec violations
  connues ; assertion sur les valeurs exactes de `num_violated_rows` et
  `num_denominator_rows`, au moins un cas par check type.
- **Tests du catalogue** — nombre et forme des checks instanciés depuis les CSV, sans données.
- **Test de dégradation** — sans vocabulaire, les checks concernés sortent `NOT_APPLICABLE`
  et le run aboutit.
- **Validation croisée, une fois, hors CI** — exécuter le vrai DQD sur
  [Eunomia](https://github.com/OHDSI/Eunomia) et comparer les JSON produits. C'est l'étalon
  de fidélité du portage ; exclu de la CI car il exige un runtime R.

## 8. Licence

**DQD est sous Apache 2.0. Les packs QALITA sont sous licence propriétaire**
(« QALITA SOFTWARE LICENSE AGREEMENT », `license = {text = "Proprietary"}`).
L'`AGENTS.md` du dépôt affirme à tort qu'ils sont Apache 2.0.

Ce n'est pas un obstacle : Apache 2.0 est une licence permissive qui autorise
explicitement l'incorporation dans une œuvre propriétaire. Les obligations à respecter
sont en revanche strictes.

| Obligation Apache 2.0 §4 | Mise en œuvre |
|---|---|
| Joindre une copie de la licence | `omop_dqd/vendor/LICENSE-APACHE-2.0.txt` |
| Conserver le `NOTICE` amont | `NOTICE` à la racine du pack, créditant `OHDSI/DataQualityDashboard` |
| Signaler les fichiers modifiés | En-tête de chaque module `checks/*.py` indiquant qu'il dérive d'un template SQL OHDSI nommé |
| Conserver les mentions de copyright | CSV vendorisés jamais modifiés ; `omop_dqd/vendor/README.md` fige la version/commit DQD repris |

Le pack reste donc à double régime : le contenu de `omop_dqd/vendor/` et les modules
`checks/` dérivés restent sous Apache 2.0 ; le reste est propriétaire QALITA. Le `README.md`
et le `NOTICE` doivent l'énoncer.

**Point à faire trancher hors ingénierie** : la réécriture en Polars de la logique des
templates SQL constitue vraisemblablement une œuvre dérivée. Apache 2.0 l'autorise sous
réserve des obligations ci-dessus, mais la qualification exacte et la rédaction du `NOTICE`
méritent une validation juridique avant publication en `visibility: public`.

## 9. Hors périmètre

- Cohortes (`runForCohort`) — la v1 tourne sur l'intégralité du CDM
- CDM antérieur à 5.3
- Mode `sqlOnly` de DQD
- Extraction des primitives Polars vers `qalita_core` : à faire quand un deuxième pack en
  aura besoin, pas avant. Abstraire sur un seul cas d'usage produit la mauvaise abstraction.
