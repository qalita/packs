#  Data Quality Checks - Référence pour Packs Qalita

Ce document liste les ~200 checks de qualité de données de , organisés par catégorie et dimension.
Utilisez cette référence pour créer de nouveaux packs Qalita.

---

## CHECKS AU NIVEAU COLONNE

### 1. ACCEPTED VALUES (Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `text_found_in_set_percent` | Pourcentage de valeurs dans un ensemble attendu | `expected_values`, `min_percent` |
| `number_found_in_set_percent` | Pourcentage de valeurs numériques dans un ensemble | `expected_values`, `min_percent` |
| `expected_text_values_in_use_count` | Compte des valeurs texte attendues trouvées | `expected_values`, `max_missing` |
| `expected_texts_in_top_values_count` | Vérifie si les valeurs attendues sont dans le top N | `expected_values`, `top`, `max_missing` |
| `expected_numbers_in_use_count` | Compte des valeurs numériques attendues trouvées | `expected_values`, `max_missing` |
| `text_valid_country_code_percent` | Pourcentage de codes pays ISO valides (2 lettres) | `min_percent` |
| `text_valid_currency_code_percent` | Pourcentage de codes devise valides | `min_percent` |

---

### 2. ACCURACY (Accuracy)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `total_sum_match_percent` | Différence de somme avec colonne de référence | `max_percent` |
| `total_min_match_percent` | Différence de valeur minimum | `max_percent` |
| `total_max_match_percent` | Différence de valeur maximum | `max_percent` |
| `total_average_match_percent` | Différence de moyenne | `max_percent` |
| `total_not_null_count_match_percent` | Différence de comptage non-null | `max_percent` |

---

### 3. ANOMALY DETECTION (Anomaly Detection)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `sum_anomaly` | Détecte les anomalies dans les sommes (fenêtre 90 jours) | `anomaly_percent` |
| `mean_anomaly` | Détecte les anomalies dans les moyennes | `anomaly_percent` |
| `median_anomaly` | Détecte les anomalies dans les médianes | `anomaly_percent` |
| `min_anomaly` | Détecte les anomalies dans les valeurs min | `anomaly_percent` |
| `max_anomaly` | Détecte les anomalies dans les valeurs max | `anomaly_percent` |
| `mean_change` | Changement de moyenne depuis dernière mesure | `max_percent` |
| `mean_change_1_day` | Changement de moyenne vs J-1 | `max_percent` |
| `mean_change_7_days` | Changement de moyenne vs J-7 | `max_percent` |
| `mean_change_30_days` | Changement de moyenne vs J-30 | `max_percent` |
| `median_change` | Changement de médiane depuis dernière mesure | `max_percent` |
| `median_change_1_day` | Changement de médiane vs J-1 | `max_percent` |
| `median_change_7_days` | Changement de médiane vs J-7 | `max_percent` |
| `median_change_30_days` | Changement de médiane vs J-30 | `max_percent` |
| `sum_change` | Changement de somme depuis dernière mesure | `max_percent` |
| `sum_change_1_day` | Changement de somme vs J-1 | `max_percent` |
| `sum_change_7_days` | Changement de somme vs J-7 | `max_percent` |
| `sum_change_30_days` | Changement de somme vs J-30 | `max_percent` |

---

### 4. BOOLEAN (Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `true_percent` | Pourcentage de valeurs true | `min_percent`, `max_percent` |
| `false_percent` | Pourcentage de valeurs false | `min_percent`, `max_percent` |

---

### 5. COMPARISONS (Accuracy/Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `sum_match` | Compare la somme avec table de référence | `max_diff` |
| `min_match` | Compare le minimum avec table de référence | `max_diff` |
| `max_match` | Compare le maximum avec table de référence | `max_diff` |
| `mean_match` | Compare la moyenne avec table de référence | `max_diff` |
| `not_null_count_match` | Compare le comptage non-null avec référence | `max_diff` |
| `null_count_match` | Compare le comptage null avec référence | `max_diff` |

---

### 6. CONVERSIONS (Validity)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `text_parsable_to_boolean_percent` | Pourcentage convertible en boolean | `min_percent` |
| `text_parsable_to_integer_percent` | Pourcentage convertible en integer | `min_percent` |
| `text_parsable_to_float_percent` | Pourcentage convertible en float | `min_percent` |
| `text_parsable_to_date_percent` | Pourcentage convertible en date | `min_percent` |

---

### 7. CUSTOM SQL (Custom Validation)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `sql_condition_failed_on_column` | Assertion SQL custom sur colonne | `sql_condition` |
| `sql_condition_passed_percent_on_column` | Pourcentage passant condition SQL | `sql_condition`, `min_percent` |
| `sql_aggregate_expression_on_column` | Expression SQL agrégée custom | `sql_expression`, `min_value`, `max_value` |
| `sql_invalid_value_count_on_column` | Comptage valeurs invalides via SQL | `sql_query`, `max_count` |
| `import_custom_result_on_column` | Import résultats custom depuis table de log | `sql_query` |

---

### 8. DATATYPE (Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `detected_datatype_in_text` | Détecte le type de données dans colonne texte | `expected_datatype` (code 1-8) |
| `detected_datatype_in_text_changed` | Détecte changement de type de données | - |

---

### 9. DATETIME (Validity/Timeliness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `date_values_in_future_percent` | Pourcentage de dates dans le futur | `max_percent` |
| `date_in_range_percent` | Pourcentage de dates dans une plage valide | `min_date`, `max_date`, `min_percent` |
| `text_match_date_format_percent` | Pourcentage correspondant au format de date | `date_format`, `min_percent` |

---

### 10. INTEGRITY (Referential Integrity)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `lookup_key_not_found` | Comptage de valeurs non trouvées dans table dictionnaire | `max_count` |
| `lookup_key_found_percent` | Pourcentage de clés valides trouvées | `min_percent` |

---

### 11. NULLS (Completeness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `nulls_count` | Comptage de valeurs null | `max_count` |
| `nulls_percent` | Pourcentage de valeurs null | `max_percent` |
| `nulls_percent_anomaly` | Détection d'anomalie sur pourcentage null | `anomaly_percent` |
| `not_nulls_count` | Comptage de valeurs non-null | `min_count` |
| `not_nulls_percent` | Pourcentage de valeurs non-null | `max_percent` |
| `empty_column_found` | Détecte colonnes vides (tous null) | `min_count` |
| `nulls_percent_change` | Changement de % null depuis dernière mesure | `max_percent` |
| `nulls_percent_change_1_day` | Changement vs J-1 | `max_percent` |
| `nulls_percent_change_7_days` | Changement vs J-7 | `max_percent` |
| `nulls_percent_change_30_days` | Changement vs J-30 | `max_percent` |

---

### 12. NUMERIC (Validity/Accuracy)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `number_below_min_value` | Comptage valeurs sous minimum | `min_value`, `max_count` |
| `number_above_max_value` | Comptage valeurs au-dessus maximum | `max_value`, `max_count` |
| `negative_values` | Comptage de valeurs négatives | `max_count` |
| `negative_values_percent` | Pourcentage de valeurs négatives | `max_percent` |
| `number_below_min_value_percent` | Pourcentage sous minimum | `min_value`, `max_percent` |
| `number_above_max_value_percent` | Pourcentage au-dessus maximum | `max_value`, `max_percent` |
| `number_in_range_percent` | Pourcentage dans plage valide | `min_value`, `max_value`, `min_percent` |
| `integer_in_range_percent` | Pourcentage d'entiers dans plage | `min_value`, `max_value`, `min_percent` |
| `min_in_range` | Valeur minimum dans plage | `min_value`, `max_value` |
| `max_in_range` | Valeur maximum dans plage | `min_value`, `max_value` |
| `sum_in_range` | Somme dans plage | `min_value`, `max_value` |
| `mean_in_range` | Moyenne dans plage | `min_value`, `max_value` |
| `median_in_range` | Médiane dans plage | `min_value`, `max_value` |
| `percentile_in_range` | Percentile custom dans plage | `percentile`, `min_value`, `max_value` |
| `percentile_10_in_range` | 10ème percentile dans plage | `min_value`, `max_value` |
| `percentile_25_in_range` | 25ème percentile dans plage | `min_value`, `max_value` |
| `percentile_75_in_range` | 75ème percentile dans plage | `min_value`, `max_value` |
| `percentile_90_in_range` | 90ème percentile dans plage | `min_value`, `max_value` |
| `sample_stddev_in_range` | Écart-type échantillon dans plage | `min_value`, `max_value` |
| `population_stddev_in_range` | Écart-type population dans plage | `min_value`, `max_value` |
| `sample_variance_in_range` | Variance échantillon dans plage | `min_value`, `max_value` |
| `population_variance_in_range` | Variance population dans plage | `min_value`, `max_value` |
| `invalid_latitude` | Comptage de latitudes invalides | `max_count` |
| `valid_latitude_percent` | Pourcentage de latitudes valides (-90 à 90) | `min_percent` |
| `invalid_longitude` | Comptage de longitudes invalides | `max_count` |
| `valid_longitude_percent` | Pourcentage de longitudes valides (-180 à 180) | `min_percent` |
| `non_negative_values` | Comptage de valeurs non-négatives | `max_count` |
| `non_negative_values_percent` | Pourcentage de valeurs non-négatives | `max_percent` |

---

### 13. PATTERNS (Validity/Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `text_not_matching_regex_found` | Comptage ne correspondant pas au regex | `regex`, `max_count` |
| `texts_not_matching_regex_percent` | Pourcentage ne correspondant pas au regex | `regex`, `max_percent` |
| `invalid_email_format_found` | Comptage d'emails invalides | `max_count` |
| `invalid_email_format_percent` | Pourcentage d'emails invalides | `max_percent` |
| `text_not_matching_date_pattern_found` | Comptage ne correspondant pas au pattern date | `date_pattern`, `max_count` |
| `text_not_matching_date_pattern_percent` | Pourcentage ne correspondant pas au pattern date | `date_pattern`, `max_percent` |
| `text_not_matching_name_pattern_percent` | Pourcentage ne correspondant pas au pattern nom | `max_percent` |
| `invalid_uuid_format_found` | Comptage d'UUIDs invalides | `max_count` |
| `invalid_uuid_format_percent` | Pourcentage d'UUIDs invalides | `max_percent` |
| `invalid_ip4_address_format_found` | Comptage d'adresses IPv4 invalides | `max_count` |
| `invalid_ip6_address_format_found` | Comptage d'adresses IPv6 invalides | `max_count` |
| `invalid_usa_phone_format_found` | Comptage de téléphones USA invalides | `max_count` |
| `invalid_usa_zipcode_format_found` | Comptage de codes postaux USA invalides | `max_count` |
| `invalid_usa_phone_format_percent` | Pourcentage de téléphones USA invalides | `max_percent` |
| `invalid_usa_zipcode_format_percent` | Pourcentage de codes postaux USA invalides | `max_percent` |

---

### 14. PII (Privacy/Security)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `contains_usa_phone_percent` | Pourcentage contenant numéros téléphone USA | `max_percent` |
| `contains_email_percent` | Pourcentage contenant emails | `max_percent` |
| `contains_usa_zipcode_percent` | Pourcentage contenant codes postaux USA | `max_percent` |
| `contains_ip4_percent` | Pourcentage contenant adresses IPv4 | `max_percent` |
| `contains_ip6_percent` | Pourcentage contenant adresses IPv6 | `max_percent` |

---

### 15. SCHEMA (Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `column_exists` | Vérifie que la colonne existe | `expected_value` (1.0) |
| `column_type_changed` | Détecte changement de type de colonne | - |

---

### 16. TEXT (Validity)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `text_min_length` | Longueur minimum du texte dans plage | `min_value`, `max_value` |
| `text_max_length` | Longueur maximum du texte dans plage | `min_value`, `max_value` |
| `text_mean_length` | Longueur moyenne du texte dans plage | `min_value`, `max_value` |
| `text_length_below_min_length` | Comptage plus court que minimum | `min_length`, `max_count` |
| `text_length_below_min_length_percent` | Pourcentage plus court que minimum | `min_length`, `max_percent` |
| `text_length_above_max_length` | Comptage plus long que maximum | `max_length`, `max_count` |
| `text_length_above_max_length_percent` | Pourcentage plus long que maximum | `max_length`, `max_percent` |
| `text_length_in_range_percent` | Pourcentage dans plage de longueur | `min_length`, `max_length`, `min_percent` |
| `min_word_count` | Comptage minimum de mots dans plage | `min_value`, `max_value` |
| `max_word_count` | Comptage maximum de mots dans plage | `min_value`, `max_value` |

---

### 17. UNIQUENESS (Uniqueness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `distinct_count` | Comptage de valeurs distinctes dans plage | `min_value`, `max_value` |
| `distinct_percent` | Pourcentage de valeurs distinctes | `min_percent`, `max_percent` |
| `duplicate_count` | Comptage de valeurs dupliquées | `min_count` |
| `duplicate_percent` | Pourcentage de valeurs dupliquées | `max_percent` |
| `distinct_count_anomaly` | Détection d'anomalie sur comptage distinct | `anomaly_percent` |
| `distinct_percent_anomaly` | Détection d'anomalie sur % distinct | `anomaly_percent` |
| `distinct_count_change` | Changement de comptage distinct | `max_percent` |
| `distinct_count_change_1_day` | Changement vs J-1 | `max_percent` |
| `distinct_count_change_7_days` | Changement vs J-7 | `max_percent` |
| `distinct_count_change_30_days` | Changement vs J-30 | `max_percent` |
| `distinct_percent_change` | Changement de % distinct | `max_percent` |
| `distinct_percent_change_1_day` | Changement vs J-1 | `max_percent` |
| `distinct_percent_change_7_days` | Changement vs J-7 | `max_percent` |
| `distinct_percent_change_30_days` | Changement vs J-30 | `max_percent` |

---

### 18. WHITESPACE (Completeness/Validity)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `empty_text_found` | Comptage de textes vides | `max_count` |
| `whitespace_text_found` | Comptage de textes whitespace uniquement | `max_count` |
| `null_placeholder_text_found` | Comptage de placeholders null (N/A, None, etc.) | `max_count` |
| `empty_text_percent` | Pourcentage de textes vides | `max_percent` |
| `whitespace_text_percent` | Pourcentage de textes whitespace | `max_percent` |
| `null_placeholder_text_percent` | Pourcentage de placeholders null | `max_percent` |
| `text_surrounded_by_whitespace_found` | Comptage de textes avec whitespace autour | `max_count` |
| `text_surrounded_by_whitespace_percent` | Pourcentage avec whitespace autour | `max_percent` |

---

## CHECKS AU NIVEAU TABLE

### 1. ACCURACY (Accuracy)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `total_row_count_match_percent` | Différence de comptage lignes vs table référence | `max_percent` |

---

### 2. AVAILABILITY (Availability)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `table_availability` | Vérifie que la table est accessible et interrogeable | - (retourne 0.0 si OK) |

---

### 3. COMPARISONS (Accuracy/Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `row_count_match` | Compare le comptage lignes avec table référence | `max_diff` |
| `column_count_match` | Compare le comptage colonnes avec table référence | `max_diff` |

---

### 4. CUSTOM SQL (Custom Validation)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `sql_condition_failed_on_table` | Assertion SQL custom sur table | `sql_condition` |
| `sql_condition_passed_percent_on_table` | Pourcentage passant condition SQL | `sql_condition`, `min_percent` |
| `sql_aggregate_expression_on_table` | Expression SQL agrégée custom | `sql_expression`, `min_value`, `max_value` |
| `sql_invalid_record_count_on_table` | Comptage enregistrements invalides via SQL | `sql_query`, `max_count` |
| `import_custom_result_on_table` | Import résultats custom depuis table de log | `sql_query` |

---

### 5. SCHEMA (Consistency)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `column_count` | Vérifie que le comptage colonnes correspond | `expected_value` |
| `column_count_changed` | Détecte changement de comptage colonnes | - |
| `column_list_changed` | Détecte changement de liste colonnes (ordre ignoré) | - |
| `column_list_or_order_changed` | Détecte changement de liste ou ordre colonnes | - |
| `column_types_changed` | Détecte changement de types de colonnes | - |

---

### 6. TIMELINESS (Timeliness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `data_freshness` | Jours depuis la ligne la plus récente (timestamp événement) | `max_days` |
| `data_freshness_anomaly` | Détection d'anomalie sur fraîcheur données | `anomaly_percent` |
| `data_staleness` | Jours depuis dernière ingestion de données | `max_days` |
| `data_ingestion_delay` | Délai entre timestamp événement et ingestion | `max_days` |
| `reload_lag` | Délai max entre ingestion et événement (partitionné) | `max_days` |

---

### 7. UNIQUENESS (Uniqueness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `duplicate_record_count` | Comptage d'enregistrements dupliqués | `min_count` |
| `duplicate_record_percent` | Pourcentage d'enregistrements dupliqués | `max_percent` |

---

### 8. VOLUME (Completeness)

| Check | Description | Paramètres |
|-------|-------------|------------|
| `row_count` | Vérifie comptage minimum de lignes | `min_count` |
| `row_count_anomaly` | Détection d'anomalie sur comptage lignes | `anomaly_percent` |
| `row_count_change` | Changement de comptage lignes depuis dernière mesure | `max_percent` |
| `row_count_change_1_day` | Changement vs J-1 | `max_percent` |
| `row_count_change_7_days` | Changement vs J-7 | `max_percent` |
| `row_count_change_30_days` | Changement vs J-30 | `max_percent` |

---

## MAPPING DIMENSIONS QUALITÉ

| Dimension | Checks associés |
|-----------|-----------------|
| **Completeness** | nulls, whitespace, volume, empty_column_found |
| **Accuracy** | accuracy, comparisons, numeric ranges |
| **Consistency** | accepted_values, patterns, schema, datatype, boolean |
| **Validity** | patterns, numeric, text, datetime, conversions |
| **Uniqueness** | uniqueness checks (distinct, duplicate) |
| **Timeliness** | datetime, timeliness checks |
| **Referential Integrity** | integrity checks (lookup_key) |
| **Privacy/Security** | pii checks |
| **Availability** | availability checks |
| **Anomaly Detection** | anomaly checks (*_anomaly, *_change) |

---

## IDÉES DE PACKS QALITA À CRÉER

### Packs existants à enrichir :
1. **profiling_pack** → ajouter statistiques percentiles, variance, stddev
2. **pii_scanner_pack** → ajouter détection IPv6, patterns internationaux
3. **duplicates_finder_pack** → ajouter anomaly detection sur distinct_count
4. **timeliness_pack** → ajouter data_staleness, ingestion_delay

### Nouveaux packs potentiels :
1. **anomaly_detection_pack** → tous les checks *_anomaly et *_change
2. **pattern_validation_pack** → regex, email, UUID, IP, phone, zipcode
3. **numeric_validation_pack** → ranges, percentiles, latitude/longitude
4. **text_validation_pack** → longueur, word count, conversions
5. **availability_pack** → table_availability, schema drift detection
6. **cross_table_comparison_pack** → comparaisons entre tables/sources
7. **custom_sql_pack** → exécution de règles SQL custom
8. **accepted_values_pack** → validation d'ensembles de valeurs autorisées

---

## NOTES D'IMPLÉMENTATION

### Niveaux de sévérité
- **Warning** : Moins critique, n'affecte pas le KPI
- **Error** : Niveau par défaut, réduit le score KPI
- **Fatal** : Critique, devrait stopper les pipelines

### Types d'exécution
- **Profiling** : Analyse ponctuelle
- **Monitoring (Daily/Monthly)** : Monitoring régulier avec stockage time-series
- **Partitioned (Daily/Monthly)** : Analyse par partition

### Paramètres communs
- `min_percent` / `max_percent` : Seuils en pourcentage
- `min_count` / `max_count` : Seuils en comptage absolu
- `min_value` / `max_value` : Plages de valeurs
- `anomaly_percent` : Seuil de détection d'anomalie
- `max_diff` : Différence maximale autorisée
