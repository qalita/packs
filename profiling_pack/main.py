from qalita_core.pack import Pack
from qalita_core.utils import (
    denormalize,
    round_if_numeric,
    extract_variable_name,
    determine_level,
)
from qalita_core import sanitize_dataframe_for_parquet
from qalita_core.aggregation import (
    detect_chunked_from_items,
    CompletenessAggregator,
    normalize_and_dedupe_recommendations,
)
import json
import pandas as pd
import numpy as np
import os
from ydata_profiling import ProfileReport
from datetime import datetime
from io import StringIO
import logging

logger = logging.getLogger(__name__)

# Big data configuration for profiling
MAX_ROWS_FOR_FULL_PROFILE = 1_000_000  # Sample if more than 1M rows
SAMPLE_SIZE_FOR_LARGE_DATASETS = 500_000  # Sample size for large datasets
USE_MINIMAL_MODE_THRESHOLD = 5_000_000  # Use minimal mode if more than 5M rows

# Try to import Polars for efficient row counting
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


def _get_row_count_efficient(paths):
    """Get row count efficiently without loading all data."""
    if not paths:
        return 0

    # Try Polars first (fastest)
    if POLARS_AVAILABLE:
        try:
            lf = pl.scan_parquet(paths)
            return lf.select(pl.len()).collect(engine="streaming").item()
        except Exception:
            pass

    # Fallback: read parquet metadata
    try:
        import pyarrow.parquet as pq

        total = 0
        for path in paths:
            if isinstance(path, str) and path.lower().endswith(
                (".parquet", ".pq")
            ):
                pf = pq.ParquetFile(path)
                total += pf.metadata.num_rows
        return total
    except Exception:
        return 0


def _load_parquet_with_sampling(paths, max_rows=None, sample_fraction=None):
    """
    Load parquet files with optional sampling for big data.

    For datasets larger than MAX_ROWS_FOR_FULL_PROFILE, automatically
    samples to SAMPLE_SIZE_FOR_LARGE_DATASETS rows.

    Returns:
        tuple: (DataFrame, is_sampled, original_row_count)
    """
    if not paths:
        return pd.DataFrame(), False, 0

    # Ensure paths is a list
    if isinstance(paths, str):
        paths = [paths]

    # Filter to valid parquet paths
    parquet_paths = [
        p
        for p in paths
        if isinstance(p, str) and p.lower().endswith((".parquet", ".pq"))
    ]
    if not parquet_paths:
        return pd.DataFrame(), False, 0

    # Get total row count efficiently
    total_rows = _get_row_count_efficient(parquet_paths)

    # Determine if sampling is needed
    is_sampled = False
    effective_max_rows = max_rows or MAX_ROWS_FOR_FULL_PROFILE

    if total_rows > effective_max_rows:
        sample_size = min(SAMPLE_SIZE_FOR_LARGE_DATASETS, effective_max_rows)
        logger.info(
            f"Large dataset detected ({total_rows:,} rows). Sampling {sample_size:,} rows for profiling."
        )
        print(
            f"Large dataset detected ({total_rows:,} rows). Sampling {sample_size:,} rows for profiling."
        )
        is_sampled = True

        # Use Polars for efficient sampling if available
        if POLARS_AVAILABLE:
            try:
                lf = pl.scan_parquet(parquet_paths)
                # Use head() for deterministic sampling (faster than random sample)
                df = lf.head(sample_size).collect().to_pandas()
                return df, is_sampled, total_rows
            except Exception as e:
                logger.warning(
                    f"Polars sampling failed: {e}, falling back to pandas"
                )

        # Pandas fallback: load only first chunk
        try:
            df = pd.read_parquet(parquet_paths[0], engine="pyarrow")
            if len(df) > sample_size:
                df = df.head(sample_size)
            return df, is_sampled, total_rows
        except Exception:
            pass

    # Load full dataset (small enough)
    try:
        if POLARS_AVAILABLE:
            lf = pl.scan_parquet(parquet_paths)
            df = lf.collect().to_pandas()
        else:
            if len(parquet_paths) == 1:
                df = pd.read_parquet(parquet_paths[0], engine="pyarrow")
            else:
                dfs = [
                    pd.read_parquet(p, engine="pyarrow") for p in parquet_paths
                ]
                df = pd.concat(dfs, ignore_index=True)
        return df, False, len(df)
    except Exception as e:
        logger.error(f"Failed to load parquet: {e}")
        return pd.DataFrame(), False, 0


# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
with Pack() as pack:
    #########################################################
    ## Figures — agrégats qui expliquent les métriques
    pack.figures.declare_measure(
        "p_cells_missing",
        unit="ratio",
        direction="lower_is_better",
        target=0.05,
        warn=0.10,
        label="Taux de cellules manquantes",
    )
    pack.figures.declare_measure(
        "p_missing",
        unit="ratio",
        direction="lower_is_better",
        target=0.05,
        warn=0.10,
        label="Taux de valeurs manquantes",
    )
    pack.figures.declare_measure(
        "n_columns", unit="count", direction="neutral", label="Colonnes"
    )

    if pack.source_config.get("type") == "database":
        # On récupère la table ou la requête depuis la config ou on la demande explicitement
        table_or_query = pack.source_config.get("config", {}).get(
            "table_or_query"
        )
        if not table_or_query:
            raise ValueError(
                "For a 'database' type source, you must specify 'table_or_query' in the config."
            )
        pack.load_data("source", table_or_query=table_or_query)
    else:
        pack.load_data("source")

    ########################### Profiling and Aggregating Results
    # Determine the appropriate name for 'dataset' in 'scope'
    dataset_scope_name = pack.source_config["name"]

    # Normaliser la source en une liste de tuples (nom_dataset, dataframe)
    raw_df_source = pack.df_source
    configured_table_or_query = pack.source_config.get("config", {}).get(
        "table_or_query"
    )
    data_items = []

    # Track sampling metadata
    sampling_metadata = {}

    # Si l'opener renvoie des chemins parquet, les charger avec sampling pour big data
    def _load_parquet_if_path(obj, dataset_name=None):
        """Load parquet with automatic sampling for large datasets."""
        try:
            if isinstance(obj, str) and obj.lower().endswith(
                (".parquet", ".pq")
            ):
                df, is_sampled, original_rows = _load_parquet_with_sampling(
                    [obj]
                )
                if dataset_name and is_sampled:
                    sampling_metadata[dataset_name] = {
                        "sampled": True,
                        "original_rows": original_rows,
                        "sample_rows": len(df),
                    }
                return df
            elif isinstance(obj, list):
                df, is_sampled, original_rows = _load_parquet_with_sampling(
                    obj
                )
                if dataset_name and is_sampled:
                    sampling_metadata[dataset_name] = {
                        "sampled": True,
                        "original_rows": original_rows,
                        "sample_rows": len(df),
                    }
                return df
        except Exception as e:
            logger.warning(f"Failed to load parquet: {e}")
        return obj

    if isinstance(raw_df_source, list):
        # Check if this is a list of chunk paths that should be treated as one dataset
        total_rows = _get_row_count_efficient(raw_df_source)

        # Load with sampling
        df, is_sampled, original_rows = _load_parquet_with_sampling(
            raw_df_source
        )
        if is_sampled:
            sampling_metadata[dataset_scope_name] = {
                "sampled": True,
                "original_rows": original_rows,
                "sample_rows": len(df),
            }

        # Si la config fournit une liste de noms, l'utiliser si elle correspond en taille
        names = None
        if isinstance(configured_table_or_query, (list, tuple)):
            names = list(configured_table_or_query)
            if len(names) != 1:  # For sampled data, we have single combined df
                names = None
        auto_named = False
        if names is None:
            names = [dataset_scope_name]
            auto_named = True
        data_items = [(names[0], df)]
    else:
        auto_named = False
        common_base_detected = False
        df = _load_parquet_if_path(raw_df_source, dataset_scope_name)
        data_items = [(dataset_scope_name, df)]

    print(f"Generating profile for {len(data_items)} dataset(s)")

    # Conteneurs globaux si besoin d'agréger
    all_alerts_records = []

    # Détection centrale des chunks
    raw_items_list = (
        raw_df_source if isinstance(raw_df_source, list) else [raw_df_source]
    )
    names_for_detect = names if (isinstance(raw_df_source, list)) else None
    treat_chunks_as_one, auto_named, common_base_detected = (
        detect_chunked_from_items(
            raw_items_list, names_for_detect, dataset_scope_name
        )
    )

    # Accumulateur d'agrégation commun
    comp_agg = CompletenessAggregator()

    for dataset_name, df in data_items:
        # Sanitize the DataFrame before profiling/serialization to avoid downstream
        # encoding/type issues with libraries expecting homogeneous column types.
        try:
            df = sanitize_dataframe_for_parquet(df)
        except Exception:
            pass
        # Aperçu
        try:
            print(f"Preview for {dataset_name}:")
            print(df.head())
        except Exception:
            pass

        # Check if this dataset was sampled
        sample_info = sampling_metadata.get(dataset_name, {})
        is_sampled = sample_info.get("sampled", False)
        original_rows = sample_info.get("original_rows", len(df))

        # Build profile title with sampling info
        if is_sampled:
            title = f"Profiling Report for {dataset_name} (Sampled: {len(df):,} of {original_rows:,} rows)"
            print(
                f"Note: Dataset was sampled from {original_rows:,} to {len(df):,} rows for profiling."
            )
        else:
            title = f"Profiling Report for {dataset_name}"

        # Use minimal mode for very large samples to reduce memory usage
        use_minimal = len(df) > USE_MINIMAL_MODE_THRESHOLD

        # Profiling pour ce dataset
        profile = ProfileReport(
            df,
            title=title,
            correlations={"auto": {"calculate": False}},
            minimal=use_minimal,  # Use minimal mode for very large datasets
        )

        if use_minimal:
            print(
                f"Using minimal profiling mode for large dataset ({len(df):,} rows)."
            )

        # Sauvegarde HTML
        html_file_name = f"{dataset_name}_report.html"
        profile.to_file(html_file_name)

        # Pour les sources fichier, on dépose aussi le rapport à côté du fichier source
        if pack.source_config["type"] == "file" and len(data_items) == 1:
            source_file_dir = os.path.dirname(
                pack.source_config["config"]["path"]
            )
            current_date = datetime.now().strftime("%Y%m%d")
            report_file_path = os.path.join(
                source_file_dir,
                f'{current_date}_profiling_report_{pack.source_config["name"]}.html',
            )
            profile.to_file(report_file_path)
            print(f"Profiling report saved to {report_file_path}")

        # Sauvegarde JSON (structure exportée par ydata_profiling)
        json_file_name = f"{dataset_name}_report.json"
        profile.to_file(json_file_name)

        # Extraire tableaux HTML pour les alertes (si présents)
        try:
            with open(html_file_name, "r", encoding="utf-8") as f:
                html_content = f.read()
                tables = pd.read_html(StringIO(html_content))
        except ValueError as e:
            print(
                f"No tables found in the HTML report for {dataset_name}: {e}"
            )
            tables = [pd.DataFrame()]

        ############################ Metrics (par dataset ou agrégé)
        # Accumuler pour agrégation globale si chunking détecté
        if treat_chunks_as_one:
            comp_agg.add_df(df)
        else:
            # Scores de complétude par colonne (mode multi-datasets)
            for col in df.columns:
                non_null_count = max(df[col].notnull().sum(), 0)
                total_count = max(len(df), 1)
                completeness_score = round(non_null_count / total_count, 2)
                pack.metrics.data.append(
                    {
                        "key": "completeness_score",
                        "value": str(completeness_score),
                        "scope": {
                            "perimeter": "column",
                            "value": col,
                            "parent_scope": {
                                "perimeter": "dataset",
                                "value": dataset_name,
                            },
                        },
                    }
                )

        # Charger le JSON du rapport pour extraire les métriques globales et variables
        print(f"Load {dataset_name}_report.json")
        with open(json_file_name, "r", encoding="utf-8") as file:
            report = json.load(file)

        general_data = denormalize(report["table"])
        if not treat_chunks_as_one:
            for key, value in general_data.items():
                if pack.source_config["type"] == "database":
                    entry = {
                        "key": key,
                        "value": round_if_numeric(value),
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                            "parent_scope": {
                                "perimeter": "database",
                                "value": pack.source_config["name"],
                            },
                        },
                    }
                else:
                    entry = {
                        "key": key,
                        "value": round_if_numeric(value),
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                        },
                    }
                pack.metrics.data.append(entry)

        # Variables détaillées: ne pas dupliquer si chunks; on les omet en mode agrégé
        if not treat_chunks_as_one:
            variables_data = report["variables"]
            for variable_name, attributes in variables_data.items():
                for attr_name, attr_value in attributes.items():
                    entry = {
                        "key": attr_name,
                        "value": round_if_numeric(attr_value),
                        "scope": {
                            "perimeter": "column",
                            "value": variable_name,
                            "parent_scope": {
                                "perimeter": "dataset",
                                "value": dataset_name,
                            },
                        },
                    }
                    pack.metrics.data.append(entry)

        # --- Figures du dataset courant
        if not treat_chunks_as_one:
            # Ventilation des manquants par colonne : ce qui explique p_cells_missing.
            missing_rows = [
                {
                    "column": variable_name,
                    "p_missing": float(attributes.get("p_missing", 0) or 0),
                }
                for variable_name, attributes in variables_data.items()
            ]
            missing_rows = [
                row for row in missing_rows if row["p_missing"] > 0
            ]
            if missing_rows:
                pack.figures.add(
                    "missing_by_column",
                    intent="breakdown",
                    of="p_cells_missing",
                    frame=missing_rows,
                    dims=["column"],
                    measures=["p_missing"],
                    scope={"perimeter": "dataset", "value": dataset_name},
                    title="Valeurs manquantes par colonne",
                )

            # Répartition des types de colonnes : une composition, pas une ventilation.
            type_counts = {}
            for attributes in variables_data.values():
                type_name = str(attributes.get("type", "inconnu"))
                type_counts[type_name] = type_counts.get(type_name, 0) + 1
            if type_counts:
                pack.figures.add(
                    "column_types",
                    intent="composition",
                    frame=[
                        {"type": k, "n_columns": v}
                        for k, v in sorted(type_counts.items())
                    ],
                    dims=["type"],
                    measures=["n_columns"],
                    scope={"perimeter": "dataset", "value": dataset_name},
                    title="Répartition des types de colonnes",
                )

        ############################  Advanced Statistics (percentiles, stddev, variance)
        # Compute advanced statistics for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                col_scope = {
                    "perimeter": "column",
                    "value": col,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_name,
                    },
                }
                # Percentiles (: percentile_10_in_range, percentile_25_in_range, etc.)
                pack.metrics.data.extend(
                    [
                        {
                            "key": "percentile_10",
                            "value": str(
                                round(float(np.percentile(col_data, 10)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "percentile_25",
                            "value": str(
                                round(float(np.percentile(col_data, 25)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "percentile_75",
                            "value": str(
                                round(float(np.percentile(col_data, 75)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "percentile_90",
                            "value": str(
                                round(float(np.percentile(col_data, 90)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                    ]
                )
                # Standard deviation (: sample_stddev_in_range, population_stddev_in_range)
                pack.metrics.data.extend(
                    [
                        {
                            "key": "sample_stddev",
                            "value": str(
                                round(float(col_data.std(ddof=1)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "population_stddev",
                            "value": str(
                                round(float(col_data.std(ddof=0)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                    ]
                )
                # Variance (: sample_variance_in_range, population_variance_in_range)
                pack.metrics.data.extend(
                    [
                        {
                            "key": "sample_variance",
                            "value": str(
                                round(float(col_data.var(ddof=1)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                        {
                            "key": "population_variance",
                            "value": str(
                                round(float(col_data.var(ddof=0)), 4)
                            ),
                            "scope": col_scope.copy(),
                        },
                    ]
                )

        # Score basé sur p_cells_missing (directement depuis general_data)
        if not treat_chunks_as_one:
            try:
                p_cells_missing = float(
                    general_data.get("p_cells_missing", 0) or 0
                )
            except Exception:
                p_cells_missing = 0.0
            score_value = max(min(1 - p_cells_missing, 1), 0)

            if pack.source_config["type"] == "database":
                pack.metrics.data.append(
                    {
                        "key": "score",
                        "value": str(round(score_value, 2)),
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                            "parent_scope": {
                                "perimeter": "database",
                                "value": pack.source_config["name"],
                            },
                        },
                    }
                )
            else:
                pack.metrics.data.append(
                    {
                        "key": "score",
                        "value": str(round(score_value, 2)),
                        "scope": {
                            "perimeter": "dataset",
                            "value": pack.source_config["name"],
                        },
                    }
                )

        ############################ Recommendations (par dataset)
        if len(tables) > 2:
            alerts_data = tables[2]
            alerts_data.columns = ["content", "type"]
            alerts_data["scope"] = alerts_data["content"].apply(
                lambda x: {
                    "perimeter": "column",
                    "value": extract_variable_name(x),
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_name,
                    },
                }
            )
            alerts_data["level"] = alerts_data["content"].apply(
                determine_level
            )
        else:
            print(
                f"No alerts table found in the HTML report for {dataset_name}."
            )
            alerts_data = pd.DataFrame()

        all_alerts_records.extend(alerts_data.to_dict(orient="records"))

        ############################ Schemas (par dataset)
        if treat_chunks_as_one:
            # Stocker seulement la liste unique des colonnes; on créera le schéma agrégé après la boucle
            for variable_name in report["variables"].keys():
                comp_agg.unique_columns.add(variable_name)
        else:
            for variable_name in report["variables"].keys():
                entry = {
                    "key": "column",
                    "value": variable_name,
                    "scope": {
                        "perimeter": "column",
                        "value": variable_name,
                        "parent_scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                        },
                    },
                }
                pack.schemas.data.append(entry)

            if pack.source_config["type"] == "database":
                pack.schemas.data.append(
                    {
                        "key": "dataset",
                        "value": dataset_name,
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                            "parent_scope": {
                                "perimeter": "database",
                                "value": pack.source_config["name"],
                            },
                        },
                    }
                )
            else:
                pack.schemas.data.append(
                    {
                        "key": "dataset",
                        "value": dataset_name,
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                        },
                    }
                )

    ############################ Consolidate recommendations across datasets
    if treat_chunks_as_one:
        pack.recommendations.data = normalize_and_dedupe_recommendations(
            all_alerts_records, dataset_scope_name
        )
    else:
        pack.recommendations.data = all_alerts_records

    ############################ Writing Results to Files

    # En mode chunk agrégé, construire les métriques et schémas consolidés dans un périmètre unique
    if treat_chunks_as_one:
        # Finaliser via l'agrégateur commun
        pack.metrics.data, pack.schemas.data = (
            comp_agg.finalize_metrics_and_schemas(dataset_scope_name)
        )

    ################## Remove unwanted metrics or recommendations

    unwanted_keys = [
        "histogram",
        "value_counts_index_sorted",
        "value_counts_without_nan",
    ]
    pack.metrics.data = [
        item
        for item in pack.metrics.data
        if item.get("key") not in unwanted_keys
    ]

    unwanted_keys = ["Unsupported"]
    pack.recommendations.data = [
        item
        for item in pack.recommendations.data
        if item.get("type") not in unwanted_keys
    ]

    pack.metrics.save()
    pack.recommendations.save()
    pack.schemas.save()
    pack.figures.save()
