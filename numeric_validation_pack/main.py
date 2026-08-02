"""
Numeric Validation Pack

Validates numeric data against configurable ranges and constraints.
Covers checks:
- number_below_min_value, number_above_max_value
- number_in_range_percent, integer_in_range_percent
- negative_values, negative_values_percent
- min_in_range, max_in_range, sum_in_range, mean_in_range
- valid_latitude_percent, valid_longitude_percent

Every rule of every column is folded into ONE streaming aggregation per
dataset, so the source is read once no matter how many rules are configured.
Dtypes come from the parquet footers rather than from a materialized frame.
"""

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

# Expressions per streaming pass. Wide tables with a rule on every column would
# otherwise build one unbounded projection.
MAX_EXPRS_PER_PASS = 512

# Bounded row-level evidence. Each example set costs one extra filtered pass,
# so the number of checks that get rows is capped as well as the row count.
DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000
DEFAULT_MAX_EXAMPLE_CHECKS = 20

# Ranges implied by a rule type.
TYPE_RANGES = {
    "latitude": (-90, 90),
    "longitude": (-180, 180),
    "percentage": (0, 100),
    "non_negative": (0, None),
}


def defined(column: str) -> pl.Expr:
    """The column with NaN folded into null.

    pandas' ``dropna()`` dropped NaN as well as null, and every aggregation
    below skips nulls, so this is what keeps the migrated numbers comparable.
    """
    col = pl.col(column)
    return pl.when(col.is_not_nan()).then(col)


def agg_batched(lf: "pl.LazyFrame", exprs: dict) -> dict:
    """``analytics.agg`` with a bound on the size of a single projection."""
    out: dict = {}
    for batch in analytics.batched(exprs.items(), MAX_EXPRS_PER_PASS):
        out.update(analytics.agg(lf, dict(batch)))
    return out


def resolve_range(rule: dict) -> tuple:
    """``(min_value, max_value)`` a rule enforces, rule type included."""
    rule_type = rule.get("type")
    if rule_type in TYPE_RANGES:
        low, high = TYPE_RANGES[rule_type]
        # A typed rule fixes the bounds; only "non_negative" leaves the upper
        # one to the rule itself.
        return low, rule.get("max_value") if high is None else high
    return rule.get("min_value"), rule.get("max_value")


def plan_checks(rules: list, schema: dict) -> list:
    """Rules that can actually run against this schema, with resolved bounds."""
    numeric = set(analytics.numeric_columns(schema))
    checks = []
    for rule in rules:
        column = rule.get("column")
        if column not in schema:
            print(f"Column '{column}' not found in dataset. Skipping.")
            continue
        if column not in numeric:
            print(f"Column '{column}' is not numeric. Skipping.")
            continue
        low, high = resolve_range(rule)
        checks.append(
            {
                "column": column,
                "type": rule.get("type"),
                "min_value": low,
                "max_value": high,
            }
        )
    return checks


def measure(lf: "pl.LazyFrame", checks: list, negatives: list) -> tuple:
    """Range statistics for every check and negative count for every column.

    ONE streaming pass: a per-column loop would re-read the whole source once
    per rule, which is precisely what this pack used to do through pandas.
    """
    exprs = {}
    for i, check in enumerate(checks):
        value = defined(check["column"])
        exprs[f"{i}|n"] = value.count()
        exprs[f"{i}|min"] = value.min()
        exprs[f"{i}|max"] = value.max()
        exprs[f"{i}|sum"] = value.sum()
        exprs[f"{i}|mean"] = value.mean()
        if check["min_value"] is not None:
            exprs[f"{i}|below"] = (value < check["min_value"]).sum()
        if check["max_value"] is not None:
            exprs[f"{i}|above"] = (value > check["max_value"]).sum()
    for j, column in enumerate(negatives):
        value = defined(column)
        exprs[f"neg|{j}"] = (value < 0).sum()
        exprs[f"negn|{j}"] = value.count()

    raw = agg_batched(lf, exprs) if exprs else {}

    results = []
    for i, check in enumerate(checks):
        total = int(raw.get(f"{i}|n") or 0)
        below = int(raw.get(f"{i}|below") or 0)
        above = int(raw.get(f"{i}|above") or 0)
        in_range_count = total - below - above
        results.append(
            {
                **check,
                "total": total,
                "below_min": below,
                "above_max": above,
                "in_range_count": in_range_count,
                "in_range_percent": (
                    round(in_range_count / total, 4) if total else 1.0
                ),
                "min_value_observed": _as_float(raw.get(f"{i}|min")),
                "max_value_observed": _as_float(raw.get(f"{i}|max")),
                "sum_value": _as_float(raw.get(f"{i}|sum")),
                "mean_value": _as_float(raw.get(f"{i}|mean")),
            }
        )

    negative_results = []
    for j, column in enumerate(negatives):
        total = int(raw.get(f"negn|{j}") or 0)
        count = int(raw.get(f"neg|{j}") or 0)
        negative_results.append(
            {
                "column": column,
                "total": total,
                "negative_count": count,
                "negative_percent": (count / total) if total else 0,
            }
        )
    return results, negative_results


def _as_float(value):
    return None if value is None else float(value)


def out_of_range_predicate(check: dict) -> "pl.Expr":
    """True for a row that violates the check, for bounded evidence."""
    value = defined(check["column"])
    parts = []
    if check["min_value"] is not None:
        parts.append(value < check["min_value"])
    if check["max_value"] is not None:
        parts.append(value > check["max_value"])
    if not parts:
        return pl.lit(False)
    predicate = parts[0]
    for extra in parts[1:]:
        predicate = predicate | extra
    return predicate


class Examples:
    """Bounded row-level evidence for failing checks.

    The pack reported counts with nothing to look at. Both the row count and
    the number of checks that get rows are capped: each example set is one
    extra filtered pass over the source.
    """

    def __init__(self, pack_config: dict):
        job = pack_config.get("job", {}) or {}
        enabled = job.get("examples", True)
        rows = int(job.get("example_rows", DEFAULT_EXAMPLE_ROWS))
        self.limit = min(max(rows, 0), MAX_EXAMPLE_ROWS) if enabled else 0
        self.budget = int(
            job.get("example_max_checks", DEFAULT_MAX_EXAMPLE_CHECKS)
        )
        self.id_columns = list(job.get("id_columns") or [])

    def rows(self, lf, predicate, column: str, schema: dict) -> list:
        if self.limit <= 0 or self.budget <= 0:
            return []
        self.budget -= 1
        keep = [
            name
            for name in self.id_columns
            if name in schema and name != column
        ]
        keep.append(column)
        _, examples = analytics.failures(
            lf, predicate, limit=self.limit, columns=keep
        )
        return examples.to_dicts()


def column_scope(column: str, dataset_label: str) -> dict:
    return {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
    }


def declare_measures(pack: Pack) -> None:
    pack.figures.declare_measure(
        "score",
        unit="score",
        direction="higher_is_better",
        target=0.95,
        warn=0.8,
        label="Score de validité",
    )
    pack.figures.declare_measure(
        "n_checks", unit="count", direction="neutral", label="Contrôles"
    )
    pack.figures.declare_measure(
        "n_violations",
        unit="count",
        direction="lower_is_better",
        target=0,
        label="Valeurs hors bornes",
    )


def run(pack: Pack) -> None:
    """Validate every logical object of the source."""
    validation_rules = pack.pack_config.get("job", {}).get("rules", [])
    check_negative = pack.pack_config.get("job", {}).get(
        "check_negative_values", False
    )
    examples = Examples(pack.pack_config)

    total_checks = 0
    total_valid_percent = 0

    # Résultats par colonne, accumulés pour les figures. Une même colonne peut
    # être validée plusieurs fois (plusieurs règles la ciblant dans pack_conf,
    # ou plusieurs objets dans la source) : on ne compte qu'un contrôle par
    # colonne et on additionne ses violations, pour ne produire qu'une ligne par
    # colonne — le dédoublonneur de dimensions du moteur exige un agrégat, pas
    # les exécutions brutes.
    check_outcomes = {}

    declare_measures(pack)

    for dataset_label in pack.tables("source"):
        print(f"Validating numeric ranges for {dataset_label}")
        lf = pack.scan("source", table=dataset_label)
        schema = pack.schema("source", table=dataset_label)

        checks = plan_checks(validation_rules, schema)
        negatives = analytics.numeric_columns(schema) if check_negative else []
        results, negative_results = measure(lf, checks, negatives)

        for check in results:
            column = check["column"]
            if check["total"] == 0:
                print(f"Column '{column}' is empty. Skipping.")
                continue

            col_scope = column_scope(column, dataset_label)

            outcome_entry = check_outcomes.setdefault(
                column, {"column": column, "n_violations": 0}
            )
            outcome_entry["n_violations"] += (
                check["below_min"] + check["above_max"]
            )

            # Add metrics ( naming convention)
            if check["below_min"] > 0:
                pack.metrics.data.append(
                    {
                        "key": "number_below_min_value",
                        "value": check["below_min"],
                        "scope": col_scope.copy(),
                    }
                )
            if check["above_max"] > 0:
                pack.metrics.data.append(
                    {
                        "key": "number_above_max_value",
                        "value": check["above_max"],
                        "scope": col_scope.copy(),
                    }
                )

            pack.metrics.data.extend(
                [
                    {
                        "key": "number_in_range_percent",
                        "value": str(check["in_range_percent"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "min_value",
                        "value": _rounded(check["min_value_observed"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "max_value",
                        "value": _rounded(check["max_value_observed"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "sum_value",
                        "value": _rounded(check["sum_value"]),
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "mean_value",
                        "value": _rounded(check["mean_value"]),
                        "scope": col_scope.copy(),
                    },
                ]
            )

            # Special metrics for latitude/longitude
            if check["type"] == "latitude":
                pack.metrics.data.append(
                    {
                        "key": "invalid_latitude",
                        "value": check["below_min"] + check["above_max"],
                        "scope": col_scope.copy(),
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "valid_latitude_percent",
                        "value": str(check["in_range_percent"]),
                        "scope": col_scope.copy(),
                    }
                )
            elif check["type"] == "longitude":
                pack.metrics.data.append(
                    {
                        "key": "invalid_longitude",
                        "value": check["below_min"] + check["above_max"],
                        "scope": col_scope.copy(),
                    }
                )
                pack.metrics.data.append(
                    {
                        "key": "valid_longitude_percent",
                        "value": str(check["in_range_percent"]),
                        "scope": col_scope.copy(),
                    }
                )

            # Track for overall score
            total_checks += 1
            total_valid_percent += check["in_range_percent"]

            out_of_range = check["below_min"] + check["above_max"]
            if out_of_range > 0:
                out_of_range_percent = 1 - check["in_range_percent"]
                range_desc = _range_description(check)

                rows = examples.rows(
                    lf, out_of_range_predicate(check), column, schema
                )
                if rows:
                    pack.metrics.data.append(
                        {
                            "key": "out_of_range_examples",
                            "value": rows,
                            "scope": col_scope.copy(),
                        }
                    )

                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {out_of_range} values ({out_of_range_percent*100:.2f}%) outside the expected range {range_desc}.",
                        "type": "Numeric Range Violation",
                        "scope": col_scope.copy(),
                        "level": determine_recommendation_level(
                            out_of_range_percent
                        ),
                    }
                )

            print(
                f"  [{column}] in_range: {check['in_range_percent']*100:.2f}%, below_min: {check['below_min']}, above_max: {check['above_max']}"
            )

        for negative in negative_results:
            if negative["total"] == 0:
                continue
            column = negative["column"]
            col_scope = column_scope(column, dataset_label)
            count = negative["negative_count"]
            percent = negative["negative_percent"]

            pack.metrics.data.extend(
                [
                    {
                        "key": "negative_values",
                        "value": count,
                        "scope": col_scope.copy(),
                    },
                    {
                        "key": "negative_values_percent",
                        "value": str(round(percent, 4)),
                        "scope": col_scope.copy(),
                    },
                ]
            )

            if count > 0:
                rows = examples.rows(lf, defined(column) < 0, column, schema)
                if rows:
                    pack.metrics.data.append(
                        {
                            "key": "negative_values_examples",
                            "value": rows,
                            "scope": col_scope.copy(),
                        }
                    )
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {count} negative values ({percent*100:.2f}%).",
                        "type": "Negative Values Found",
                        "scope": col_scope.copy(),
                        "level": "info",
                    }
                )

    add_figures(pack, check_outcomes)

    score = total_valid_percent / total_checks if total_checks > 0 else 1.0
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(score, 2)),
            "scope": {
                "perimeter": "dataset",
                "value": pack.source_config.get("name"),
            },
        }
    )


def _rounded(value):
    return "null" if value is None else str(round(value, 4))


def _range_description(check: dict) -> str:
    low, high = check["min_value"], check["max_value"]
    if low is not None and high is not None:
        return f"[{low}, {high}]"
    if low is not None:
        return f">= {low}"
    if high is not None:
        return f"<= {high}"
    return ""


def add_figures(pack: Pack, check_outcomes: dict) -> None:
    if not check_outcomes:
        return

    dataset_scope = {
        "perimeter": "dataset",
        "value": pack.source_config.get("name"),
    }

    # Verdicts : une composition dont les modalités sont pass/fail. Le moteur
    # leur donne les couleurs de statut, toujours avec icône et libellé. Un
    # contrôle par colonne (voir la collecte plus haut), donc pas de double
    # comptage même si plusieurs règles ciblent la même colonne.
    outcome = {"pass": 0, "fail": 0}
    for entry in check_outcomes.values():
        outcome["fail" if entry["n_violations"] > 0 else "pass"] += 1
    pack.figures.add(
        "checks_outcome",
        intent="composition",
        frame=[{"status": k, "n_checks": v} for k, v in outcome.items()],
        dims=["status"],
        measures=["n_checks"],
        scope=dataset_scope,
        title="Résultat des contrôles",
    )

    # Ventilation des valeurs hors bornes : ce qui explique un score dégradé.
    violations = [e for e in check_outcomes.values() if e["n_violations"] > 0]
    if violations:
        pack.figures.add(
            "violations_by_column",
            intent="breakdown",
            of="score",
            frame=violations,
            dims=["column"],
            measures=["n_violations"],
            scope=dataset_scope,
            title="Valeurs hors bornes par colonne",
        )


if __name__ == "__main__":
    with Pack() as pack:
        if pack.source_config.get("type") == "database":
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

        run(pack)

        pack.metrics.save()
        pack.recommendations.save()
        pack.figures.save()
