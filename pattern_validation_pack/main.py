"""
Pattern Validation Pack

Validates data formats using predefined and custom regex patterns.
Covers checks:
- invalid_email_format_found, invalid_email_format_percent
- invalid_uuid_format_found, invalid_uuid_format_percent
- invalid_ip4_address_format_found, invalid_ip6_address_format_found
- text_not_matching_regex_found, texts_not_matching_regex_percent
- text_not_matching_date_pattern_found

Matching runs inside the engine as a vectorized expression, and every rule of
every column is folded into ONE streaming aggregation per dataset. The previous
implementation compiled a Python regex and called it once per row through
``Series.apply``, materializing three full-length Series per column.
"""

import re

import polars as pl

from qalita_core import analytics
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

# Predefined patterns for common data formats
BUILTIN_PATTERNS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "ipv4": r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    "ipv6": r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}$",
    "url": r"^https?://[^\s/$.?#].[^\s]*$",
    "phone_international": r"^\+?[1-9]\d{1,14}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "date_us": r"^\d{2}/\d{2}/\d{4}$",
    "date_eu": r"^\d{2}-\d{2}-\d{4}$",
    "datetime_iso": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
    "credit_card": r"^(?:\d[ -]*?){13,16}$",
    "hex_color": r"^#(?:[0-9a-fA-F]{3}){1,2}$",
    "mac_address": r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",
    "postal_code_us": r"^\d{5}(?:-\d{4})?$",
    "alphanumeric": r"^[A-Za-z0-9]+$",
}

# Expressions per streaming pass, so a table with hundreds of pattern rules
# cannot build one unbounded projection.
MAX_EXPRS_PER_PASS = 512

# Rows drawn when a pattern cannot run inside the engine (see METHOD_SAMPLED).
DEFAULT_SAMPLE_ROWS = 100_000

# How a count was obtained, reported next to every metric as "<key>_method".
METHOD_EXACT = "exact"
METHOD_SAMPLED = "sampled_python_regex"

# Bounded row-level evidence.
DEFAULT_EXAMPLE_ROWS = 10
MAX_EXAMPLE_ROWS = 1000
DEFAULT_MAX_EXAMPLE_CHECKS = 20

# Dtypes whose string rendering is undefined; a pattern check on them is
# meaningless and ``cast(Utf8)`` raises rather than producing something useful.
NESTED_TYPES = (pl.List, pl.Array, pl.Struct, pl.Object)


def anchored(pattern: str) -> str:
    """Rust-regex equivalent of Python's ``re.match``: anchored at the start.

    The non-capturing group is load-bearing. ``re.match('a|b', s)`` anchors the
    alternation as a whole, and so does ``^(?:a|b)`` — while ``^a|b`` anchors
    only the first branch and would accept "xb".
    """
    return f"^(?:{pattern})"


def polars_supports(pattern: str) -> bool:
    """Whether the Rust regex engine accepts this pattern.

    Polars has neither backreferences nor look-around, and a pattern using them
    raises at collect time. Probing on a one-row frame costs nothing and lets
    the pack degrade to a bounded sampled check instead of killing the job.
    """
    try:
        pl.DataFrame({"__probe": [""]}).select(
            pl.col("__probe").str.contains(anchored(pattern))
        )
    except Exception:
        return False
    return True


def as_text(column: str) -> "pl.Expr":
    """The column rendered as text, the way the check reads it."""
    return pl.col(column).cast(pl.Utf8)


def invalid_expr(column: str, pattern: str) -> "pl.Expr":
    """True for a row whose value does not match ``pattern``.

    Nulls and empty strings pass, which is what the pandas implementation did
    (it dropped nulls first, then treated "" as valid).
    """
    text = as_text(column)
    return (
        pl.col(column).is_not_null()
        & (text != "")
        & ~text.str.contains(anchored(pattern))
    )


def agg_batched(lf: "pl.LazyFrame", exprs: dict) -> dict:
    """``analytics.agg`` with a bound on the size of a single projection."""
    out: dict = {}
    for batch in analytics.batched(exprs.items(), MAX_EXPRS_PER_PASS):
        out.update(analytics.agg(lf, dict(batch)))
    return out


def resolve_pattern(rule: dict) -> tuple:
    """``(pattern_name, pattern)`` a rule asks for, or ``(None, None)``."""
    pattern_type = rule.get("type")
    custom_regex = rule.get("regex")
    if pattern_type == "regex" and custom_regex:
        return "custom_regex", custom_regex
    if pattern_type in BUILTIN_PATTERNS:
        return pattern_type, BUILTIN_PATTERNS[pattern_type]
    return None, None


def checkable(column: str, schema: dict) -> bool:
    if column not in schema:
        return False
    return not isinstance(schema[column], NESTED_TYPES)


def plan_checks(rules: list, schema: dict) -> list:
    """Configured rules that can run against this schema."""
    checks = []
    for rule in rules:
        column = rule.get("column")
        if column not in schema:
            print(f"Column '{column}' not found in dataset. Skipping.")
            continue
        pattern_name, pattern = resolve_pattern(rule)
        if pattern is None:
            print(
                f"Unknown pattern type '{rule.get('type')}' for column '{column}'. Skipping."
            )
            continue
        if not checkable(column, schema):
            print(
                f"Column '{column}' has a nested type and cannot be matched. Skipping."
            )
            continue
        checks.append(
            {
                "column": column,
                "pattern_name": pattern_name,
                "pattern": pattern,
            }
        )
    return checks


def plan_autodetected(schema: dict) -> list:
    """Checks inferred from column names when no rule is configured."""
    checks = []
    for column in schema:
        if not checkable(column, schema):
            continue
        lowered = column.lower()
        if "email" in lowered or "mail" in lowered:
            checks.append(
                {
                    "column": column,
                    "pattern_name": "email",
                    "pattern": BUILTIN_PATTERNS["email"],
                }
            )
        if "uuid" in lowered or "guid" in lowered:
            checks.append(
                {
                    "column": column,
                    "pattern_name": "uuid",
                    "pattern": BUILTIN_PATTERNS["uuid"],
                }
            )
        if (
            "ip" in lowered
            and "address" in lowered
            or lowered in ["ip", "ip_address", "ipaddress"]
        ):
            checks.append(
                {
                    "column": column,
                    "pattern_name": "ipv4",
                    "pattern": BUILTIN_PATTERNS["ipv4"],
                }
            )
    return checks


def measure(lf: "pl.LazyFrame", checks: list) -> tuple:
    """Invalid counts for every check, in ONE streaming pass.

    Checks whose pattern the engine refuses are counted afterwards, on a
    bounded sample, and reported with ``METHOD_SAMPLED``.
    """
    exprs = {"__rows": pl.len()}
    for i, check in enumerate(checks):
        exprs[f"{i}|n"] = pl.col(check["column"]).count()
        if check["supported"]:
            exprs[f"{i}|bad"] = invalid_expr(
                check["column"], check["pattern"]
            ).sum()

    raw = agg_batched(lf, exprs)
    rows = int(raw.get("__rows") or 0)

    results = []
    for i, check in enumerate(checks):
        total = int(raw.get(f"{i}|n") or 0)
        results.append(
            {
                **check,
                "total": total,
                "invalid_count": (
                    int(raw.get(f"{i}|bad") or 0)
                    if check["supported"]
                    else None
                ),
                "method": (
                    METHOD_EXACT if check["supported"] else METHOD_SAMPLED
                ),
            }
        )
    return results, rows


def sampled_invalid(
    lf: "pl.LazyFrame",
    check: dict,
    total: int,
    rows: int,
    sample_rows: int,
    keep: list,
) -> tuple:
    """Estimate the invalid count of an engine-incompatible pattern.

    Polars cannot run backreferences or look-around, so the pattern is applied
    by Python's ``re`` to a bounded uniform sample: the answer is an estimate,
    it is labelled as one, and memory stays proportional to the sample rather
    than to the dataset.

    Returns
        ``(estimated_invalid_count, invalid_fraction, sampled_row_positions,
        sample)``.
    """
    compiled = re.compile(check["pattern"])
    column = check["column"]
    frame = analytics.sample(
        lf.select(keep), n=sample_rows, total_rows=rows or None
    )
    if frame.height == 0:
        return 0, 0.0, [], frame

    values = frame[column].cast(pl.Utf8).to_list()
    considered = 0
    invalid_positions = []
    for position, value in enumerate(values):
        if value is None:
            continue
        considered += 1
        if value != "" and not compiled.match(value):
            invalid_positions.append(position)

    if considered == 0:
        return 0, 0.0, [], frame
    fraction = len(invalid_positions) / considered
    return round(fraction * total), fraction, invalid_positions, frame


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

    def columns(self, column: str, schema: dict) -> list:
        keep = [
            name
            for name in self.id_columns
            if name in schema and name != column
        ]
        keep.append(column)
        return keep

    def rows(self, lf, predicate, column: str, schema: dict) -> list:
        if self.limit <= 0 or self.budget <= 0:
            return []
        self.budget -= 1
        _, examples = analytics.failures(
            lf,
            predicate,
            limit=self.limit,
            columns=self.columns(column, schema),
        )
        return examples.to_dicts()


def column_scope(column: str, dataset_label: str) -> dict:
    return {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
    }


def emit(pack: Pack, key: str, value, scope: dict, method: str) -> None:
    """Append a metric together with the method that produced it.

    The sibling ``<key>_method`` is what lets the UI say whether a number is
    exact or estimated instead of showing both the same way.
    """
    pack.metrics.data.append(
        {"key": key, "value": value, "scope": scope.copy()}
    )
    pack.metrics.data.append(
        {"key": f"{key}_method", "value": method, "scope": scope.copy()}
    )


def emit_check(pack: Pack, check: dict, dataset_label: str) -> None:
    """Metrics for one finished check, under the historical key names."""
    scope = column_scope(check["column"], dataset_label)
    pattern_name = check["pattern_name"]
    method = check["method"]
    invalid_count = check["invalid_count"]
    invalid_percent = check["invalid_percent"]
    valid_percent = check["valid_percent"]

    if pattern_name == "email":
        emit(pack, "invalid_email_format_found", invalid_count, scope, method)
        emit(
            pack,
            "invalid_email_format_percent",
            str(invalid_percent),
            scope,
            method,
        )
    elif pattern_name == "uuid":
        emit(pack, "invalid_uuid_format_found", invalid_count, scope, method)
        emit(
            pack,
            "invalid_uuid_format_percent",
            str(invalid_percent),
            scope,
            method,
        )
    elif pattern_name == "ipv4":
        emit(
            pack,
            "invalid_ip4_address_format_found",
            invalid_count,
            scope,
            method,
        )
    elif pattern_name == "ipv6":
        emit(
            pack,
            "invalid_ip6_address_format_found",
            invalid_count,
            scope,
            method,
        )
    else:
        # Generic pattern validation (text_not_matching_regex)
        emit(
            pack, "text_not_matching_regex_found", invalid_count, scope, method
        )
        emit(
            pack,
            "texts_not_matching_regex_percent",
            str(invalid_percent),
            scope,
            method,
        )

    emit(
        pack,
        f"valid_{pattern_name}_percent",
        str(valid_percent),
        scope,
        method,
    )


def run(pack: Pack) -> None:
    """Validate every logical object of the source."""
    job = pack.pack_config.get("job", {}) or {}
    validation_rules = job.get("patterns", [])
    sample_rows = int(job.get("sample_rows", DEFAULT_SAMPLE_ROWS))
    examples = Examples(pack.pack_config)

    total_checks = 0
    total_valid_percent = 0
    any_sampled = False

    for dataset_label in pack.tables("source"):
        print(f"Validating patterns for {dataset_label}")
        lf = pack.scan("source", table=dataset_label)
        schema = pack.schema("source", table=dataset_label)

        if validation_rules:
            checks = plan_checks(validation_rules, schema)
        else:
            print("No explicit patterns configured. Running auto-detection...")
            checks = plan_autodetected(schema)

        # Probe once per distinct pattern: the answer only depends on the
        # pattern, never on the data.
        supported = {
            check["pattern"]: polars_supports(check["pattern"])
            for check in checks
        }
        for check in checks:
            check["supported"] = supported[check["pattern"]]

        results, rows = measure(lf, checks)

        for check in results:
            column = check["column"]
            if check["total"] == 0:
                print(f"Column '{column}' is empty. Skipping.")
                continue

            sample_frame = None
            invalid_positions = []
            if check["method"] == METHOD_SAMPLED:
                any_sampled = True
                keep = examples.columns(column, schema)
                (
                    invalid_count,
                    fraction,
                    invalid_positions,
                    sample_frame,
                ) = sampled_invalid(
                    lf, check, check["total"], rows, sample_rows, keep
                )
                print(
                    f"  [{column}] {check['pattern_name']}: pattern rejected by the "
                    f"engine (backreference or look-around), estimated on a "
                    f"{sample_frame.height}-row sample"
                )
            else:
                invalid_count = check["invalid_count"]
                fraction = invalid_count / check["total"]
            invalid_percent = round(fraction, 4)
            valid_percent = round(1 - fraction, 4)

            check["invalid_count"] = invalid_count
            check["invalid_percent"] = invalid_percent
            check["valid_percent"] = valid_percent
            emit_check(pack, check, dataset_label)

            total_checks += 1
            total_valid_percent += valid_percent

            if invalid_count > 0:
                scope = column_scope(column, dataset_label)
                if check["method"] == METHOD_SAMPLED:
                    # Evidence comes from the sample already in memory: the
                    # engine cannot filter on a pattern it cannot compile.
                    rows_out = sample_frame[
                        invalid_positions[: examples.limit]
                    ].to_dicts()
                else:
                    rows_out = examples.rows(
                        lf,
                        invalid_expr(column, check["pattern"]),
                        column,
                        schema,
                    )
                if rows_out:
                    pack.metrics.data.append(
                        {
                            "key": "invalid_format_examples",
                            "value": rows_out,
                            "scope": scope.copy(),
                        }
                    )
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {invalid_count} values ({invalid_percent*100:.2f}%) that don't match the {check['pattern_name']} pattern.",
                        "type": f"Invalid {check['pattern_name'].replace('_', ' ').title()} Format",
                        "scope": scope.copy(),
                        "level": determine_recommendation_level(
                            invalid_percent
                        ),
                    }
                )

            print(
                f"  [{column}] {check['pattern_name']}: {invalid_count} invalid ({invalid_percent*100:.2f}%), {valid_percent*100:.2f}% valid"
            )

    score = total_valid_percent / total_checks if total_checks > 0 else 1.0
    dataset_scope = {
        "perimeter": "dataset",
        "value": pack.source_config.get("name"),
    }
    emit(
        pack,
        "score",
        str(round(score, 2)),
        dataset_scope,
        METHOD_SAMPLED if any_sampled else METHOD_EXACT,
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
