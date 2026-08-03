# omop_cdm_pack — Plan Revision 1

**Date:** 2026-07-31
**Amends:** `2026-07-31-omop-cdm-pack.md`, Tasks 2 and 5
**Trigger:** Task 5's review fetched the real OHDSI SQL and found the original plan
diverges from upstream in six places.

The original plan was written from OHDSI's prose documentation. This revision is written
from the vendored CSVs and the upstream SQL templates, both of which are now in the repo
or fetchable. Where they disagree with the prose, they win.

## What was wrong

### A. The plan ignored the `evaluationFilter` column

`Check_Descriptions.csv` carries an `evaluationFilter` column: upstream's own rule for
whether a check instantiates for a given metadata row. The original plan never read it.

Audit of all 27 check types: 21 have a filter equivalent to what the plan's trigger modes
already do. Six do not, and four of those are real gaps:

| Check | `evaluationFilter` | Plan did | Consequence |
|---|---|---|---|
| `cdmDatatype` | `cdmDatatype=='integer'` | instantiated for every declared datatype | runs on ~432 fields upstream never checks |
| `fkDomain` | `isForeignKey=='Yes' & fkDomain!=''` | ignored the `isForeignKey` clause | over-instantiates |
| `fkClass` | `isForeignKey=='Yes' & fkClass!=''` | ignored the `isForeignKey` clause | over-instantiates |
| `plausibleUnitConceptIds` | `plausibleUnitConceptIdsThreshold!=''` | triggered on the value column | wrong gate |
| `cdmTable` | `cdmTableName!=''` | — | already equivalent |
| `cdmField` | `cdmFieldName!=''` | — | already equivalent |

### B. `isPrimaryKey` used the wrong formula

`field_is_primary_key.sql` selects rows whose value appears in
`... GROUP BY field HAVING COUNT_BIG(*) > 1`, then counts them. **Every row of a duplicate
group is a violation**, including the first. The plan used `total - n_unique`, which counts
`k-1` per group of size `k`. For the fixture's single duplicated id it reports 1 where
upstream reports 2, and the gap widens with group size.

### C. `sourceValueCompleteness` implemented a different check

The plan counted null-or-blank rows. Upstream measures **distinct source values that are
unmapped**:

- **Numerator** — `SELECT DISTINCT field WHERE <standardConceptFieldName> = 0`, counted.
  Distinct source values, not rows. A NULL source value forms one group and does count.
- **Denominator** — `COUNT(DISTINCT field) + COUNT(DISTINCT CASE WHEN field IS NULL THEN 1 END)`:
  distinct non-null source values, plus 1 if any NULL exists.

It needs the companion column `standardConceptFieldName`, which the plan never passed.
All 32 rows that trigger this check do carry it, so the faithful version is buildable.

### D. `cdmDatatype` is a per-row content check, not a schema check

`field_cdm_datatype.sql` counts non-null rows whose value is non-numeric, or numeric with a
decimal point. Denominator is `COUNT(*)` of the whole table, nulls included. The plan
compared the parquet dtype against a set and used a denominator of 1.

---

## Task 2 revision — gate instantiation on the real filters

### `CheckSpec` gains three fields

```python
@dataclass(frozen=True)
class CheckSpec:
    """How one check column of a metadata CSV instantiates checks.

    trigger_column  column that gates instantiation; defaults to `name`
    value_column    column supplying params["value"]; defaults to `name`
    trigger_equals  for TRIGGER_VALUE, the cell must equal this
                    (case-insensitive) rather than merely be non-empty
    requires        prerequisite (column, value) pairs, all of which must
                    hold for the check to instantiate
    """

    name: str
    trigger: str
    param_columns: Tuple[str, ...] = ()
    trigger_column: Optional[str] = None
    value_column: Optional[str] = None
    trigger_equals: Optional[str] = None
    requires: Tuple[Tuple[str, str], ...] = ()
```

These exist to express `evaluationFilter` declaratively. Do **not** build a general
expression interpreter: there are four rules, they are static, and hardcoding them stays
auditable against the CSV column.

### `_instantiate` honours them

Resolution order, per spec, per row:

1. `requires` — every `(column, value)` must match the row, case-insensitively. Fail → skip.
2. Gate cell = `row[spec.trigger_column or spec.name]`.
3. `TRIGGER_ALWAYS` → instantiate. `TRIGGER_YES` → gate cell must equal `"Yes"`.
   `TRIGGER_VALUE` → gate cell must be non-empty, and if `trigger_equals` is set it must
   equal it case-insensitively.
4. `params["value"]` comes from `row[spec.value_column or spec.name]`, not from the gate
   cell — they differ for `plausibleUnitConceptIds`.
5. Threshold column stays `f"{spec.name}Threshold"`.

### Four spec changes

```python
CheckSpec("cdmDatatype", TRIGGER_VALUE, trigger_equals="integer"),

CheckSpec(
    "fkDomain", TRIGGER_VALUE, requires=(("isForeignKey", "Yes"),)
),
CheckSpec(
    "fkClass", TRIGGER_VALUE, requires=(("isForeignKey", "Yes"),)
),

CheckSpec(
    "sourceValueCompleteness",
    TRIGGER_YES,
    param_columns=("standardConceptFieldName",),
),

# concept level
CheckSpec(
    "plausibleUnitConceptIds",
    TRIGGER_VALUE,
    trigger_column="plausibleUnitConceptIdsThreshold",
    value_column="plausibleUnitConceptIds",
    param_columns=("conceptId",),
),
```

`sourceValueCompleteness` keeps its `TRIGGER_YES` gate; only the new param column is added,
because Task 5 needs it.

### Tests to add to `tests/test_catalog.py`

```python
def test_cdm_datatype_is_instantiated_only_for_integer_fields():
    catalog = load_catalog("5.4")
    datatypes = {
        c.params["value"].lower()
        for c in catalog
        if c.check_name == "cdmDatatype"
    }
    assert datatypes == {"integer"}


def test_fk_domain_requires_the_field_to_be_a_foreign_key():
    catalog = load_catalog("5.4")
    fk_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isForeignKey"
    }
    domain_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "fkDomain"
    }
    assert domain_fields
    assert domain_fields <= fk_fields


def test_fk_class_requires_the_field_to_be_a_foreign_key():
    catalog = load_catalog("5.4")
    fk_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "isForeignKey"
    }
    class_fields = {
        (c.cdm_table_name, c.cdm_field_name)
        for c in catalog
        if c.check_name == "fkClass"
    }
    assert class_fields
    assert class_fields <= fk_fields


def test_plausible_unit_concept_ids_is_gated_on_its_threshold():
    catalog = load_catalog("5.4")
    units = [
        c for c in catalog if c.check_name == "plausibleUnitConceptIds"
    ]
    assert units
    # the gate is the threshold column, but the payload is the id list
    assert all(c.params["value"] for c in units)
    assert all(c.params["conceptId"] for c in units)
    assert all(c.threshold > 0 for c in units)


def test_source_value_completeness_carries_its_companion_field():
    catalog = load_catalog("5.4")
    checks = [
        c
        for c in catalog
        if c.check_name == "sourceValueCompleteness"
    ]
    assert checks
    assert all(
        c.params.get("standardConceptFieldName") for c in checks
    )
```

Update `test_catalog_instantiates_thousands_of_checks` to assert the **exact** post-change
counts for both CDM versions rather than a loose lower bound — the reviewer flagged the
loose bound as near-vacuous, and the exact number is knowable. Measure, then pin, and put
the measured numbers in the fix report.

---

## Task 5 revision — three checks

### `isPrimaryKey` — count every row of a duplicate group

```python
@register("isPrimaryKey")
def is_primary_key(ctx, chk) -> CheckResult:
    """Rows whose key value is not unique.

    Upstream counts EVERY row of a duplicated group, not the excess
    beyond the first: a value appearing twice contributes 2, not 1.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    duplicated = (
        frame.group_by(field)
        .agg(pl.len().alias("_n"))
        .filter(pl.col("_n") > 1)
        .select(field)
    )
    violated = frame.join(duplicated, on=field, how="semi")
    return counted(_row_count(violated), total)
```

Test expectation changes: the fixture's `condition_occurrence_id` 104 appears twice, so
`test_is_primary_key_detects_the_duplicate` now expects **2** violations out of 6, not 1.
Add a comment in the test saying why 2 and not 1 — it is the single most counter-intuitive
number in the suite.

### `cdmDatatype` — per-row integer content check

```python
@register("cdmDatatype")
def cdm_datatype(ctx, chk) -> CheckResult:
    """Non-null values that are not whole numbers.

    Upstream only ever runs this for fields declared `integer` (see the
    evaluationFilter in Check_Descriptions.csv), and counts non-null
    rows that are non-numeric or numeric-with-a-decimal-point, over a
    denominator of every row in the table.

    In parquet the declared type is enforced by the file, so an integer
    dtype can hold no violation; float and string columns can.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)
    total = _row_count(frame)
    dtype = ctx.dtypes(chk.cdm_table_name)[field]
    column = pl.col(field)

    if dtype in _INTEGER_DTYPES:
        return counted(0, total)
    if dtype in _FLOAT_DTYPES:
        violated = column.is_not_null() & (column != column.floor())
    elif dtype == pl.Utf8:
        parsed = column.str.strip_chars().cast(pl.Int64, strict=False)
        violated = column.is_not_null() & parsed.is_null()
    else:
        return not_applicable(
            f"dtype {dtype} cannot be checked as an integer"
        )
    return counted(
        _row_count(frame.filter(violated)), total
    )
```

Delete `_DATATYPE_GROUPS` and the `varchar` entry added during Task 5 — with the catalog
gating on `integer`, the group table has no remaining purpose. Keep two module-level
frozensets `_INTEGER_DTYPES` and `_FLOAT_DTYPES` listing the concrete Polars dtypes, since
`pl.INTEGER_DTYPES` is deprecated.

Test changes: `test_cdm_datatype_passes_for_a_matching_integer` keeps passing but its
denominator becomes the PERSON row count (4), not 1. Replace
`test_cdm_datatype_flags_a_mismatch` — declaring `person_id` as `varchar(50)` is no longer
meaningful, since the catalog would never instantiate that. Instead write a test over a
purpose-built one-column parquet holding strings, where some parse as integers and some do
not, and assert the exact violated/denominator pair. Build it in the test with `tmp_path`
and `CdmContext.from_paths`, as Task 8 does for observation periods — do not touch the
shared fixture.

### `sourceValueCompleteness` — distinct unmapped source values

```python
@register("sourceValueCompleteness")
def source_value_completeness(ctx, chk) -> CheckResult:
    """Distinct source values that are unmapped.

    Upstream counts DISTINCT source values whose companion standard
    concept field is 0, over a denominator of distinct non-null source
    values plus one bucket for NULL if any row has one. Both numbers
    are counts of values, not of rows.
    """
    skip = guard(ctx, chk)
    if skip:
        return skip
    standard_field = chk.params.get(
        "standardConceptFieldName", ""
    ).lower()
    if not standard_field or not ctx.has_column(
        chk.cdm_table_name, standard_field
    ):
        return not_applicable(
            f"companion field {standard_field!r} unavailable"
        )
    field = chk.cdm_field_name
    frame = ctx.table(chk.cdm_table_name)

    violated = (
        frame.filter(pl.col(standard_field) == 0)
        .select(field)
        .unique()
    )
    distinct_non_null = _row_count(
        frame.select(field).drop_nulls().unique()
    )
    has_null = (
        _row_count(frame.filter(pl.col(field).is_null())) > 0
    )
    denominator = distinct_non_null + (1 if has_null else 0)
    return counted(_row_count(violated), denominator)
```

`.unique()` groups nulls together, matching `SELECT DISTINCT`, so a NULL source value on an
unmapped row contributes 1 to the numerator — as upstream does.

Test changes: against the shared fixture, `CONDITION_OCCURRENCE.condition_source_value` with
companion `condition_concept_id` has no row where the concept is 0, so the check yields
**0 violations over a denominator of 6** — five distinct non-null values `{A,B,C,D,F}` plus
one NULL bucket. Assert that pair; it pins the denominator formula, which is the part most
likely to be got wrong. Then add a second test building a small purpose-made parquet with
`tmp_path` that does contain rows with a 0 concept, including a repeat of one source value,
and assert that the numerator counts distinct values rather than rows.

---

## Knock-on effects

- **Task 7** (`fkDomain`, `fkClass`): no check-code change. Their instance counts drop
  because of the `isForeignKey` prerequisite. Any count assertion must be re-measured.
- **Task 9** (`plausibleUnitConceptIds`): no check-code change; the gate moves to the
  threshold column. `params["value"]` still holds the id list.
- **Tasks 10 and 12**: total catalog size changes. Assert measured values, not the old ones.
- **`omop_cdm_pack/README.md`** still claims "roughly 4 000 checks" and must be corrected to
  the measured post-revision numbers in Task 12.

## Not in scope

The remaining ~20 check types were not audited against their upstream SQL line by line.
This revision fixes what Task 5's review surfaced. The Eunomia cross-validation deferred in
the original plan remains the systematic fidelity gate, and should now be treated as
required rather than optional before the pack goes public.
