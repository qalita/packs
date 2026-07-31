# omop_cdm_pack — Plan Revision 2

**Date:** 2026-07-31
**Amends:** Task 4 (`evaluate.py`) and Task 10 (`runner.py`)
**Trigger:** Task 10's review found that `evaluate.py`'s zero-denominator rule diverges
from upstream far more broadly than the implementer's own flagged edge case suggested.

## What is wrong

`evaluate.py` marks any check `NOT_APPLICABLE` when `num_denominator_rows == 0`.

Upstream does not work that way. Reading `R/evaluateThresholds.R` and
`R/calculateNotApplicableStatus.R` from OHDSI/DataQualityDashboard:

1. `.evaluateThresholds` decides `failed` **purely** from the threshold and
   `numViolatedRows`. A zero denominator with zero violations yields `failed = 0`, i.e. a
   pass. Nothing there consults the denominator for applicability.
2. Only afterwards, and only if the batch contains all three of `cdmTable`, `cdmField` and
   `measureValueCompleteness` (`.containsNAchecks`), does `.calculateNotApplicableStatus`
   reclassify results using **named rules that need cross-check context** — the results of
   *other* checks on the same table and field.

So a zero denominator alone makes a check not-applicable **only at CONCEPT level**. At
FIELD and TABLE level, non-applicability comes from the table or field being missing or
empty, looked up per table and per field — never from the individual check's own
denominator.

### Why it matters

Any check whose denominator comes from joining a *different* table is affected. Take
`plausibleBeforeDeath` on a CDM where `DEATH` exists but holds no rows: our denominator is
0, so we report `NOT_APPLICABLE`. Upstream evaluates the checked table
(`CONDITION_OCCURRENCE`), finds it neither missing nor empty, and reports **pass**.

A CDM extract with no deaths, or no visits, is entirely ordinary. The divergence therefore
fires on real data, and it moves checks out of the scored population — which changes the
score Task 11 computes. That is why this is fixed before Task 11 rather than after.

## The authoritative rules

`.applyNotApplicable`, in evaluation order. The order matters: earlier returns win.

1. `measurePersonCompleteness` → NA **iff** the table is missing. Nothing else makes it NA.
2. `cdmTable` → **never** NA, whatever else is true.
3. `cdmField` → NA **iff** the table is missing.
4. Any other check → NA if the table is missing **or** the field is missing.
5. An error not caused by a missing table or field → **not** NA. Errors stay errors.
6. Table empty → NA.
7. `measureValueCompleteness` → **never** NA from an empty field. It is the check that
   measures emptiness, so it must keep reporting.
8. Field empty, or concept missing, or concept-and-unit missing → NA.
9. Otherwise not NA.

Plus one special case: `measureConditionEraCompleteness` is NA when `CONDITION_OCCURRENCE`
is missing **or empty**.

### The derived variables

- `tableIsMissing` — the `cdmTable` check for that `cdmTableName` **failed**.
- `fieldIsMissing` — the `cdmField` check for that `(table, field)` **failed**.
- `tableIsEmpty` — derived from a per-table lookup of `numDenominatorRows == 0`. Read
  `calculateNotApplicableStatus.R` lines ~133–160 to see exactly which check supplies it,
  and reproduce that, rather than inventing an equivalent.
- `fieldIsEmpty` — `numDenominatorRows == numViolatedRows` on that field's
  `measureValueCompleteness` result, i.e. every value is null.
- `conceptIsMissing` — `checkLevel == "CONCEPT"` and no unit concept id and
  `numDenominatorRows == 0`.
- `conceptAndUnitAreMissing` — `checkLevel == "CONCEPT"` and a unit concept id is present
  and `numDenominatorRows == 0`.

### The gate

The whole reclassification pass runs only when the batch contains `cdmTable`, `cdmField`
**and** `measureValueCompleteness`. On a batch lacking any of them, upstream leaves every
result as pass or fail. Reproduce this; it is what makes partial-catalog runs behave
predictably.

## Task 4 revision — `evaluate.py`

Delete the blanket zero-denominator rule. `evaluate()` becomes:

- `ERROR` and `NOT_APPLICABLE` still pass through untouched.
- Threshold `<= 0` → fail on any violation; threshold `> 0` → fail only when the violated
  percentage is strictly greater. Unchanged.
- **No denominator inspection at all.** A zero denominator with zero violations is a pass
  at this layer; the runner reclassifies it if a named rule applies.

Update the tests that pinned the old behaviour. `test_empty_denominator_is_not_applicable`
must be replaced by one asserting a zero denominator now yields `PASS`, with a comment
recording that applicability is the runner's job and why. Keep every other threshold test —
they are correct and were verified against upstream.

`pct_violated_rows` keeps its zero-denominator guard: that is arithmetic, not policy.

## Task 10 revision — `runner.py`

`run_checks` gains a reclassification pass after all checks have run and been evaluated.
It needs the whole result set, so it cannot be done per check.

- Build the derived variables above from the evaluated results, indexed by table and by
  `(table, field)`.
- Apply `.applyNotApplicable`'s ordered rules to every result, converting `PASS`/`FAIL` to
  `NOT_APPLICABLE` where they fire. Never touch `ERROR`, and never convert `NOT_APPLICABLE`
  back.
- Carry a reason string, mirroring upstream's `notApplicableReason` — the existing
  `CheckResult.message` field is the right home. Reasons are useful in the QALITA
  recommendations Task 11 builds.
- Gate the whole pass on the batch containing the three required check names.

Keep the existing `fieldIsEmpty` handling if it already matches rule 8; fold it into the
ordered implementation rather than leaving two mechanisms.

## Testing

Each of the nine ordered rules needs a test that fails if that rule is removed, proven by
an alter/run/observe/revert cycle with the observed output in the report. The ordering
itself needs at least one test: a `cdmTable` check on a missing table must stay `FAIL` and
never become `NA`, because rule 2 precedes rule 4.

Also pin the gate: a batch lacking `measureValueCompleteness` must leave results
unreclassified.

Re-measure the full-catalog tally against `mini_cdm` afterwards. It will shift — the
current 88 FAIL / 2286 NOT_APPLICABLE / 165 PASS / 0 ERROR was computed under the old
blanket rule. Report the new tally and explain the direction of the shift.

## Not in scope

The remaining checks' internals. This revision changes only when a computed result is
reported as not-applicable, never what any check computes.
