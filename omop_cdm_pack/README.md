## OMOP CDM Pack

Evaluates an OMOP Common Data Model instance against the check suite of the
[OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard):
27 check types instantiated over the CDM specification into 2 535 checks for CDM 5.4
(2 005 for CDM 5.3), grouped by the three Kahn framework categories (conformance,
completeness, plausibility).

Everything runs in Polars, lazily and in streaming mode. No R, no JVM, no SQL pushdown.

### Source configuration

Point the source at a schema holding OMOP CDM tables. The pack reads the CDM
specification to decide which tables to look for; tables absent from the source are
reported as failures of the `cdmTable` check rather than crashing the run. This is
unconditional — there is no config option to make a missing table fail the job instead.

The OMOP vocabulary tables (`CONCEPT`, `CONCEPT_ANCESTOR`) are **optional**. Four check
types depend on them outright — `fkDomain`, `fkClass`, `isStandardValidConcept` (all three
read `CONCEPT`) and `plausibleGenderUseDescendants` (reads `CONCEPT_ANCESTOR`) — plus the
`isForeignKey` instances whose referenced table is `CONCEPT`. On CDM 5.4 that is 232 of the
2 535 check instances (43 + 3 + 64 + 4, plus 118 of the 177 `isForeignKey` instances). When
the vocabulary is absent, those instances are reported as `Not Applicable`; every other
check, including the remaining 59 `isForeignKey` instances, runs normally.

### Metrics

| Key | Scope | Meaning |
|---|---|---|
| `score` | dataset | Share of passing checks, weighted by severity |
| `conformance_score` / `completeness_score` / `plausibility_score` | dataset | Per Kahn category |
| `fatal_failure_count` | dataset, table | Raw count of failing `fatal`-severity checks |
| `score` | table | Share of passing checks for that CDM table |
| `<checkName>_pct_violated_rows` | column, table | Emitted only for failing checks, e.g. `isRequired_pct_violated_rows`. The check name is part of the key because one column routinely fails several checks, and key+scope must identify a metric uniquely. Table-scoped for table-level checks, which have no column. |

The pack also emits `schemas`: one entry per present CDM table and one per checked column
(`TABLE.field`). The platform builds a source's table/column tree from these alone, so the
metrics above would have nothing to attach to without them.

### Attribution

Check metadata and check logic derive from the OHDSI DataQualityDashboard, licensed
under Apache 2.0. See `NOTICE`. All other files are proprietary to QALITA SAS.
