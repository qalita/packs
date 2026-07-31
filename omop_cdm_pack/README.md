## OMOP CDM Pack

Evaluates an OMOP Common Data Model instance against the check suite of the
[OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard):
27 check types instantiated over the CDM specification into 2 539 checks for CDM 5.4
(2 021 for CDM 5.3), grouped by the three Kahn framework categories (conformance,
completeness, plausibility).

Everything runs in Polars, lazily and in streaming mode. No R, no JVM, no SQL pushdown.

### Source configuration

Point the source at a schema holding OMOP CDM tables. The pack reads the CDM
specification to decide which tables to look for; tables absent from the source are
reported as failures of the `cdmTable` check rather than crashing the run. This is
unconditional — there is no config option to make a missing table fail the job instead.

The OMOP vocabulary tables (`CONCEPT`, `CONCEPT_ANCESTOR`) are **optional**. When they
are absent, the seven check types that need them are reported as `Not Applicable`.

### Metrics

| Key | Scope | Meaning |
|---|---|---|
| `score` | dataset | Share of passing checks, weighted by severity |
| `conformance_score` / `completeness_score` / `plausibility_score` | dataset | Per Kahn category |
| `score` | table | Share of passing checks for that CDM table |
| `pct_violated_rows` | column | Emitted only for failing checks |

### Attribution

Check metadata and check logic derive from the OHDSI DataQualityDashboard, licensed
under Apache 2.0. See `NOTICE`. All other files are proprietary to QALITA SAS.
