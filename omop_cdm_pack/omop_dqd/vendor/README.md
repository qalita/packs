# Vendored OHDSI DataQualityDashboard metadata

Source: https://github.com/OHDSI/DataQualityDashboard
Commit: 608b690b6c549d555fd2b5b713895b709dc05d5c
Retrieved: 2026-07-31
License: Apache License 2.0 (see `../LICENSE-APACHE-2.0.txt`)

`csv/` contains the check metadata files copied **verbatim** from `inst/csv/` upstream.
They are never modified. To refresh them, re-run the download in Task 1 Step 2 of
`docs/superpowers/plans/2026-07-31-omop-cdm-pack.md` and update the commit SHA above.

The repository root `.gitignore` ignores `*.csv`, with an explicit negation for
`omop_cdm_pack/omop_dqd/vendor/csv/*.csv` so these 8 files stay trackable by a plain
`git add`. If that negation is ever removed, a refresh will appear to succeed and commit
nothing. Verify with `git check-ignore -v omop_cdm_pack/omop_dqd/vendor/csv/*.csv`, which
must print nothing.

`.pre-commit-config.yaml` also excludes this directory from every formatting hook. Both
protections must stay: the files' byte-for-byte fidelity to upstream is what makes the
Apache-2.0 attribution in `NOTICE` accurate.
