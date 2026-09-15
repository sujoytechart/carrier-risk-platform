# Phase 1 Temporal Correction Plan

**Spec:** `docs/design/phase-1-event-spine.md`

## Global Constraints

- Both `event_date < scoring_date` and `reported_date < scoring_date` are mandatory.
- Knowledge intervals use `knowledge_valid_from < scoring_date` and an exclusive `knowledge_valid_to`.
- Inspection lookback is six months. Crash lookback is twenty-four months.
- `record_hash` detects payload changes and is never a source or version identity.
- Invalid non-null source values are quarantined with stable reasons.
- Only complete snapshots can produce deletions. Inspection retention expiry never creates a tombstone.
- All modeled dbt relations have enforced contracts.
- All three temporal tests are tagged `temporal` and retain error severity.
- No secrets, raw federal data, suppressed checks, skipped tests, or unfinished stubs enter source control.

## Repair conformance and knowledge-history edge cases

This work is limited to `dbt/**`, `dbt_project.yml`, `tests/dbt_support.py`, and
the dbt integration tests. Orchestration, ingestion, infrastructure, and runtime
configuration are outside this correction's scope. Split history logic only when
a named, cohesive macro makes the behavior easier to verify.

- [ ] Add regression tests and record expected failures before implementation.
- [ ] Prevent production `dbt build` from seeding over raw federal tables: fixture seeds are opt-in with `load_test_fixtures`, false by default. Update test commands accordingly.
- [ ] Guard all parser conversions, including integer bounds and real calendar validity. Invalid non-null change dates, crash counts, booleans, report times, and sequence values receive stable quarantine reasons. Missing optional values remain distinguishable from zero/false in canonical hashes.
- [ ] Require stable crash identity: a missing crash ID needs every documented fallback component. Namespace fallback keys to avoid collisions.
- [ ] Allow reported date equal to event date. Only earlier reported dates violate chronology.
- [ ] Preserve eligibility-changing corrections, closing the eligible predecessor without retaining a stale current carrier event.
- [ ] Include finite source-version ends in incident boundaries. Changes to carrier/incident identity must close the old incident.
- [ ] Pin the raw batch input for a model build, including valid empty batches. Never apply a new raw batch whose clean candidates have not been published. Recheck applied batches after acquiring serialization locks. Add deterministic regression tests for publication races and replay concurrency.
- [ ] Record inspection retention expiry separately without creating a deletion version.
- [ ] Enforce the actual event_versions contract within its custom materialization and prove an intentional schema/type mismatch fails.
- [ ] Remove the hardcoded scoring-date default. When an explicit scoring_dates list is supplied, validate ISO month-start values. Otherwise derive a reproducible monthly grid from complete input metadata. Document the choice. No arbitrary current-time default.
- [ ] Strengthen replay comparisons to include event_version_key and every persisted version field, and retain a committed fixture proving the reported-date clock independently changes contributions.
- [ ] Run focused red/green tests, then all dbt integration tests on disposable local PostgreSQL. Run Ruff and formatting over changed Python. Document exact commands/output.
- [ ] Review and commit the changes in meaningful groups without weakening checks.

Run the integration tests against a disposable local PostgreSQL database. Tests
read `CARRIER_RISK_TEST_DATABASE_URL`. Use libpq conninfo syntax and supply the
password through `PGPASSWORD`, never through a literal password URL in source.
Native Airflow uses a separate warehouse database.
