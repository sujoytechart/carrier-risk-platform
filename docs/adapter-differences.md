# PostgreSQL and Snowflake semantics

PostgreSQL remains the operational warehouse. Snowflake is an optional
transformation-portability target, as described in [ADR 0005](adr/0005-snowflake-for-portability-not-scale.md).
The shared transformations compile for both adapters. Snowflake now has dedicated
candidate publication, event-history mutation, and persisted contract checks.
Live synthetic-fixture acceptance passed the full build, nine-relation business
parity, exact replay, three deliberately failing temporal tests, persisted-schema
rejection, and transaction rollback. The whole-command writer guard also passed
competing-writer and killed-process probes. See the
[verification record](phase-2-verification.md) for scope and recovery details.

## Target configuration

Install `.[dev]` for credential-free compilation tests of both adapters, or
`.[snowflake]` to add the Snowflake adapter to a runtime environment.
[The example profile](../profiles.yml.example) keeps `local` as the default.
Selecting it does not require Snowflake environment variables. Selecting
`snowflake` requires the account, user, role, database, warehouse, and key-file
configuration shown in [.env.example](../.env.example).

Private keys and backend configuration belong outside version control. The
Snowflake profile uses key-pair authentication, UTC sessions, a 60-second
statement timeout, and no session keep-alive. Its single worker thread controls
parallelism; it does not establish a warehouse-wide writer lock.

## Shared business rules

Both event and reported dates must be strictly before the scoring date. A version
must also start strictly before midnight UTC on that date, and its finite end
must be strictly after that instant. The existing strict correction-boundary
behavior is preserved. A session's local timezone must not move scoring midnight.

Source dates and timestamps are parsed with explicit lexical, calendar, and range
checks. Blanks remain distinguishable from null source values, invalid values
remain quarantined, and parse-reason precedence is stable. Quarantine arrays use
adapter helpers because array indexing and null handling differ.

Feature ratios have the explicit contract `decimal(38,6)`; Snowflake's default
numeric scale is not suitable for fractional features. The scalar regression
suite includes fractional and rounding-boundary cases.

## Identity and hashes

| Value | PostgreSQL | Snowflake |
|---|---|---|
| Fallback source key | Existing lowercase UTF-8 hex of `json_build_array(...)::text`, prefixed with `fallback:` | Explicit encoding with the same JSON escaping, comma-space separators, dates, and integral numbers |
| Crash incident key | Existing MD5 of unit-separator-delimited identity fields | Same field order and explicit scalar formatting |
| Payload change detector | Existing `md5(jsonb_build_array(...)::text)` | MD5 of a JSON array with explicit JSON nulls and formatted source dates/timestamps |
| Knowledge timestamps | `timestamptz` | `timestamp_tz` |
| Source-clock timestamps | Timestamp without timezone | `timestamp_ntz` |

PostgreSQL's payload encoding stays unchanged so an upgrade does not manufacture
corrections for existing histories. Payload hashes are intentionally
adapter-specific change detectors; they are never source identities. Cross-target
comparisons must normalize business values and compare them independently of
surrogate version IDs and payload hashes. Within-target replay must still preserve
the complete version rows, links, retention lineage, and applied-batch markers.

The Snowflake source encoder handles quotes, backslashes, Unicode, and ASCII
control characters. NUL is outside the shared text domain because PostgreSQL text
does not support it. Local encoding tests execute basic string operations against
an independent JSON reference; they do not substitute for Snowflake execution.

## Transaction boundary

The PostgreSQL launcher holds a session advisory lock for the entire dbt command,
including worker connection changes. Its existing transactional history
materialization remains in place.

Snowflake DDL commits an active transaction. Its materializations precreate
persistent and staging relations outside business transactions, then publish the
candidate/registry pair and apply history using separate DML-only transactions.
All pending history batches share one transaction; closing versions, inserting
successors, linking versions, recording retention, and marking application either
commit together or explicitly roll back in the exception handler. Standard-table
uniqueness, chronology, and lineage are checked before and after history mutation. See the [Snowflake transaction reference](https://docs.snowflake.com/en/sql-reference/transactions).

The Snowflake launcher combines a committed run claim with a separate write
transaction. Failure retains the claim even when the transaction lock disappears;
explicit recovery must stop and reconcile old work before clearing it. Its
[operating procedure](../analytics/snowflake/README.md) and bounded live checks
cover competing processes, worker connection churn, normal failure, and
lost-parent recovery. These guard checks do not establish business-transaction
atomicity. Snowflake Time Travel does not supply the source
availability or correction-knowledge clocks maintained by this project.

## Verification boundary

`tests/test_dbt_portability_sql.py` compiles both targets with dummy profiles and
checks rendered model contracts and temporal tests without warehouse access.
`tests/test_snowflake_history_sql.py` additionally compiles intermediate models,
renders both materializations and production transaction macros, executes shared
validation predicates locally, and checks persisted type matching. Integer aliases
require NUMBER(38,0); timestamp_tz requires precision 9. Text widening is accepted
but narrowing below the explicit 16,777,216-character contract is rejected.
Compilation checks Jinja and dependency resolution; it does not validate warehouse
grammar, execution, transaction safety, or persisted types.

`dbt/tests/portable_scalar_regressions.sql` is executable warehouse SQL. It and the
shared model regression scenarios still have to pass on each real target before
full transformation parity can be claimed. Phase 2's live event-history mutation,
rollback, schema-drift, replay, and business-parity results are recorded in the
[verification report](phase-2-verification.md).

## Phase 3 operational additions

The monthly Python pipeline owns `modeled.label_maturity_watermarks` in
PostgreSQL. Its immutable metadata and current-version pointer are outside dbt's
event-model graph, so rebuilding features does not erase measurement history.
The additive `label_maturity_policy` temporal test validates that registry on
PostgreSQL. It returns no rows when the registry is absent and during Snowflake
execution; absence does not authorize training. Python independently requires a
complete, valid persisted policy before constructing training rows.

PostgreSQL-only dbt index configuration accelerates carrier/date lookups in
`inspections` and `training_features`. Model projections and the original three
temporal tests are unchanged. Offline adapter checks passed, but Phase 3 did not
resume Snowflake compute or rerun its live acceptance. See the
[Phase 3 verification report](phase-3-verification.md) for the local scope.
