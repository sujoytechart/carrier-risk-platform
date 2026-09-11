# PostgreSQL and Snowflake semantics

PostgreSQL remains the operational warehouse. Snowflake is an optional
transformation-portability target, as described in [ADR 0005](adr/0005-snowflake-for-portability-not-scale.md).
The shared transformations compile for both adapters. A complete Snowflake build
is not yet supported: candidate publication, event-history mutation, persisted
contract checks, and full regression parity still require implementation and live
verification. The whole-command writer guard now has a bounded live proof.

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

Snowflake DDL commits an active transaction. A future Snowflake implementation
must precreate persistent and staging relations outside the transaction, then
publish candidates and apply history with DML-only transactions. Closing an old
version, inserting its successor, linking versions, and recording batch application
must either all commit or all roll back. Standard-table uniqueness and chronology
also need explicit validation. See the [Snowflake transaction reference](https://docs.snowflake.com/en/sql-reference/transactions).

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
Compilation checks Jinja and dependency resolution; it does not validate warehouse
grammar, execution, transaction safety, or persisted types.

`dbt/tests/portable_scalar_regressions.sql` is executable warehouse SQL. It and the
shared model regression scenarios still have to pass on each real target before
full transformation parity can be claimed. Live event-history mutation, rollback,
schema-drift, and replay evidence remains outstanding; the guard and one-row dbt
seed proof are recorded separately.
