# Snowflake history and writer guard

The Snowflake target implements guarded candidate publication and event-history
materialization. Local SQL checks cover the transaction structure and contracts.
Live acceptance status is recorded separately in the
[verification record](../../docs/phase-2-verification.md).

Use a dedicated writer user and a small test warehouse with auto-resume disabled,
short query and idle timeouts, and a resource monitor. The launcher never resumes
or resizes the warehouse. Install the optional Snowflake dependency and configure
the variables in [the example profile](../../profiles.yml.example). Keep private
keys outside the repository.

During a bounded setup session, explicitly resume the test warehouse and select
the intended disposable database, then run
[`initialize_build_guard.sql`](initialize_build_guard.sql) once before starting
writers. The initializer preserves existing rows. Never replace, truncate, or
reinitialize a guard table to clear a stuck build. The launcher refuses missing,
duplicate, malformed, or already-owned singleton state.

With that warehouse running for the bounded session, run:

```sh
python -m orchestration.dbt_cli --warehouse-target snowflake seed \
  --vars '{load_test_fixtures: true}'
```

The launcher selects the `snowflake` profile target. It commits a unique run claim,
then holds a separate write transaction on the singleton for the entire dbt
subprocess. The startup hook checks that exact transaction and fully qualified
guard table through `SHOW LOCKS`. The launcher and dbt must use the same dedicated
user. Worker DDL cannot commit the guard connection's transaction.

After a successful synchronous command, the launcher checks its lock, explicitly
rolls back that transaction, and clears only its own claim. A failure, interrupted
connection, or lost lock leaves the committed claim in place. Another launcher
must refuse it even if the old transaction no longer holds a lock. The guard is
an operational boundary for these launchers and trusted dbt code, not an access
control mechanism against a user who can modify the guard table directly.

## Recovery

Recovery is deliberately explicit. There is no timeout that automatically erases
ownership and no unattended force-unlock command.

1. Stop the old launcher and its dbt process tree. Establish which host and run
   owned the recorded claim. If that cannot be determined, keep it blocked.
2. Identify the old run's warehouse sessions, queries, and transactions using
   retained private logs and live metadata. Stop the specific outstanding work
   and verify terminal query state. Closing a client or aborting its guard
   transaction alone is insufficient.
3. Reconcile any partially completed model work. The whole-command guard does
   not make dbt's separate model DDL or the separate publication and history transactions atomic.
4. Abort any remaining guard transaction only after old work is stopped. Verify
   no old writer remains, then clear the exact reviewed `owner_run` with a
   conditional update that affects exactly one row.
5. Run a new guarded build and suspend the dedicated warehouse afterward.

If worker termination, transaction cleanup, or business-state reconciliation is
uncertain, leave the claim intact. Suspending the dedicated warehouse limits
compute use but does not by itself establish safe application recovery.

The live acceptance checks use synthetic rows. They cover normal completion,
failed-worker retention, independent worker DDL, killed-process recovery, direct
command rejection, stale transaction rejection, and suspension. They do not prove
full event-history rollback, replay, or the three temporal detectors on Snowflake.

## Business transactions

`event_change_candidates` uses a Snowflake materialization that stages candidates
and complete-batch metadata before a DML-only publication transaction. The old
candidate/registry pair remains intact if validation or insertion fails. Empty
complete batches remain explicit registry rows. Duplicate batch IDs, duplicate
nonnull source identities within a batch, mismatched feeds, observation timestamps,
and row counts reject publication.

`event_versions` precreates history, applied-batch, retention, and temporary prior
relations outside its transaction. It validates the persisted history schema and
then applies all pending batches in observation-time/batch-ID order inside one
transaction. An exception explicitly rolls back transitions, last-seen updates,
successor links, retention rows, and application markers. Snowflake sequences can
consume values during rollback. Gaps in surrogate IDs do not represent events.

The first processed batch for each feed uses source-proxy knowledge time. Later
batches use observed knowledge time, including when the first batch was empty.
Crash disappearance produces tombstones. Inspection disappearance older than the
batch minimum event date records retention lineage. Excluded corrections close
previous eligible history. Replaying already-applied batches leaves history and
application timestamps unchanged.

Persisted schema checks distinguish NUMBER(38,0), timestamp_tz(9), and explicit
VARCHAR widths. Text widening is accepted. Narrowing below 16,777,216 characters,
fractional integer scale, changed timezone types, missing/extra columns, identity,
or nullability drift requires an explicit migration. Standard-table constraints
are supplemented by transaction-local uniqueness, chronology, current-version,
link, and retention validation. Source timestamps are interpreted explicitly as
UTC and observation dates are extracted in UTC.

Run the focused offline checks with:

```sh
.venv/bin/pytest tests/test_snowflake_history_sql.py tests/test_dbt_portability_sql.py
```

These checks render production macros, exercise validation predicates locally,
and compile all models. They are not evidence of live Snowflake grammar, rollback,
replay, or transformation parity. A bounded guarded build must establish those
properties in the dedicated disposable warehouse before claiming acceptance.
