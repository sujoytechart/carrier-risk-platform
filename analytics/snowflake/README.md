# Snowflake writer guard

The Snowflake launcher supports guarded synthetic verification. Full event-history
publication and transformation parity remain incomplete; see the
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
guard table through `SHOW LOCKS`; the launcher and dbt must use the same dedicated
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
   owned the recorded claim; if that cannot be determined, keep it blocked.
2. Identify the old run's warehouse sessions, queries, and transactions using
   retained private logs and live metadata. Stop the specific outstanding work
   and verify terminal query state. Closing a client or aborting its guard
   transaction alone is insufficient.
3. Reconcile any partially completed model work. The whole-command guard does
   not make dbt's separate model DDL or future publication transactions atomic.
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
