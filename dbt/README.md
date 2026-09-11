# Warehouse build boundary

PostgreSQL is the supported execution target. Shared transformation SQL compiles
for Snowflake, but its history publication, writer guard, and full live regression
proof remain incomplete. The [adapter guide](../docs/adapter-differences.md)
records the semantic differences; the [Phase 2 verification record](../docs/phase-2-verification.md)
separates compilation checks from warehouse execution evidence.

Use `python -m orchestration.dbt_cli build`, `run`, or `seed` for mutating dbt
commands.
The Python launcher holds PostgreSQL session advisory lock `764301234` for the
entire dbt subprocess and exports its backend PID as `CARRIER_RISK_DBT_LOCK_PID`.
The startup hook verifies that PID owns this exact lock in the target database.
Unsupported direct mutations fail with the launcher command. Plain `dbt test`,
`compile`, and `list` do not need the lock. The lock must outlive all worker
connections: this adapter closes its startup-hook connection before models run.

`clean_snapshot_batches` pins the loaded input metadata before either feed is
conformed. Candidate publication reconciles every pinned batch's row count,
including excluded and quarantined rows. Its transactional post-hook publishes
`intermediate.event_candidate_batches` with the candidate table. Empty complete
batches have metadata and zero candidates. History only consumes that publication;
raw arrivals after it are eligible for the next complete build.

Fixture seeds are disabled by default. Only disposable test databases should use
`--vars '{load_test_fixtures: true}'` when seeding. Production builds never load
those fixtures over raw federal tables.

Source history retains eligibility-changing corrections with
`is_model_eligible = false` and `exclusion_reason`. Carrier projections select
eligible versions; finite predecessor ends still delimit the old incident.
`events_union` preserves each crash vehicle version for strict as-of queries;
apply both clocks before counting distinct `carrier_crash_key` values or aggregating
severity. `crash_incidents` is an interval summary for browsing, and `current_events`
uses it to show one current row per incident. The summary is not the exact-instant
as-of input: a boundary on one vehicle must not hide another unchanged vehicle.
Excluded versions may have missing carrier/event fields, while the table enforces
their presence for eligible versions. Invalid source values remain in clean
quarantine with stable parse reasons. Retention expiry records acquisition
lineage in `modeled.inspection_retention_expiries` and creates no deletion version.
When an excluded correction supplies a future event date, that rejected date stays
in raw/clean quarantine; the exclusion version records a null modeled event date
and closes its predecessor. Blank non-null values receive parse reasons as well.

Explicit `scoring_dates` must be a nonempty list of ISO `YYYY-MM-01` strings.
Without that list, the monthly grid spans the first through last successfully
applied complete-batch observation month in UTC, inclusive. This is reproducible
from stored input metadata and deliberately independent of today's date. Supply
explicit historical month starts when a retrospective scoring window is needed.
Every scoring date means midnight UTC regardless of the PostgreSQL session
timezone.

Changing the event-history schema or canonical hash requires a controlled rebuild
from immutable snapshots; existing histories are not silently migrated. The custom
materialization checks the actual persisted columns and types against the dbt
contract before applying any batches.

`tests/test_temporal_dbt.py` commits a synthetic late-report fixture with event
date January 20, knowledge start February 1, report date April 1, and scoring date
March 1. Only the reported-date predicate excludes it: the test independently
proves that removing this clock changes the inspection contribution.
