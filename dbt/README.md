# Warehouse build boundary

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
Excluded versions may have missing carrier/event fields, while the table enforces
their presence for eligible versions. Invalid source values remain in clean
quarantine with stable parse reasons. Retention expiry records acquisition
lineage in `modeled.inspection_retention_expiries` and creates no deletion version.

Explicit `scoring_dates` must be a nonempty list of ISO `YYYY-MM-01` strings.
Without that list, the monthly grid spans the first through last successfully
applied complete-batch observation month in UTC, inclusive. This is reproducible
from stored input metadata and deliberately independent of today's date. Supply
explicit historical month starts when a retrospective scoring window is needed.

Changing the event-history schema or canonical hash requires a controlled rebuild
from immutable snapshots; existing histories are not silently migrated. The custom
materialization checks the actual persisted columns and types against the dbt
contract before applying any batches.

`tests/test_temporal_dbt.py` commits a synthetic late-report fixture with event
date January 20, knowledge start February 1, report date April 1, and scoring date
March 1. Only the reported-date predicate excludes it: the test independently
proves that removing this clock changes the inspection contribution.
