# 0001. Use an incremental `event_versions` model instead of dbt snapshots

Date: 2026-09-02
Status: Accepted

## Context

Federal event records can change after publication. For example, a carrier can
dispute an inspection or crash record through DataQs. The correction may arrive
long after the event occurred.

To answer "what was knowable on date X?", the platform must keep every version
of an event and the period when that version was current.

That is the problem dbt snapshots exist to solve, and the closest native
configuration is a real candidate:

```yaml
strategy: check
unique_key: [feed_name, source_record_key]
check_cols: [record_hash]
updated_at: observed_at
hard_deletes: new_record
```

`hard_deletes: new_record` is available in dbt Core 1.9 and later and is
supported by dbt-postgres and dbt-snowflake. It creates a deleted version when a
row disappears from the source. See the
[dbt configuration reference](https://docs.getdbt.com/reference/resource-configs/hard-deletes).

## Decision

Build `event_versions` as an incremental model with an explicit merge contract.
Do not use dbt snapshots. Keep `record_hash` strictly as a payload-change
detector, never as part of the merge key.

Snapshots handle ordinary updates well, but two problems prevent their direct
use here.

**Deletion timestamps come from the warehouse clock.** For inserts and updates,
a check snapshot can use `observed_at` as its `updated_at` value. A deleted row
is absent, so dbt instead uses `snapshot_get_time()`, which resolves to the
warehouse's current time. If a June batch is replayed in September, dbt records
the deletion in September. That makes historical rebuilds nondeterministic and
gives the deletion the wrong knowledge time.

**Retention expiry looks like deletion.** The public inspection file contains a
rolling three-year window. An old record eventually disappears even when FMCSA
has not deleted it. A snapshot only sees that the row is absent, so it would
create false tombstones for ordinary expiry.

Fixing these problems would require a custom clock override, staging logic that
classifies missing rows, a completeness gate, and separate batch-lineage logic.
That is as much custom behavior as the incremental model, but it is harder to
see and test.

## Consequences

- The merge logic is ours to get right, and it is the most correctness-critical
  code in the repository. It carries dedicated tests.
- All validity and tombstone timestamps derive from the immutable batch
  `observed_at`, so a rebuild from the raw snapshots is deterministic.
- Deletion is only inferred from a snapshot that passed completeness checks. A
  partial or failed download can never tombstone a record.
- Disappearance caused by the rolling window never tombstones the event. Source
  presence metadata may record the expiry separately.
- The `snapshots/` directory does not exist. A second snapshot representation
  will not be added purely to exercise the feature.

## Revisit if

dbt gains a way to supply the deletion timestamp explicitly, or the platform
moves to a source that publishes deletions as events rather than as absence.
