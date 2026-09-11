# Phase 1 verification report

This report records the short-lived AWS acceptance run completed on 2026-09-11
(UTC). It contains no account numbers, IP addresses, role ARNs, database
credentials, or raw federal records.

## What was verified

The acceptance path was:

```text
validated snapshot
  -> manifest published last in S3
  -> manifest-only S3 notification in SQS
  -> Airflow transactional raw load
  -> asset-triggered dbt build
  -> correction-aware event history and point-in-time features
```

The temporary stack used the deliberately smallest practical shape: two
server-side-encrypted SQS queues, one encrypted single-AZ PostgreSQL 17.9
`db.t4g.micro` with 20 GiB gp3 storage, a two-subnet VPC with no NAT gateway, and
a least-privilege loader role. During the local Airflow proof, database ingress
was limited to one reviewed IPv4 `/32`, and PostgreSQL required certificate
validation with `sslmode=verify-full`.

The full Phase 0 snapshots remain in S3: 4,986,413 crash rows and 8,281,794
inspection rows. Loading all 13.3 million rows into a temporary 20 GiB micro
database would have added time and cost without testing another interface, so
the live Phase 1 run used contract-valid synthetic acceptance snapshots instead.
Those fixtures exercised the real AWS and application path; they are not
presented as production data or scale evidence.

## Retry, commit, and replay behavior

The retry proof sent a crash-manifest notification before its snapshot object
existed. The mapped Airflow task received `NoSuchKey` on attempts 1 through 3,
left the message unacknowledged, and succeeded on attempt 4 after the two-row
snapshot and manifest were published. This demonstrated at-least-once recovery
without weakening the task's retry policy.

The acceptance data consisted of:

- two vehicle rows belonging to one synthetic crash incident;
- a complete two-row inspection snapshot; and
- a later complete inspection snapshot that changed one inspection from three
  violations and one out-of-service violation to seven and two.

An older duplicate notification was then consumed before the correction. Its
successful replay did not insert another batch or event version. After the
correction was consumed, the arrival queue reported zero available, zero
in-flight, and zero delayed messages.

The final warehouse counts were:

| Relation or invariant | Count |
|---|---:|
| Completed snapshot batches | 3 |
| Raw crash rows | 2 |
| Raw inspection rows | 4 |
| Event versions | 5 |
| Current deduplicated events | 3 |
| Training feature rows | 1 |
| Duplicate completed batches | 0 |
| Overlapping knowledge intervals | 0 |

The two crash vehicle rows became one carrier-level incident in the current
event set. The four raw inspection rows are two complete versions of the same
two-row source snapshot, while only the changed source record gained another
event version.

## Two-clock correction proof

Inspection `9001` retained the same event date while its reported and knowledge
times changed:

| Version | Event date | Reported date | Knowledge interval | Violations | OOS violations |
|---|---|---|---|---:|---:|
| Original | 2026-06-15 | 2026-06-17 | `[2026-06-17 12:00, 2026-09-05 12:00)` | 3 | 1 |
| Corrected | 2026-06-15 | 2026-09-05 | `[2026-09-05 12:00, infinity)` | 7 | 2 |

Direct as-of queries on either side of the correction boundary returned:

| Knowledge time | Violations | OOS violations |
|---|---:|---:|
| 2026-09-05 11:59 UTC | 3 | 1 |
| 2026-09-05 12:01 UTC | 7 | 2 |

This is the key Phase 1 guarantee: a later correction changes the current view
without leaking that correction into an earlier scoring time.

## Deterministic backfill proof

The parameterized inspection backfill for 2026-09-04 through 2026-09-05 ran
twice through the normal manifest loader and dbt build. Both runs succeeded.
Counts remained unchanged and SHA-256 hashes of stable, explicitly ordered
business columns matched before, after the first replay, and after the second:

| Relation | SHA-256 |
|---|---|
| `modeled.event_versions` | `2e75bafbdc2f7a3217a75da4f0420e041700d0b2ff9bc2e7e7c45d731121bf41` |
| `modeled.current_events` | `30f99c2f884e469d3c045a43762a7375182a2dcd0e7d8d917a9cd0206112fb23` |

The hashes are acceptance evidence, not public contracts; schema evolution can
legitimately change them.

## Test evidence

The final live dbt artifact contained 12 successful models, 91 passing tests,
one successful startup operation, and zero failures. It included the three
blocking temporal singular tests:

- no impossible event chronology;
- no overlapping event-version knowledge intervals; and
- training features equal an independently recomputed point-in-time result.

The repository's complete local suite passed all 133 tests in one run, reporting
88% total coverage and 90.79% changed-line coverage. Its temporal fixture also
ran dbt under an America/Detroit database session and proved that a version first
known after midnight UTC cannot leak into that day's score. Ruff, formatting,
strict mypy, the Terraform formatting/validation/mock-provider suite, and the
repository floor guard also passed. These checks are rerun from a clean working
tree before a release.

## Teardown evidence

The reviewed disable plan was exactly `0 add, 0 change, 19 destroy`. Applying it
removed the RDS instance, both SQS queues, the loader role, the database network,
and the S3 notification. Independent checks then showed:

- no matching RDS database instances;
- no matching SQS queues;
- no `module.spine` resources in Terraform state; and
- the Phase 0 raw bucket and ten immutable snapshot/manifest objects still
  present.

This is an operational part of the design, not cleanup trivia. RDS is rebuilt by
replaying the retained manifests, so the raw bucket is the recovery source and
the warehouse is disposable between learning sessions.

## Screenshot evidence

Sanitized screenshots under `docs/evidence/phase-1/` show:

1. successful Airflow `load_events` and `build_tables` runs plus both named
   backfills; and
2. the post-teardown AWS inventories with `Databases (0)` and `Queues (0)`.

The AWS account banner and assumed-role session were cropped before the images
were committed. The live RDS and SQS configuration is recorded by the reviewed
Terraform plan and machine-readable acceptance results above; the stack was not
recreated merely to produce additional screenshots. Screenshots support those
checks rather than replacing them.
