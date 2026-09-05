# Phase 1 Event Spine Design

**Status:** Approved

**Date:** 2026-09-04

## Purpose

Phase 1 turns immutable FMCSA snapshots into a queryable, point-in-time-correct
event history. A successful build can answer which version of every inspection
or crash was knowable at an arbitrary scoring date, and it proves that answer
with three blocking temporal tests.

The phase deliberately stops before label-maturity calculation, model training,
Snowflake portability, and serving. It builds the trustworthy event spine those
later phases require.

## Design principles

- Raw source values remain unchanged and auditable.
- A manifest is the only signal that a snapshot is complete enough to load.
- A deterministic batch identity makes every retry and backfill idempotent.
- Source record identity and payload change detection remain separate concepts.
- Invalid non-null source values are quarantined with stable reasons.
- Knowledge time comes from source proxies for pre-platform history and observed
  acquisition time for versions first seen by this platform.
- Version transitions are transactional: a predecessor is closed only when its
  successor is inserted successfully.
- Airflow orchestrates boundaries; it does not contain parsing, loading, or
  temporal business logic.
- RDS exists only during working sessions and must be destroyable without
  affecting the immutable S3 evidence base.

## Architecture

```text
FMCSA export
    |
    v
immutable S3 snapshot + manifest
    |
    +-- S3 ObjectCreated(manifest.json) --> SQS --> Airflow load_events
                                                  |
                                                  v
                                      raw snapshot batches and rows
                                                  |
                                      Airflow dataset dependency
                                                  |
                                                  v
                                      dbt clean and modeled build
                                                  |
                       +--------------------------+-------------------+
                       |                          |                   |
                       v                          v                   v
                event_versions             event views       crash incidents
                       |
                       v
             point-in-time features + temporal tests
```

Local development uses the same Python and DAG code with MinIO, an
SQS-compatible local endpoint, and container Postgres. Environment-backed
adapters select local or AWS endpoints; domain code does not branch by profile.

## Batch identity and completeness

`batch_id` is the lowercase SHA-256 hex digest of this length-prefixed tuple:

```text
(feed_name, dataset_id, observed_at, object_key, object_sha256)
```

Length-prefixing prevents ambiguous concatenation. Future manifests carry the
identifier explicitly. Existing Phase 0 manifests derive it from the same fields,
so their identity is stable without rewriting immutable objects.

Before a batch is eligible for loading, the loader verifies:

- the manifest feed and dataset match a configured feed;
- the manifest object key belongs to the same feed and acquisition date;
- the S3 object's size and checksum metadata match the manifest;
- the manifest columns exactly match the versioned feed contract;
- the copied row count matches the manifest row count; and
- the batch reached `loaded` status in one database transaction.

Only a loaded, complete snapshot may drive disappearance detection. A failed or
partial batch cannot close an event version.

## Raw loading

The warehouse contains one batch registry plus one raw table per feed:

- `raw.snapshot_batches` records immutable manifest metadata and a load status.
- `raw.inspection_rows` preserves every source field as text plus `batch_id` and
  `source_row_number`.
- `raw.crash_rows` follows the same contract.
- `raw.row_quarantine` records source lineage, the original row payload, and one
  or more stable parse reasons.

The Python loader streams gzip CSV from object storage into PostgreSQL `COPY`.
It never loads a full feed into memory. A unique `batch_id` prevents duplicate
loads, while `(batch_id, source_row_number)` gives every physical input row an
auditable identity.

Database and object-store operations sit behind small protocols. Tests use real
Postgres for transaction and replay behavior and focused fakes only at remote
service boundaries.

## Clean data contracts

dbt clean models preserve raw lineage and produce typed, normalized fields.

USDOT identifiers accept only positive integral values and are stored as text.
Event dates use strict `YYYYMMDD` parsing. Source audit timestamps use strict
`YYYYMMDD HHMM` parsing and are not passed through the event-date parser.
Invalid non-null values produce a quarantine reason rather than a null that looks
like missing source data.

Stable source keys are `inspection_id` for inspections and `crash_id` for
crashes. A crash without `crash_id` uses an unambiguous encoding of
`(report_state, report_number, report_date, report_time, report_seq_no)`.
`record_hash` is calculated from a canonical representation of the modeled
payload and is used only to detect changes.

Crash model eligibility requires a valid USDOT number, a federally recordable
flag, and valid incident-key fields. Excluded rows remain queryable with an
explicit reason.

## Knowledge-time policy

The first loaded snapshot contains historical records that predate this
platform. Their `reported_date` and `knowledge_valid_from` use the feed-specific
source add timestamp plus one publication day:

- crashes: `add_date + 1 day`;
- inspections: `mcmis_add_date + 1 day`.

Those rows carry `availability_quality = 'source_proxy'`. A new source key or a
changed payload first discovered in a later platform snapshot uses that batch's
`observed_at` and carries `availability_quality = 'observed'`.

If a proxy would make a version visible before its event date, the row is
quarantined. The pipeline does not clamp or silently repair the dates.

## Event version transitions

`modeled.event_versions` is an incremental table with a bigint surrogate key.
The merge runs through a dbt materialization backed by adapter-dispatched SQL.
Phase 1 implements Postgres; Phase 2 adds Snowflake dispatch without changing the
model interface.

For each complete batch, the transaction applies these transitions:

1. A new `(feed_name, source_record_key)` inserts one open version.
2. An unchanged `record_hash` updates only `last_seen_batch_id`.
3. A changed hash inserts a successor, closes the predecessor at `observed_at`,
   and sets `superseded_by_version_key`.
4. A confirmed source deletion inserts a tombstone and closes its predecessor.
5. An inspection older than the new complete snapshot's minimum event date is
   classified as retention expiry and does not create a tombstone.

The transaction is serialized per feed. Batches normally apply in ascending
`observed_at` order. Replaying an already applied batch is a no-op; introducing a
previously unseen older batch requires the controlled rebuild path rather than
mutating history out of order.

## Modeled relations

Every modeled relation has an enforced dbt contract.

- `event_versions` preserves all knowledge-time versions and tombstones.
- `inspections` and `crashes` are version-aware typed projections.
- `crash_incidents` deduplicates commercial-vehicle rows by
  `(usdot_number, report_state, report_number, report_date, report_time)`, taking
  maximum fatalities and injuries and boolean-or for tow-away.
- `events_union` retains validity bounds and deletion state so consumers can
  independently resolve an as-of version.
- `training_features` materializes event-only features for parameterized monthly
  scoring dates. Labels and label maturity remain Phase 3 work.

The as-of predicate is strict on both clocks and on the version interval:

```sql
knowledge_valid_from < scoring_date
and (knowledge_valid_to is null or knowledge_valid_to > scoring_date)
and event_date < scoring_date
and reported_date < scoring_date
and not is_deleted
```

## Airflow orchestration

S3 publishes an event only when a `manifest.json` commit marker is created. The
queue has long polling, server-side encryption, a dead-letter queue, and five
receives before redrive.

`load_events` continuously waits for messages, validates their bucket and key,
and dynamically maps one load task per manifest. A message is deleted only after
its raw batch transaction commits. Visibility timeout exceeds the expected load
duration, and redelivery is harmless because the loader is idempotent.

Successful raw loads publish an Airflow dataset event. `build_tables` consumes
that dataset, runs the dbt build for the affected batches, and executes the
temporal tests. `backfill` is manually triggered with feed and acquisition-date
parameters, resolves manifests in ascending observation order, and uses the same
load and build functions as normal arrival processing.

## Failure behavior

- Unknown schemas fail the batch before any raw rows become visible.
- Malformed rows are preserved in quarantine; batch counts must still reconcile.
- Database writes roll back together on copy or reconciliation failure.
- A dbt contract or temporal test failure stops downstream publication.
- SQS messages remain available after failed loads and eventually reach the DLQ.
- Out-of-order new history fails with an actionable rebuild instruction.
- A failed Terraform apply is safe to retry; Phase 1 resources remain tagged and
  individually discoverable for teardown verification.

## Testing and evidence

Python unit tests cover manifest compatibility, deterministic batch identity,
message parsing, schema validation, and strict source parsing. PostgreSQL
integration tests cover raw replay, version insertion, correction, deletion,
retention expiry, and transaction rollback.

The three dbt tests tagged `temporal` are blocking:

1. no version is reported before its event date;
2. versions of one source event never overlap in knowledge time; and
3. stored features equal an independent two-clock recomputation.

The fixture for the third test contains an event where
`event_date < scoring_date <= reported_date`, ensuring a one-clock
implementation fails.

Phase evidence includes a real SQS-triggered load, one deliberate retry, one
backfill replayed twice without data changes, an as-of correction query, full
quality-gate output, and a Terraform destroy inventory showing no surviving RDS
or SQS resources.

## Delivery boundaries

Phase 1 does not add Snowflake, Glue, Athena, ECR, remote Terraform state,
label-maturity calculation, MLflow, model training, or FastAPI. It may add the
feature relation required by the third temporal test, but not labels or a
trainable dataset.

The public repository receives code, tests, dbt models, DAGs, Terraform, and
operator documentation. Raw federal data, credentials, local state, run logs,
screenshots containing account details, and process-only instruction files stay
uncommitted.
