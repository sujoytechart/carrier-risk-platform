# Phase 1 Event Spine Implementation Plan

**Goal:** Build an SQS-driven Airflow pipeline that loads complete FMCSA snapshots into RDS Postgres, preserves correction-aware event versions, and proves point-in-time correctness with three blocking dbt tests.

**Architecture:** Immutable S3 manifests become deterministic warehouse batches. Python owns remote I/O and transactional raw loading. The dbt project owns strict conformance, version-aware modeled relations, feature computation, contracts, and temporal tests. Airflow only coordinates those boundaries. Local adapters exercise the same workflow against MinIO, an SQS-compatible endpoint, and container Postgres before the short-lived AWS integration run.

**Tech Stack:** Python 3.12, PostgreSQL 17, psycopg 3, dbt-core/dbt-postgres, Apache Airflow with the Amazon provider, Docker Compose, Terraform, AWS S3/SQS/RDS/IAM.

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
- Changed-line coverage is at least 80%. Lint, formatting, and strict typing have zero errors.
- No secrets, raw federal data, suppressed checks, skipped tests, or unfinished stubs enter source control.
- Phase 1 RDS resources are destroyed between working sessions.

---

### Task 1: Phase 0 closeout and deterministic batch identity

**Files:**
- Modify: `README.md`
- Modify: `.gitignore`
- Modify: `pyproject.toml`
- Modify: `ingest/models.py`
- Modify: `tests/test_models.py`
- Create: `docs/architecture.svg`
- Create: `ingest/schemas/crashes.json`
- Create: `ingest/schemas/inspections.json`

**Interfaces:**
- Produces: `SnapshotManifest.batch_id: str`
- Produces: `SnapshotManifest.resolved_batch_id() -> str`
- Produces: `derive_batch_id(feed_name: str, dataset_id: str, observed_at: str, object_key: str, object_sha256: str) -> str`
- Compatibility: manifests without `batch_id` derive it from their immutable fields.

- [ ] Write tests proving batch IDs are deterministic, tuple-boundary-safe, validated as lowercase SHA-256, and derived for Phase 0 manifests.
- [ ] Run `pytest tests/test_models.py -v` and confirm failures identify the missing batch interface.
- [ ] Implement the length-prefixed identity encoding and backward-compatible manifest parser.
- [ ] Add versioned required-column contracts from the verified 2026-09-03 manifests.
- [ ] Export the committed Draw.io architecture to the README's SVG path and correct stale status text.
- [ ] Add runtime dependencies for psycopg, dbt, Airflow, and the Amazon provider with bounded compatible versions.
- [ ] Run `pytest tests/test_models.py -v`, Ruff, formatting, and strict mypy over `ingest`.
- [ ] Commit as `feat: define snapshot batch and schema contracts`.

### Task 2: Manifest validation and object-store boundaries

**Files:**
- Create: `ingest/contracts.py`
- Create: `ingest/object_store.py`
- Create: `tests/test_contracts.py`
- Create: `tests/test_object_store.py`
- Modify: `ingest/storage.py`

**Interfaces:**
- Produces: `FeedSchema.load(path: Path) -> FeedSchema`
- Produces: `validate_manifest(manifest: SnapshotManifest, schema: FeedSchema) -> None`
- Produces: `SnapshotObjectStore.open_snapshot(manifest: SnapshotManifest) -> ContextManager[BinaryIO]`
- Produces: `S3SnapshotObjectStore` and `FileSnapshotObjectStore` adapters.

- [ ] Write failing tests for unknown datasets, wrong feed paths, schema drift, object-size mismatch, checksum-metadata mismatch, and a valid legacy manifest.
- [ ] Run the focused tests and verify each fails because validation or the adapters do not exist.
- [ ] Implement immutable schema loading and manifest validation with actionable exceptions.
- [ ] Implement streaming S3 and filesystem adapters without loading archives into memory.
- [ ] Run the focused tests and the existing Phase 0 storage tests.
- [ ] Commit as `feat: validate complete snapshot manifests`.

### Task 3: Transactional raw warehouse loading

**Files:**
- Create: `ingest/database.py`
- Create: `ingest/raw_loader.py`
- Create: `ingest/sql/create_raw_schema.sql`
- Create: `tests/test_raw_loader.py`
- Create: `tests/integration/test_raw_loader_postgres.py`
- Create: `tests/support/postgres.py`

**Interfaces:**
- Produces: `RawSnapshotLoader.load(manifest: SnapshotManifest) -> RawLoadResult`
- Produces: `RawLoadResult(batch_id: str, inserted_rows: int, already_loaded: bool)`
- Produces: `create_raw_schema(connection: psycopg.Connection[tuple[object, ...]]) -> None`

- [ ] Write a failing unit test for row-count reconciliation and completed-batch replay.
- [ ] Write a failing PostgreSQL integration test showing a mid-copy failure leaves neither visible rows nor a completed batch.
- [ ] Run the focused tests and confirm expected failures.
- [ ] Create `raw.snapshot_batches`, feed row tables, and `raw.row_quarantine` with explicit keys and checks.
- [ ] Stream gzip CSV records through PostgreSQL COPY, preserving raw text and source row numbers.
- [ ] Lock by batch identity, reconcile counts, commit status last, and return a no-op result for a completed replay.
- [ ] Run unit and integration tests against container Postgres.
- [ ] Commit as `feat: load snapshot batches transactionally`.

### Task 4: dbt project and strict clean models

**Files:**
- Create: `dbt_project.yml`
- Create: `profiles.yml.example`
- Create: `dbt/models/sources.yml`
- Create: `dbt/models/clean/clean_inspections.sql`
- Create: `dbt/models/clean/clean_crashes.sql`
- Create: `dbt/models/clean/schema.yml`
- Create: `dbt/macros/parse_source_date.sql`
- Create: `dbt/macros/parse_source_timestamp.sql`
- Create: `dbt/macros/normalize_usdot.sql`
- Create: `dbt/macros/stable_key.sql`
- Create: `dbt/seeds/temporal_fixture_batches.csv`
- Create: `dbt/seeds/temporal_fixture_inspections.csv`
- Create: `dbt/seeds/temporal_fixture_crashes.csv`
- Create: `tests/test_dbt_clean.py`

**Interfaces:**
- Produces: `clean.clean_inspections` and `clean.clean_crashes` with typed dates, audit timestamps, source keys, eligibility, exclusion reason, canonical payload, and record hash.
- Produces: adapter-dispatched macros with Postgres implementations.

- [ ] Add fixture rows covering valid values, invalid non-null values, missing USDOT, non-federal crashes, and fallback crash identity.
- [ ] Write a failing dbt build test that asserts exact clean and quarantine outputs.
- [ ] Run the focused dbt test and verify it fails because the clean models are absent.
- [ ] Implement strict parsing macros that distinguish invalid from missing input.
- [ ] Build clean models and quarantine projections without dropping source lineage.
- [ ] Reconcile every raw fixture row to clean or quarantine.
- [ ] Run `dbt build --select clean` against local Postgres.
- [ ] Commit as `feat: conform and quarantine FMCSA events`.

### Task 5: Incremental event version history

**Files:**
- Create: `dbt/models/intermediate/event_change_candidates.sql`
- Create: `dbt/models/modeled/event_versions.sql`
- Create: `dbt/models/modeled/schema.yml`
- Create: `dbt/macros/materializations/event_version_history.sql`
- Create: `dbt/macros/postgres/event_version_merge.sql`
- Create: `tests/test_idempotency.py`

**Interfaces:**
- Produces: contracted `modeled.event_versions` with bigint `event_version_key`.
- Consumes: one `batch_id` passed through dbt vars.
- Guarantees: serialized close-and-insert transaction, deterministic replay, explicit tombstones, and retention-expiry classification.

- [ ] Write fixture assertions for new records, unchanged replay, correction, confirmed crash deletion, and inspection retention expiry.
- [ ] Run `pytest tests/test_idempotency.py -v` and confirm it fails before the version materialization exists.
- [ ] Implement change-candidate classification using stable source keys and record hashes.
- [ ] Implement a Postgres transaction that inserts successors before closing predecessors and links both version keys.
- [ ] Reject previously unseen out-of-order batches with a rebuild instruction.
- [ ] Run the same complete batch twice and compare ordered event-version rows plus the current source-key set byte-for-byte.
- [ ] Run dbt contracts for `event_versions`.
- [ ] Commit as `feat: preserve correction-aware event versions`.

### Task 6: Version-aware events, crash incidents, and features

**Files:**
- Create: `dbt/models/modeled/inspections.sql`
- Create: `dbt/models/modeled/crashes.sql`
- Create: `dbt/models/modeled/crash_incidents.sql`
- Create: `dbt/models/modeled/events_union.sql`
- Create: `dbt/models/modeled/current_events.sql`
- Create: `dbt/models/modeled/training_features.sql`
- Modify: `dbt/models/modeled/schema.yml`
- Create: `dbt/tests/no_impossible_event_chronology.sql`
- Create: `dbt/tests/no_overlapping_event_versions.sql`
- Create: `dbt/tests/training_features_match_point_in_time_events.sql`
- Create: `tests/test_temporal_dbt.py`

**Interfaces:**
- Produces: contracted modeled relations named above.
- Consumes: `scoring_dates` dbt variable as an explicit ISO-date list.
- Guarantees: strict two-clock filtering and one carrier-level row per crash incident version.

- [ ] Extend fixtures with a late event satisfying `event_date < scoring_date <= reported_date` and a multi-vehicle crash incident.
- [ ] Write a failing test proving a deliberately one-clock query disagrees with the expected feature row.
- [ ] Run the temporal suite and verify the intended failure.
- [ ] Implement version-aware projections and incident aggregation.
- [ ] Implement six-month inspection and twenty-four-month crash features on the explicit scoring grid.
- [ ] Add all three singular tests with `temporal` tags and enforced error severity.
- [ ] Run `dbt test --select tag:temporal` and `dbt build` against local Postgres.
- [ ] Commit as `feat: enforce two-clock event features`.

### Task 7: SQS-driven Airflow orchestration and local stack

**Files:**
- Create: `orchestration/messages.py`
- Create: `orchestration/load_service.py`
- Create: `orchestration/dbt_runner.py`
- Create: `dags/load_events.py`
- Create: `dags/build_tables.py`
- Create: `dags/backfill.py`
- Create: `tests/test_messages.py`
- Create: `tests/test_load_service.py`
- Create: `tests/test_dags.py`
- Modify: `docker-compose.yml`
- Modify: `.env.example`

**Interfaces:**
- Produces: `parse_s3_manifest_message(body: str) -> tuple[ManifestReference, ...]`
- Produces: `load_manifest(reference: ManifestReference, dependencies: LoadDependencies) -> RawLoadResult`
- Produces: import-safe Airflow DAGs `load_events`, `build_tables`, and `backfill`.

- [ ] Write failing tests for S3 message parsing, test events, malformed messages, duplicate records, and URL-encoded keys.
- [ ] Write failing service tests proving acknowledgement happens only after a committed raw load.
- [ ] Run focused tests and confirm failures precede implementation.
- [ ] Implement pure message and load services with injected clients and runners.
- [ ] Implement deferrable SQS waiting, dynamic task mapping, dataset publication, and parameterized backfill DAGs as thin adapters.
- [ ] Extend Compose with Airflow components and local queue emulation while retaining MinIO and Postgres.
- [ ] Run DAG import tests and an offline queue-to-Postgres smoke test.
- [ ] Commit as `feat: orchestrate event loads from SQS`.

### Task 8: SQS, RDS, IAM, and teardown-safe Terraform

**Files:**
- Create: `infra/base/sqs.tf`
- Create: `infra/base/rds.tf`
- Create: `infra/base/network.tf`
- Modify: `infra/base/s3.tf`
- Modify: `infra/base/iam.tf`
- Modify: `infra/base/variables.tf`
- Modify: `infra/base/outputs.tf`
- Modify: `infra/base/terraform.tfvars.example`
- Create: `tests/test_terraform_contract.py`

**Interfaces:**
- Produces: manifest-only S3 notification to the arrival queue.
- Produces: encrypted queue/DLQ with five-receive redrive.
- Produces: encrypted single-AZ `db.t4g.micro` RDS Postgres with explicit developer CIDR access and sensitive connection outputs.

- [ ] Write failing static contract tests for queue encryption, redrive, source-scoped queue policy, manifest suffix filtering, RDS encryption, non-public defaults, and required tags.
- [ ] Run the contract tests and confirm failures precede resources.
- [ ] Implement queue, DLQ, queue policy, and S3 notification dependencies.
- [ ] Implement the smallest network and RDS configuration compatible with locally run Airflow, documenting the connectivity trade-off explicitly.
- [ ] Split IAM permissions by snapshot writer and event loader. Neither receives delete access to raw objects.
- [ ] Run Terraform format, validation, static contract tests, and a reviewed plan.
- [ ] Commit as `infra: provision Phase 1 queue and warehouse`.

### Task 9: End-to-end verification and phase evidence

**Files:**
- Create: `docs/phase-1-verification.md`
- Modify: `README.md`
- Modify: `CONSTRAINTS.md` only in the local process checkout to record measured coverage. Do not commit it.

**Interfaces:**
- Produces: reproducible operator commands and sanitized evidence for the Phase 1 definition of done.

- [ ] Start the local stack from empty volumes and run the offline end-to-end fixture twice.
- [ ] Run the repository's fast, task, and full checks with the documented environment.
- [ ] Apply AWS Phase 1 infrastructure and confirm the budget before starting RDS.
- [ ] Land a current snapshot, observe its SQS event, and load it through Airflow into RDS.
- [ ] Run a parameterized real backfill twice and compare event-version rows and current source-key sets.
- [ ] Execute an as-of query on both sides of a correction boundary and record sanitized results.
- [ ] Deliberately cause one retryable task failure and record the successful retry without weakening a test.
- [ ] Destroy Phase 1 infrastructure and inventory RDS and SQS to prove no billable resources survive.
- [ ] Document exact row counts, test output, coverage, limitations, and teardown evidence.
- [ ] Commit as `docs: record Phase 1 verification evidence`.

## Final review checklist

- [ ] Compare every Phase 1 requirement in `CARRIER_RISK_SPEC.md` with a delivered file or recorded runtime result.
- [ ] Confirm the three temporal tests fail against the intentionally broken fixture/query and pass against production SQL.
- [ ] Confirm every new public class and function explains guarantees and failure behavior.
- [ ] Confirm modules remain cohesive and external boundaries use dependency injection.
- [ ] Search the full diff for suppression comments, disabled/warn-only dbt tests, unfinished stubs, credentials, and raw data.
- [ ] Run the requesting-code-review workflow over the entire Phase 1 diff.
- [ ] Run fresh full verification after review fixes.
