# Phase 2 verification

Phase 2 infrastructure and portability acceptance is complete as of September
11, 2026. The disposable AWS stacks were removed. The original raw snapshots
remain. Snowflake is suspended with auto-resume disabled and no running queries,
queued queries, or writer locks. Its small fixture database and expiring service
identity remain for the scheduled trial shutdown. This is not an account deletion.

## Full-source Athena acceptance

The approved Parquet derivation preserves the original gzip CSV objects and their
unchanged acquisition timestamps. Complete compressed/uncompressed hashes and
byte counts, headers, row widths, row counts, required-string schema, and hashes
of every source and Parquet value reconciled. Empty strings, leading zeros,
backslashes, Unicode and embedded newlines remain unchanged values.

| September 3 snapshot | Manifest rows | Athena rows | Delta |
|---|---:|---:|---:|
| Crashes | 4,986,413 | 4,986,413 | 0 |
| Inspections | 8,281,794 | 8,281,794 | 0 |

Exactly one metadata completion row was present per feed. Both feeds reported
zero missing/invalid event dates, missing/invalid source-add timestamps, negative
lags, and excluded rows. The retained-source download made nine S3 requests and
transferred 1,218,548,989 body bytes. Full local conversion/readback took 1894.864
seconds. This is local CPU work, not a cloud performance benchmark. Publication
revalidated both artifacts and used the scoped analytics role and conditional S3
writes with server-checked SHA-256.

The reconciliation query scanned 33,236,627 bytes. The subsequent lag query scanned
66,472,018 bytes. Together that is 99,708,645 bytes, approximately $0.0005 of Athena
scan usage at $5/TB before per-query rounding. A $0.01 allowance covers scan
rounding. Storage, requests and transfer are accounted separately.

| Source-row lag, days | Median | p90 | p95 | p99 | p99.5 |
|---|---:|---:|---:|---:|---:|
| Crashes | 39 | 429 | 897 | 1,891 | 2,193 |
| Inspections | 3 | 8 | 11 | 61 | 147 |

These are labeled **source-proxy, source-row** distributions using source-add time
plus one day. They are not the incident-level label-maturity watermark and do not
by themselves decide the later model-training gate. Full buckets and lineage are
in [Athena results](evidence/phase-2/athena-results.json) and
[Parquet reconciliation](evidence/phase-2/parquet-reconciliation.json).

Earlier raw-in-place attempts rejected backslashes and then embedded newlines.
The live CSV parser probe matched 2,440 synthetic field values, but the real source
still contained unsupported embedded newlines. The approved derived representation
resolved that incompatibility without deleting or normalizing source rows.

## Snowflake execution and semantics

The full fixture build passed **108 dbt nodes with zero errors and zero skips**.
A separate PostgreSQL fixture build produced the reference results. Exact replay
preserved all persisted history, applied-batch markers and retention records.
Normalized row multisets matched across all nine business relations. Surrogate
keys, adapter-specific record hashes and application timestamps were excluded.

All three temporal detectors rejected an actual injected data violation with
`status=fail` and a positive failure count: impossible chronology, overlapping
knowledge intervals, and incorrect point-in-time features. After restoration all
temporal tests passed. An extra persisted-history column was rejected before
history mutation. A two-pending-batch transaction rolled back every history,
lineage, retention and applied-marker change when the second batch was invalid.
A candidate/registry mismatch preserved the previous published pair. Final parent
history and all nine business relations were unchanged. See
[the semantic evidence](evidence/phase-2/snowflake-semantics.json).

The separate writer-guard probes passed competing-writer exclusion, independent
worker DDL, normal release, direct-command rejection and killed-process recovery.
Closing a connection did not reliably release the old transaction, so recovery
requires explicit terminal-worker checks, exact run ownership and reconciliation.
There is no automatic force-unlock path.

Acceptance corrections remain visible: the first full-build harness used an
invalid scoring date. A later replay was stopped by the initial 0.25-credit
monitor, and an isolated rollback fixture exceeded its hash-column width. Each
failed run suspended compute. Recovery verified exact ownership, terminal workers
and unchanged persisted rows before clearing its claim. The corrected rollback
fixtures use valid-width hashes. The final remaining-only proof passed in 52.341
seconds. These harness failures are not counted as successful tests.

The dedicated X-Small generation-1 warehouse retains 60-second idle/query limits,
auto-resume disabled, and a non-resetting one-credit monitor with a 75% immediate
suspension trigger. Its deadline remains September 13, 2026 at 15:00 UTC. Only
synthetic data was loaded into Snowflake. No production source loader is claimed.

## Infrastructure lifecycle

The earlier 36-resource acceptance stack proved ECR push/pull, immutable tags,
remote Terraform state and native S3 lock contention. Its bounded ECR round trip
verified 108.361 MiB of artifacts. A competing Terraform operation failed with
HTTP 412 while the first held the lock and succeeded after normal release.
Migration into an empty S3 backend reset lineage under Terraform 1.16.0. A
reviewed, locked state push restored it after resource/output comparisons. This
was a documented manual recovery, not an automatic migration pass.

That stack was destroyed and its application objects and state versions removed.
The later clean 35-resource derived-Athena stack was also created and destroyed.
Exact-name live checks verified its buckets, Glue catalog, IAM roles, ECR
repository and Athena workgroup absent, with an empty managed Terraform state.
Bucket versions and delete markers were removed. Original raw storage was
explicitly excluded and its continued existence verified. No RDS, EC2, NAT or
other continuously running AWS compute was created for this acceptance. The
optional runtime-spine redeployment was not part of the Phase 2 acceptance.
[Phase 1 verification](phase-1-verification.md) records that runtime's earlier proof.

## Local quality and security

The complete suite passed **260 tests in 1417.20 seconds**. Project statement and
branch coverage was **90.090090090%**, above the exact reproduced Phase 1 baseline
of **88.09963099630997%**. Changed executable Python lines were **520/555
(93.694%)**, above the 80% threshold. Ruff, formatting, strict typing and the floor
guard passed. Final infrastructure contracts passed 15 tests. Independent final
review found no actionable defects and passed 58 focused checks. These overlapping
counts must not be added to the full-suite count.

OSV found a PyArrow vulnerability in 21.0.0. The dependency floor was raised to
23.0.1, the scanner's fixed release. All 23 converter/publisher tests passed with
that release. The full-suite result above predates this dependency-only update.
The final OSV scan returned no findings. Gitleaks found no leaks in the
publication snapshot. No suppression or quality-threshold changes were introduced.

Checkov reports **138 passed, 24 failed, zero skipped, zero parsing errors** across
54 resources. It is not clean. Remaining findings concern customer-managed KMS,
replication/logging/monitoring, disposable single-AZ RDS protections and IAM auth,
default VPC rules, state retention, and result-bucket versioning/notifications.
The temporary ECR acceptance image scan had **2 critical, 11 high and 2 medium**
findings. Registry API success is not a clean image-security result or deployment.

The hosted GitHub workflow has not run and nothing was pushed. Local CI validation
does not establish a successful hosted run or branch protection. Production
hardening and model/serving work remain outside this completed phase.

## Cost and screenshots

The user-authorized ceiling remains $10 of gross testing usage, including credits.
A conservative $3.50 allowance covers all Snowflake testing. AWS operations,
transfer and cleanup remain within their $1.70 combined allocation. This is an
allowance, not a finalized bill. AWS credits were checked in
**sujoy-das-team-management**, showing $119.96 estimated remaining and two credits
expiring September 2, 2027. Delayed billing and organization-sharing coverage must
not be inferred from a workload-account budget. Snowflake trial balance is not
verified.

The [evidence index](evidence/phase-2/README.md) distinguishes native screenshots
from machine-readable execution records. Browser interruptions prevented a full
set of live query/teardown captures. No reconstructed screenshot is substituted.
