# Phase 2 verification

**Phase 2 is incomplete.** The temporary AWS stack was created and live checks
covered Athena parser behavior, ECR artifact operations, remote state, and native
lock contention. Real-feed Athena reconciliation is blocked by embedded newlines;
Snowflake guard and one-row dbt seed proofs have run, but full dbt execution remains outstanding;
a setup/suspension screenshot is saved. See the
[infrastructure guide](phase-2-infrastructure.md), [adapter differences](adapter-differences.md),
and [evidence index](evidence/phase-2/README.md).

## Live acceptance and retained-snapshot preflight

On 2026-09-11, two synthetic Athena queries matched all 2,440 expected field values
across 20 rows per feed and scanned 1,462 bytes. The catalog uses an actual NUL
escape character, preserving literal backslashes; source NULs remain rejected.

The first bounded, read-only preflight rejected a backslash at crash source row
172. After the live parser proof and validator correction, a second preflight
verified the complete crash object's compressed SHA-256 and stopped at an
embedded newline at source row 100,978. Complete CSV validation and uncompressed
hash/row-count verification did not finish. Neither run downloaded the inspection
body. Each made eight S3 requests and transferred 564,301,248 body bytes. Both
removed their local downloads and performed no cloud writes. Their conservative
incremental estimates were $0.0518 and $0.0519 before credits, not finalized bills.
The retained snapshots are unchanged. A derived format requires an explicit
scope decision and must not be represented as raw-in-place analysis.

ECR acceptance uploaded and downloaded 108.361 MiB, verified every artifact hash,
and confirmed immutable-tag replacement rejection using the scoped publisher
role. This exercised registry APIs, not a Docker runtime deployment. The BASIC
scan completed with **2 critical, 11 high, and 2 medium findings** in the temporary
acceptance image; this is not a clean image-security result.

Remote-state migration preserved resource values but Terraform 1.16.0 initially
reset the lineage and serial when writing into the empty S3 backend. After secure
backups and exact resource/output comparisons, a locked state push restored the
original lineage; this was a manual recovery, not an automatic migration pass.
A paused, output-only apply held the native S3 lock: a competing plan failed with
HTTP 412 and lock metadata. The apply was declined; the next plan passed with no
changes after normal release. No compute or database runtime was provisioned.

All 36 temporary managed resources were destroyed after migrating the base state
back to local storage with its lineage and resource values preserved. Cleanup
removed 12 application objects and 14 remote-state versions/delete markers;
exact-name live inventories verified the temporary resources absent. Secure state
backups remain private. The original raw stack was excluded from cleanup. The
conservative AWS work estimate including cleanup is approximately $0.20, below the
user-authorized $10 ceiling; final billing must be checked in the management
account. Browser disconnection prevented AWS screenshot capture. A later Snowflake
setup session captured the suspended test warehouse; see the evidence index.
The test warehouse has an effective 0.25-credit immediate-suspension threshold,
60-second idle/query limits, disabled auto-resume, and a September 13, 2026
15:00 UTC end timestamp. Local browser authentication failed because the account
was not configured for SAML. The approved two-day service login now authenticates
with a key pair and has only the dedicated database and warehouse grants.
No real source data has been loaded and no full project dbt builds have run on Snowflake. Its remaining
trial balance and dollar billing are not yet verified; the UI showed approximately
0.12 account credits of usage with delayed cost data.

### Snowflake writer-guard experiments

A bounded synthetic probe passed five checks in 13.716 seconds: a separate process
could not acquire the held table lock, an independent worker connection could
perform DDL without releasing that lock, rollback restored the fixture, and a
subsequent writer could acquire the lock after normal release. These are prototype
results, not verification of a production dbt guard.

The connection-loss probe failed its assumption that closing the owner connection
would remove the transaction lock. The lock remained visible after disconnection.
The probe cancelled its known worker query and suspended the warehouse. Explicitly
aborting that exact orphan transaction subsequently succeeded; a readback showed
its lock gone. This does not establish safe automatic crash recovery: recovery
must stop or reconcile all work from the previous run before allowing a new writer.
Both probes verified the warehouse suspended during cleanup. Private reports retain
the exact query and transaction identifiers; they are excluded from this repository.

The revised guard commits a run claim before taking the long transaction lock.
An initial prototype crash test had a pipe-lifetime defect and is excluded from
the valid results. Its corrected run verified actual killed-process exit codes.
The same crash scenario then passed against the repository implementation in
19.581 seconds: the old worker remained active after launcher death and guard
abortion, a separate process rejected the retained claim, the exact worker query
reached terminal state after cancellation, and a new writer was admitted only
after explicit recovery. No automatic force-unlock path was added.

Implementation checks passed normal claim release, failed-worker retention,
lost-guard detection, and a successful next run after explicit recovery in
22.797 seconds. Earlier attempts exposed the string return type of
`CURRENT_TRANSACTION()` and the server's rejection of `SHOW LOCKS` inside an
aborted transaction. Both failure paths retained ownership and suspended compute;
the corrected implementation reports recovery required when verification fails.

A separate real dbt acceptance project used the repository launcher and startup
macro with a single synthetic seed row and two adapter threads. Direct mutation
was rejected, the locked seed materialized the expected row, successful completion
cleared the claim, and a stale transaction identifier was rejected. All four checks
passed in 33.790 seconds. This does not establish full project model, history,
contract, replay, or temporal-test parity. The warehouse was suspended and writer
locks were absent after each successful proof. A console screenshot records the
stopped state following the dbt seed and final killed-process checks.

These runs bring the conservative cumulative project estimate to approximately
$0.85 before credits, including allowances for failed attempts. This is not a
finalized bill or a verified remaining trial balance. The $10 testing ceiling
and September 13 shutdown deadline are unchanged.

## Regression baseline and current run

The original Phase 1 source and dependencies were rerun against a disposable local
PostgreSQL database before measuring the new implementation.

| Measurement | Reproduced Phase 1 baseline |
|---|---:|
| Passing pytest tests | 133 |
| Runtime | 1513.30 seconds |
| Combined statement-and-branch coverage | 88.09963099630997% |

The project percentage must remain at or above this exact baseline. Changed
executable Python lines in `ingest` and `orchestration` must have at least 80%
coverage. A rounded display of 88% is insufficient to establish that comparison.

**Current guard checkpoint: 226 tests passed in 1555.27 seconds.** A separate
61-test supplement passed in 50.82 seconds and includes two CLI checks added
after collection of the full run. The counts overlap and must not be added.
Combining coverage from those runs gives 89.4878706199461% project coverage,
above the exact Phase 1 baseline. Changed executable Python lines have 298/319
coverage (93.417%), above the 80% minimum. Both coverage gates passed. Lint,
formatting, strict typing, and the floor guard also passed.

For comparison, the earlier full suite before the parser correction had
189 tests pass in 1478.02 seconds. Its combined
statement-and-branch coverage is 88.65454545454546%, above the exact baseline.
Changed executable Python lines have 209/225 coverage (92.889%), above the 80%
minimum. Both coverage gates passed. The local floor checks also passed.

The full pytest suite supplies the controlled PostgreSQL fixtures and dbt build
lock. Its integration tests execute model builds and enforced contracts, replay
complete snapshots for idempotency, and exercise all three temporal detectors
with deliberate violations. No additional standalone dbt build is needed to
duplicate those fixture-driven checks. Snowflake requires its own real warehouse
execution and transaction/concurrency proof.

## Completed scoped checks

These are local results recorded on 2026-09-11. Counts refer to their named scope;
they are not additive totals for the current regression suite.

The current local environment uses Python 3.12.14, dbt-core 1.12.4,
dbt-postgres 1.11.0, dbt-snowflake 1.12.0, pytest 9.1.1, and Airflow 3.3.1.
The reproduced baseline used the original Phase 1 dependencies.

| Scope | Command or check | Recorded result |
|---|---|---|
| Shared SQL | `pytest tests/test_dbt_portability_sql.py -q` | 25 passed; both adapter compilations plus independent encoding and exact-ratio arithmetic checks |
| Catalog publisher | `pytest tests/test_athena_catalog.py -q` | 31 passed after the NUL escape correction, including literal-backslash acceptance and NUL rejection |
| Remote-state/ECR configuration | Terraform format, validate, and mock tests for both roots | Both roots valid; base 7 mock runs and bootstrap 4 mock runs passed before the Glue/Athena addition |
| Remote-state/ECR Python contracts | `pytest tests/test_terraform_contract.py` in scoped selections | 11 passed before the Glue/Athena addition |
| Final infrastructure contracts | `pytest tests/test_terraform_contract.py -q` | 15 passed after the final Glue/Athena edits; includes format/validation, base 21-run and bootstrap 6-run mock suites, graph isolation, and IAM/cost contracts |
| Snowflake launcher supplement | Guard, startup-macro, CLI, runner, runtime-import, and SQL portability tests | 61 passed in 50.82 seconds; includes 35 guard/launcher checks and both adapter compilation checks |
| Current Python quality | Ruff lint/format and strict `mypy ingest orchestration` | Clean; 79 Python files formatted and 21 source files type-checked |
| CI configuration | actionlint 1.7.12 and Bash syntax checks | Passed; seven workflow shell bodies checked |
| CI coverage helper | Ruff, formatting, strict mypy, and synthetic Git/coverage cases | Passed; 20 behavioral cases, including threshold failures and missing metadata |

Compilation resolves Jinja, dependencies, and rendered contracts; it does not
execute Snowflake SQL. Catalog tests use synthetic objects and do not establish
that the selected full federal snapshots are compatible with OpenCSVSerde.
Terraform mock results do not establish AWS permissions, resource creation, state
migration, remote locking, or deletion. The 15-test infrastructure run covers the
final edits made after collection of the 189-test full suite; it does not claim
that those later cases were included in the earlier count.

## Security scan scope

- Gitleaks 8.30.1 found no secrets in the 33-commit history at this local
  checkpoint and a 161-file publication snapshot.
- A later publication scan including the Snowflake guard and evidence found no
  secrets across 168 files. This is a working-tree publication scan, separate from
  the earlier history scan. A subsequent history scan found no secrets across
  37 commits before committing the guard increment.
- OSV-Scanner 2.5.1 reported no findings for 210 installed third-party package
  versions after the pytest upgrade; the installed environment's dependency check
  also passed. This describes that resolved environment at scan time.
- Checkov 3.3.17's final configuration scan reported 138 passes, 24 failures,
  zero skipped checks, and zero parsing errors across 50 resources. **Checkov is
  not clean.** A results-bucket finding prompted whole-bucket cleanup of abandoned
  multipart uploads. No suppression directives were added.

The remaining Checkov findings concern service-managed encryption instead of
customer-managed KMS keys; replication, access/query/flow logging and additional
monitoring; deliberately disposable single-AZ RDS without deletion protection;
RDS IAM authentication and snapshot tag copying; default VPC security-group
rules; retained state versions without automatic expiry; and results-bucket
versioning or bucket notifications. Runtime traffic uses a dedicated security
group. These choices and limitations remain visible in the scan; local contract
success does not certify production hardening or live cleanup.

## Reproducible CI and remaining acceptance

The [Checks workflow](../.github/workflows/checks.yml) installs Python 3.12 with
`.[dev,airflow]`, Terraform 1.16.0, and an ephemeral PostgreSQL 17 database. It runs
the full suite with JSON/LCOV branch coverage and the exact project/changed-line
gates. AWS credentials are removed; Snowflake compilation needs no credentials.
See [CI operator notes](../.github/README.md) for setup and comparison semantics.
**The hosted workflow has not run.** Local workflow validation does not establish
a successful GitHub run or configure merge protection.

Completion still requires:

- A complete Snowflake implementation and real builds, contracts, temporal
  failures, normalized business-result parity, replay, and business-transaction
  rollback. The guard's competing-writer and killed-process proofs are complete
  within the synthetic acceptance scope described above.
- Live Athena reconciliation of one selected complete snapshot per feed against
  its manifest, followed by labeled source-proxy lag analysis with scanned bytes
  and cost. The publisher must reject incompatible raw CSV before publication.
- The optional PostgreSQL/SQS spine acceptance and recovery evidence. The
  36-resource infrastructure-only stack was created, tested, and removed; its
  acceptance did not deploy that runtime spine.
- Sanitized captures of the actual acceptance states listed in the
  [evidence index](evidence/phase-2/README.md).
