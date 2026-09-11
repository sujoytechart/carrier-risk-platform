# Phase 2 verification

**Phase 2 is incomplete.** Local checks cover the catalog publisher, shared SQL
compilation, remote-state/ECR configuration, and CI configuration. No Phase 2
Snowflake execution, live Athena query, AWS apply, remote-lock contention, or
fresh-create/full-teardown acceptance has been completed. No Phase 2 screenshots
have been saved. See the [infrastructure guide](phase-2-infrastructure.md),
[adapter differences](adapter-differences.md), and [evidence index](evidence/phase-2/README.md).

## Retained-snapshot preflight

A bounded read-only S3 check on 2026-09-11 verified the selected manifests and
versioned Standard-storage object metadata for both feeds. It downloaded the
complete crash snapshot, verified its compressed SHA-256, then stopped when the
catalog CSV validator found a backslash at source row 172. The configured
OpenCSVSerde escape behavior makes that input unsafe to publish under the
current contract. Full CSV validation did not finish, and the inspection snapshot
body was not downloaded.

The check made eight S3 requests and transferred 564,301,248 body bytes, including
the small manifests. It completed in 13.392 seconds, performed no cloud writes,
and removed its temporary local download. The conservative incremental estimate
was $0.0518 before credit allowances, rather than a finalized bill. Athena
publication remains blocked until a supported parser configuration preserves
the raw values or the data-access approach is explicitly revised.

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

**Current full suite: 189 tests passed in 1478.02 seconds.** Combined
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
| Catalog publisher | `pytest tests/test_athena_catalog.py -q` | 27 passed in 0.88 seconds; separate branch-coverage run reported 91% for this module |
| Remote-state/ECR configuration | Terraform format, validate, and mock tests for both roots | Both roots valid; base 7 mock runs and bootstrap 4 mock runs passed before the Glue/Athena addition |
| Remote-state/ECR Python contracts | `pytest tests/test_terraform_contract.py` in scoped selections | 11 passed before the Glue/Athena addition |
| Final infrastructure contracts | `pytest tests/test_terraform_contract.py -q` | 15 passed after the final Glue/Athena edits; includes format/validation, base 21-run and bootstrap 4-run mock suites, graph isolation, and IAM/cost contracts |
| Current Python quality | Ruff lint/format and strict `mypy ingest orchestration` | Clean; 75 Python files formatted and 20 source files type-checked |
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
  failures, normalized business-result parity, replay, rollback, and competing
  writer/lost-parent recovery evidence.
- Live Athena reconciliation of one selected complete snapshot per feed against
  its manifest, followed by labeled source-proxy lag analysis with scanned bytes
  and cost. The publisher must reject incompatible raw CSV before publication.
- A disposable AWS stack created and removed using both Terraform roots, with
  preserved state migration, actual native-lock contention/release, ECR image
  operations/scan, and inventories proving removal of the stack's resources and
  retained object versions. The original raw snapshot stack remains separate.
- Sanitized captures of the actual acceptance states listed in the
  [evidence index](evidence/phase-2/README.md).
