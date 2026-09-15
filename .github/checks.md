# Automated checks

`Checks` runs on pull requests, manual dispatches, and pushes to `main`,
`phase-*`, and `demo-maintainability`. It uses a GitHub-hosted Ubuntu runner
with a 60-minute timeout. It requires no repository
secrets. PostgreSQL 17 listens only on localhost; its password is generated and
masked during each job, and its container and volume are removed afterward.

The workflow runs redacted Gitleaks scanning over fetched Git history, Ruff lint
and formatting, strict mypy over `ingest`, `orchestration`, `ml` and `serving`,
and the full pytest suite with branch coverage. Python 3.12 installs the `dev`
and `airflow` extras.
Action revisions and the Gitleaks archive are pinned to verified upstream commits
and a SHA-256 digest, respectively.

The pytest database fixtures run dbt builds, enforced model contracts, idempotency
regressions, the three core temporal tests with deliberate violations, and the
label-maturity policy guard. A separate dbt build is unnecessary. Terraform
1.16.0 tests initialize with `-backend=false` and isolated `TF_DATA_DIR`
directories, validate both roots, and exercise mock-provider plans. Provider
downloads are cached using the committed
lockfiles. AWS credentials are removed and metadata credential lookup is disabled.
Snowflake SQL compilation is offline; successful CI does not prove Snowflake
execution or an AWS resource lifecycle.

The serving fixture CLI also runs against a dedicated temporary database and
local MLflow registry. Its integration test checks replay equality, synthetic
scoring, production-mode rejection, and cleanup.

`coverage_gate.py` enforces the exact project baseline of 88.09963099630997% and
at least 80% coverage of changed executable Python lines in `ingest`,
`orchestration`, `ml` and `serving`. It intersects added lines with coverage.py's
executed and missing lines, using the PR base SHA or push's previous SHA. Deleted
files are excluded; renamed destinations are conservatively treated as new files.
For an unavailable base or manual run, a distinct merge-base with `origin/main`
is used; otherwise all tracked source lines are checked. Missing coverage
metadata fails the gate.

Dependency versions are bounded in `pyproject.toml`, not frozen to a lockfile;
future resolver changes can therefore affect installation. OSV and Checkov reviews
are separate security evidence, not checks claimed by this workflow. A workflow
file does not configure branch protection: maintainers must require its successful
status in repository rules if merging is to be blocked automatically.
