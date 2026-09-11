# Phase 3 verification

The model and serving implementation preserves the committed maturity gate.
The real-data result is a skipped training run, not a trained risk model.
Successful scores and load tests use an isolated synthetic validation model.
The serving latency acceptance target is met by the final bounded-session local
curve. This completes Phase 3's local acceptance evidence.

## Empirical maturity

The full retained September 3 snapshot was scanned locally: 4,986,413 crash
source rows. The calculation selects first source versions before deduplicating
carrier incidents, omits vehicle sequence from incident identity, and applies
the existing federal-recordability rule. Source timestamps plus one publication
day are labeled `source_proxy`; the current snapshot does not reconstruct deleted
or superseded historical versions.

The latest twelve event-month cohorts mature under the committed month-end plus
nine-calendar-month rule are December 2024–November 2025, with 140,706 eligible
incidents. A deterministic 2,000-replicate empirical histogram bootstrap uses
linear p99.5 quantiles and the 95th percentile of replicate quantiles. This is
ordinary bootstrap sampling with replacement represented as multinomial counts.
The maximum upper bound is 494.87 days, yielding a 495-day grace. A second actual
run reproduced all twelve cohort estimates exactly; the evidence includes source
hashes, software version, seed, execution timestamps and elapsed time.

Missing-carrier exclusions are 1,371,090 source rows and 1,344,754 distinct source
incident keys. The latter are not identifiable carrier incidents. The selected
cohorts also exclude seventeen invalid incident keys. No federal source rows or
carrier-level training rows are committed.

See [maturity evidence](evidence/phase-3/maturity-september-3-source-proxy.json).
The [local MLflow run](evidence/phase-3/training-gate.json) records the exact
watermark, skips before dataset construction or fitting, and leaves zero
registered models. No real-data average precision, baseline improvement or
production score is claimed.

## Implemented policy

Monthly feature rows retain both strict event/report clocks and knowledge-time
version filters. Complete source-coverage bounds must be attested explicitly;
the first event in a snapshot is not evidence of a complete historical window.
Training requires a six-month forward label plus the validated empirical grace.
The final three monthly dates form the holdout; the preceding six are purged,
and at least six earlier dates must remain for training. Empty population dates
remain in the grid so sparse rows cannot shorten the purge.

The fixed model uses 100 gradient-boosted trees, maximum depth three, learning
rate 0.1 and seed 20260911. Average precision must strictly beat the raw
24-month crash-count baseline and any incumbent evaluated on the identical
holdout. Ties preserve the incumbent. Candidate fitting is exercised only with
synthetic test data while the real-data maturity gate fails.

MLflow records model parameters, the dataset fingerprint, exact watermark,
availability-quality mix, exclusion counts, retained dates and evaluation
metrics. The champion alias changes only after passing the comparison. A
warehouse advisory lock serializes cooperating publishers; an alias recheck
also detects an operator change during evaluation. Model artifacts are trusted
operator-controlled inputs, not an untrusted public upload interface.

The Python-owned watermark registry has SQL constraints and immutable JSON
metadata, with one current version. It is outside dbt's event-model graph.
Incomplete measurements retain the previous effective grace while blocking
training. A recovered lower measurement still requires explicit review. The
PostgreSQL regression follows a 90-day policy through an incomplete measurement,
an unreviewed 10-day recovery that retains 90 days, and an explicitly reviewed
10-day policy. It also rejects imports that erase or alter the retained floor.
Missing or rolled-back current pointers fail before publication, preserving the
existing registry rows.

The additive dbt temporal guard independently compares summary columns with the
immutable metadata. Changing the measured 495-day summary to 494 days produced
one failing row; restoring 495 returned zero. A grace exceeding nine months is
valid measurement evidence and is rejected by training eligibility, rather than
being erased to make the consistency test pass. See the
[negative and restored test results](evidence/phase-3/maturity-dbt-negative-test.json).
The original three temporal tests are unchanged.

Training artifacts are immutable local files keyed by the dataset fingerprint.
The API reads the shared feature table using PostgreSQL indexes. PostgreSQL
index configuration is conditional; no Snowflake compute was resumed for this
phase, and no new live Snowflake acceptance is claimed.

## Local operation

Install `.[dev,airflow]`, provide a local PostgreSQL connection through
`CARRIER_RISK_DATABASE_URL` and libpq credentials, and configure
`MLFLOW_TRACKING_URI`. Existing aggregate evidence can exercise the real skip:

```bash
python -m ml.monthly train \
  --watermark docs/evidence/phase-3/maturity-september-3-source-proxy.json
```

For future eligible measurements, `python -m ml.monthly measure --data-as-of
YYYY-MM-DD` recomputes from the warehouse. `build` and `train` also require
attested `--inspection-start` and `--crash-start` bounds (or their named
`CARRIER_RISK_*_START` environment variables). Use
`CARRIER_RISK_DBT_PROFILES_DIR` for the existing dbt profile directory.
The extraction row limit is a memory guard; exceeding it fails without silently
truncating the source. The two Airflow DAGs use the same adapters and policy.

The local Compose definition gives MLflow a separate PostgreSQL metadata
database: both MLflow and Airflow use `public.alembic_version`, so sharing their
database would collide during migration. MLflow artifacts use the separate
MinIO model bucket, and the API waits for the registry health check before its
startup alias lookup. Compose configuration and packaging were validated; these
new container images were not built or deployed in this acceptance run.

The [monthly replay](evidence/phase-3/monthly-skip-replay.json) persists the actual
watermark twice through the PostgreSQL/MLflow adapters and retains one registry
row, one current version, and no registered model. An eligible fictional carrier
against the default production profile returns HTTP 503 `model_unavailable`
through actual PostgreSQL and MLflow lookups; readiness is also 503. The
[response evidence](evidence/phase-3/production-model-unavailable.json) contains
no risk score.

## Measured serving latency

The accepted full frozen-model run used an uninstrumented Uvicorn process, the
actual MLflow-loaded 100-tree model, and PostgreSQL lookups over 512 fictional
carriers. Locust scheduled independent open arrivals at 50, 100, 200 and 300 rps
for thirty measured seconds each after five seconds of warmup. Thirty-two
persistent loopback sessions kept connections warm while allowing overlapping
requests; this avoids the benchmark's earlier 256 idle client connections.

| Target rps | Achieved rps | HTTP p99 ms | Scheduled-arrival p99 ms | Failures / completed |
|---:|---:|---:|---:|---:|
| 50 | 50.00 | 12 | 14.96 | 0 / 1,500 |
| 100 | 100.00 | 10 | 10.76 | 0 / 3,000 |
| 200 | 200.00 | 14 | 14.30 | 0 / 6,000 |
| 300 | 299.98 | 86 | 118.23 | 0 / 9,000 |

The required 200-rps stage completed every request with 14 ms HTTP p99, 14.30 ms
scheduled-arrival p99, and 78.85 ms maximum scheduler lag. Each acceptance
measure is within the committed 120-ms limit. The raw per-request timings,
summary, harness source, SHA-256 hashes, fixture provenance, and limitations are
in the [final latency manifest](evidence/phase-3/latency-session-32/manifest.json).
This is local synthetic-path evidence; it does not claim real-data predictive
quality or cloud production latency.

The earlier 256-session curve, diagnostic probes, and rejected two-worker trial
remain in the [latency investigation](evidence/phase-3/latency-investigation.md).
They are historical failed experiments, not acceptance evidence. The failed
256-session run held roughly 282 server file descriptors while 256 loopback
clients remained connected; the final bounded-session configuration corrects
that benchmark-induced connection pressure without changing model or service
semantics.

## Quality verification

The full repository suite passed 363 tests in 1,628.04 seconds, including the
original temporal failures/restorations, idempotency, contracts and offline
infrastructure checks. Review fixes made while that long run was active were
verified with fresh component runs: seventeen maturity tests, forty-four
dataset/monthly tests, twenty-one training/tracking/promotion tests, eighty-eight
serving tests, four independent dbt maturity-guard tests and five Compose
contract tests. These runs overlap the full suite; their counts are not added
to claim a larger single run.

Coverage uses the full run only for unchanged `ingest` and `orchestration`
modules, combined with fresh final measurements for each changed `ml` and
`serving` module. Stale line measurements from before review edits are excluded.
The serving harness additionally has a separate one-rps, ten-request coverage
smoke test, which is not latency acceptance evidence.

Final branch-enabled project coverage is **89.93576017130621%**, above the
unchanged 88.09963099630997% baseline. Changed executable lines are
**1,415 / 1,522 = 92.970%**, above the unchanged 80% threshold.
The [aggregate quality record](evidence/phase-3/quality.json) includes exact
counts, installed versions and the runtime source hash.

Ruff, formatting, strict mypy, the unchanged floor guard, Compose rendering,
workflow linting and wheel packaging passed. OSV found no vulnerabilities among
the 266 installed packages using its exact-version scan. This does not resolve
the inherited infrastructure and image findings below. Secret scanning covers
the public source/evidence snapshot, including decompressed timing files.

## Cost and remaining limits

All Phase 3 execution is local. No new paid cloud work was performed, no original
raw snapshots were removed, and the suspended Snowflake trial and scheduled
shutdown were untouched. The prior conservative $5.20 allowance remains an
allowance, not finalized billing. The total authorized ceiling is still $10,
with paid work stopping at $8 to retain cleanup headroom.

All temporary API processes were stopped. The owned Phase 3 PostgreSQL test
container and disposable volumes were removed, its absence was verified, and its
temporary credential files were deleted. Existing containers were untouched.
See the [local cleanup record](evidence/phase-3/local-cleanup.json).

The prior 24 Checkov hardening findings and critical/high findings in the earlier
disposable ECR image are not resolved by model and API tests. This is not a
production-readiness claim. No push, merge, hosted CI run or cloud deployment is
part of this local Phase 3 work.
