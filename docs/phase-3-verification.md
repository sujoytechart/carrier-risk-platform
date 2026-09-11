# Phase 3 verification

The model and serving implementation preserves the committed maturity gate.
The real-data result is a skipped training run, not a trained risk model.
Successful scores and load tests use an isolated synthetic validation model.
The serving latency acceptance target remains unmet; this report does not mark
Phase 3 acceptance complete.

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

The full frozen-model run used an uninstrumented Uvicorn process, the actual
MLflow-loaded 100-tree model, and PostgreSQL lookups over 512 fictional carriers.
Locust scheduled open arrivals at 50, 100, 200 and 300 rps for thirty measured
seconds each after five seconds of warmup. Repository test suites were stopped;
the eight-GiB desktop's other workloads were uncontrolled. Every stage and
failure is retained, with raw timings, hashes, model version and platform.

At the required 200-rps stage, all 6,000 request attempts completed, 516 failed,
achieved throughput including drain was 166.69 rps, and HTTP p99 was 5,600 ms.
Scheduled-arrival-to-response p99 was 6,098.17 ms, and maximum scheduler delay
was 866.97 ms. The committed 120-ms budget was not met. Failed request durations
clustered around the configured five-second timeout; the original harness lost
their underlying exception details. The diagnostic fix preserves transport
errors and per-request timestamps without changing timing or acceptance rules.

A subsequent bounded diagnostic used two seconds of warmup and ten measured
seconds at 200 rps. It completed 2,000 requests without failures, achieved
199.94 rps, and still failed the budget with HTTP p99 of 300 ms. All 2,400
handler durations including warmup were within 120 ms. This narrows the remaining
delay to work outside that measured handler, including queuing, transport and
the load client; it does not isolate a cause. CPU/memory samples show concurrent
host activity and paging, but do not prove those caused the earlier failures.

An additional instrumented ASGI probe matched every client response with server
timestamps. Its 2,000 measured requests succeeded, but client p99 was 385.04 ms
(400 ms in Locust's rounded histogram). ASGI entry to response start had p99
258.55 ms, while combined time outside ASGI had p99 144.53 ms. Of 208 responses
exceeding 120 ms, 136 spent most of their time inside ASGI. Final-body writes
took at most 0.801 ms. These measurements identify framework/request scheduling
as part of the tail, without attributing the earlier transport failures to it.
The first attempt lost its shutdown timing buffer; both attempts are retained,
and the recovered records agree with response timing headers.

The earlier concurrent diagnostic with a smaller 32-tree fixture is retained
separately and does not establish the frozen model's latency. The README
publishes the complete frozen-model curve; short instrumented probes are not
substituted for acceptance measurements.

The observed framework delay motivated one controlled two-worker configuration
trial with the same full curve, model and timing rules. All 19,500 requests
completed without failures, but the required 200-rps stage achieved 197.04 rps,
HTTP p99 200 ms, scheduled-arrival p99 462.78 ms and maximum scheduler delay
435.83 ms. The configuration was rejected and the single-worker default was
retained. System CPU reached 82.6%; the monitoring interval recorded 362.2 MB of
swap-ins and 22.7 MB of swap-outs. These concurrent observations do not establish
a unique cause. See the [complete investigation and attempt ledger](evidence/phase-3/latency-investigation.md).

Further acceptance needs a controlled host/load-generator environment and
request-scheduling investigation. The current results do not demonstrate the
200-rps budget, even though some other rate stages passed.

## Quality verification

The full repository suite passed 363 tests in 1,628.04 seconds, including the
original temporal failures/restorations, idempotency, contracts and offline
infrastructure checks. Review fixes made while that long run was active were
verified with fresh component runs: seventeen maturity tests, forty-four
dataset/monthly tests, twenty-one training/tracking/promotion tests, eighty-seven
serving tests, four independent dbt maturity-guard tests and five Compose
contract tests. These runs overlap the full suite; their counts are not added
to claim a larger single run.

Coverage uses the full run only for unchanged `ingest` and `orchestration`
modules, combined with fresh final measurements for each changed `ml` and
`serving` module. Stale line measurements from before review edits are excluded.
The serving harness additionally has a separate one-rps, ten-request coverage
smoke test, which is not latency acceptance evidence.

Final branch-enabled project coverage is **89.9330655957162%**, above the
unchanged 88.09963099630997% baseline. Changed executable lines are
**1,414 / 1,521 = 92.965%**, above the unchanged 80% threshold.
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

The prior 24 Checkov hardening findings and critical/high findings in the earlier
disposable ECR image are not resolved by model and API tests. This is not a
production-readiness claim. No push, merge, hosted CI run or cloud deployment is
part of this local Phase 3 work.
