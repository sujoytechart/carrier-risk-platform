# Carrier risk serving

`GET /score/{usdot_number}` returns a probability for the positive six-month crash
label, the resolved MLflow model version, `features_as_of`, and a UTC
`computed_at`. The service loads the `champion` alias once at startup. Restarting
adopts a later promoted version. It never substitutes a fixture for a missing
production model.

| HTTP | `status` | Meaning |
|---|---|---|
| 200 | `scored` | Current eligibility and a current-month feature snapshot are available |
| 404 | `insufficient_history` | Invalid USDOT or no currently visible inspection in the preceding six calendar months |
| 503 | `features_unavailable` | An eligible carrier has no usable current-month feature row |
| 503 | `model_unavailable` | The promoted classifier is absent, incompatible, or returned an invalid probability distribution |
| 503 | `warehouse_unavailable` | The feature lookup could not complete |

Failure responses contain no risk score. ASCII positive integral USDOT numbers
are normalized by removing whitespace and leading zeros, with a PostgreSQL bigint
upper bound. Decimal, signed, exponent, and zero identifiers are ineligible.

PostgreSQL supplies the latest `modeled.training_features` row whose
`scoring_date` is on or before the UTC request date. The service requires that row
to belong to the current calendar month. An independent lookup in
`modeled.inspections` checks current eligibility using the event date, reported
date, knowledge interval, deletion flag, and six-calendar-month lower bound in
one statement. An inspection that ages out after a monthly build cannot keep a
carrier eligible. Events reported on the request date itself are excluded.

The classifier must expose the frozen seven features and binary classes `[0, 1]`.
Its MLflow model-version `feature_names` tag must contain the exact ordered,
comma-separated `ml.features.FEATURE_NAMES`. Undefined OOS rates use the same zero
encoding as training. Every score includes `validation_fixture` provenance.

## Start locally

The default app, `serving.app`, serves the scheduled training workflow.
`carrier-risk-v0` is its model registry name. Scoring requires a promoted model.

Install `.[serving]`, supply the warehouse connection and MLflow tracking URI, and
run `uvicorn serving.app:app --host 127.0.0.1 --port 8000`. Keep passwords in the
process environment or a private libpq password file. The container entry point
is [the API Dockerfile](../docker/api.Dockerfile).

| Environment variable | Default |
|---|---|
| `CARRIER_RISK_DATABASE_URL` | Required for warehouse availability |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` |
| `CARRIER_RISK_MODEL_NAME` | `carrier-risk-v0` |
| `CARRIER_RISK_MODEL_ALIAS` | `champion` |
| `CARRIER_RISK_VALIDATION_PROFILE` | `production` |

`/health/live` reports process liveness. `/health/ready` reports whether startup
loaded a warehouse connection and promoted classifier. Each score lookup still
handles subsequent warehouse failures. `/metrics` exposes bounded outcome
counters, a scoring-handler duration histogram, and model availability. USDOTs
are never metric labels. Client latency is measured separately by the load test.

## Run the retrospective experiment

The experiment runs in a separate app, `serving.demo_app`, with a
`/demo/score/{identifier}` endpoint. It loads `carrier-risk-learning-demo` using
the `demo` alias and marks responses as experimental. The default app rejects
experimental models. Follow the [experiment guide](../docs/learning-demo.md)
to prepare its data and start that app explicitly.

## Reproduce synthetic validation

Scheduled training can remain blocked by its data eligibility or promotion
checks. The explicit synthetic profile exists to verify the successful serving
path independently. It uses 512 fictional feature rows and the same frozen
gradient-boosting parameters as scheduled training. It does not establish real
predictive quality or production-scale warehouse latency.

Point `CARRIER_RISK_DATABASE_URL` at a **loopback** PostgreSQL database named
`carrier_risk_phase3_serving`, and supply its password through `PGPASSWORD`.
Fixture setup refuses other database names and remote hosts. It creates the
dedicated database when absent and upserts its fixture tables on replay.

```bash
python -m serving.benchmark_fixture --output-dir /tmp/carrier-risk-serving-fixture
export MLFLOW_TRACKING_URI=sqlite:////tmp/carrier-risk-serving-fixture/fixture-mlflow.db
export CARRIER_RISK_MODEL_NAME=carrier-risk-fixture
export CARRIER_RISK_VALIDATION_PROFILE=synthetic
uvicorn serving.app:app --host 127.0.0.1 --port 8000 --no-access-log
```

The fixture registry uses only `carrier-risk-fixture`, and each version carries
`validation_fixture=true` plus the frozen feature-order tag. The production
profile rejects this name and tag. The synthetic profile requires both. Models
are fully fitted, registered, resolved, and loaded through MLflow before the
PostgreSQL/HTTP path is tested.

With the API running, install `.[dev]` and run:

```bash
python -m serving.benchmark --host http://127.0.0.1:8000 \
  --output-dir /tmp/carrier-risk-loadtest --seconds 30 --warmup 5
```

The harness uses Locust's real HTTP client, request events, and latency
histograms. It schedules independent arrivals at 50, 100, 200, and 300 rps rather
than waiting for a response before scheduling another request. Each stage warms
up separately, resets statistics, and drains every scheduled request. Responses
must contain a valid synthetic score for the requested carrier, model version,
and current feature month. Error responses are failures.

`summary.json` reports the achieved rate including drain time, completed and
failed counts, Locust p50/p95/p99, scheduler delay, and p99 from the scheduled
arrival to response. Per-stage CSVs retain unrounded durations, request start
timestamps, and bounded failure details, preserving original transport exception
types. The harness hash and platform are recorded. A stage only passes the
latency gate with every scheduled request completed, zero failures, HTTP p99 and
scheduled-arrival p99 at most 120 ms, and maximum scheduler lag at most 120 ms.
The project acceptance target applies to the 200-rps stage.

For a separate coverage smoke test, `--rates 1 --seconds 10 --warmup 1` exercises
the same real path with ten measured requests. This smoke result is not the
published throughput benchmark. Keep all diagnostic and final runs separately.
Label concurrent host workloads and fixture size when reporting a result.
