# Retrospective experiment

This experiment proves that reconciled federal snapshots can flow through
PostgreSQL, contracted dbt features, Airflow, model training, MLflow registration
and a FastAPI estimate. Its separate registry name is
`carrier-risk-learning-demo`, with a `demo` alias and `experimental=true` tags.
This manual workflow uses separate dataset preparation and a separate serving
app. It shares the platform's ingestion, orchestration utilities and fixed model
parameters with scheduled training. Scheduled training keeps its data eligibility
and model promotion checks, and is currently skipped before fitting. The
experiment does not promote a model to the default API.

## Frozen experiment

| | Training | Later-period test |
|---|---|---|
| Scoring date | February 1, 2024 | September 1, 2024 |
| Inspection features | October 2023–January 2024 | May–August 2024 |
| Prior-crash features | Preceding 24 months | Preceding 24 months |
| Six-month outcome | February–July 2024 | September 2024–February 2025 |
| Eligible carriers | 203,560 | 233,291 |
| Recorded positive carriers | 22,835 | 25,764 |

The dates were selected from inspection coverage and non-overlapping outcome
windows before fitting, rather than from model performance. Six intermediate
monthly scoring dates are omitted. The estimator uses the original fixed
100-tree gradient boosting parameters and seed. The seven-feature demo contract
names inspection counts explicitly as `_4m`. A classification threshold of
0.239572 was selected by training F1 only, before evaluating test predictions.
There was no tuning against the test period.

The target means **at least one qualifying federal crash recorded in the retained
September 3, 2026 snapshot during the following six months**. A negative means no
such retained recorded crash. It does not guarantee no actual crash.

## Measured results

| Measurement | Result |
|---|---:|
| Average precision | 49.00% |
| Prior 24-month crash-count baseline average precision | 40.17% |
| Recorded crash-positive carriers detected (recall) | 45.77% |
| Positive predictions that were correct (precision) | 48.28% |
| Overall classification accuracy | 88.60% |
| Always-negative accuracy | 88.96% |

| Actual outcome | Predicted negative | Predicted positive |
|---|---:|---:|
| No qualifying retained recorded crash | 194,897 | 12,630 |
| Qualifying retained recorded crash | 13,972 | 11,792 |

Accuracy alone would conceal the class imbalance. The model's useful result is
its higher ranking precision than the prior-crash baseline and its detection of
11,792 recorded positive carriers. This is a rough retrospective estimate. It
does not establish calibrated probabilities or prospective predictive quality.

### Interpreting precision

The model flagged 24,422 carriers positive. Of those, 11,792 had a qualifying
recorded crash and 12,630 did not have one in the retained outcome data. This
gives **48.28% precision** at the threshold selected on training data.
Average precision is a separate ranking measure across thresholds. Its
**49.00%** result is compared with the prior-crash baseline's **40.17%**.

The project emphasizes the data pipeline and ML platform. A fixed 100-tree
gradient boosting model exercises feature generation, reproducible training,
registry publication and serving. There was no hyperparameter search.
The training cohort contains 203,560 carriers, but historical coverage and
feature depth are limited to two scoring cohorts and seven aggregate features.
The threshold was selected for training F1, which balances precision and recall.
It was not selected to meet a minimum precision target.

The limitations below may affect predictive performance. This experiment does
not isolate how much each one contributes to the observed precision. More
history, richer features or tuning would need a new held-out evaluation before
claiming an improvement.

## Limitations encountered

Four months of inspection features allow two usable historical cohorts without
shortening the six-month target. The retained files do not provide all historical
vintages, and the original maturity requirement still cannot be satisfied.
Labels are therefore explicitly snapshot-ascertained, without a guarantee of
99.5% eventual completeness. Older reporting-lag cohorts can exceed the measured
495-day recent-cohort bound.

Source-add time plus one publication day is a reporting-availability proxy.
Current retained values cannot reconstruct every past correction or deletion.
The public inspection download excludes currently inactive carriers, so these
historical cohorts are conditional on acquisition-time inclusion. There are
114,486 repeated carriers across training and test. This measures a later period,
not unseen carriers. Historical fleet exposure is unavailable.

The model was fitted retrospectively using outcomes retained in 2026. Training
labels were not necessarily mature before the September 2024 scoring date. This
is not a simulation of a model deployed then. More historical snapshots and
outcome records could expand training and validation coverage and may improve
the model, but any accuracy improvement must be measured.

## Reproduce locally

Install Python 3.12 and `.[dev,airflow]` (`.[charts]` also regenerates the results
chart). Start local PostgreSQL and create a dbt
profile as described in [the warehouse guide](../dbt/README.md). Keep credentials
in environment variables or private files. Never commit full source records.

```sh
export CARRIER_RISK_DATABASE_URL='host=localhost port=5432 dbname=carrier_risk user=carrier_risk'
export CARRIER_RISK_DBT_PROFILES_DIR=/absolute/path/to/private/profiles
export CARRIER_RISK_DEMO_DERIVED_ROOT=/absolute/path/to/reconciled/derived
export CARRIER_RISK_DEMO_OUTPUT_DIR=/absolute/path/to/private/demo-output
export MLFLOW_TRACKING_URI=sqlite:////absolute/path/to/private/demo-output/mlflow.db
python -m ml.demo_pipeline extract
python -m ml.demo_pipeline build
python -m ml.demo_pipeline train
```

The derived root contains `inspections/snapshot.parquet`,
`crashes/snapshot.parquet` and their `lineage.json` files. Extraction verifies
both SHA-256 fingerprints and source-value reconciliation. Selected source rows
land only in `learning_demo.source_events`. Canonical raw and modeled history
are untouched. Conflicting inspection identities fail dbt validation. Features
require event and proxy reporting dates strictly before scoring. Only historical
cohorts receive labels. The September 2026 scoring snapshot remains unlabeled.

Alternatively, trigger the manual-only `train_demo_model` Airflow DAG with the
same environment available to its workers. It extracts, builds/tests features,
then trains/registers. Concurrency locks serialize demo steps and dbt publication.
The training task has no automatic retries, making duplicate registration visible.

```sh
python -m uvicorn serving.demo_app:app --host 127.0.0.1 --port 8003
# /demo/score/{usdot}?scoring_date=2024-09-01 or the default 2026-09-01
# /readyz and /metrics expose demo availability and request behavior.
```

For screenshots, configure a real eligible carrier privately through
`CARRIER_RISK_DEMO_EXAMPLE_USDOT`. `GET /demo/example` scores that carrier using
the September 2026 snapshot and withholds its identifier in the returned example.
The service rejects incompatible provenance, feature order, estimator classes,
missing features and invalid probabilities. The production model loader rejects
experimental versions.

```sh
python analytics/demo_load_test.py --rate 200 --seconds 30 --output /private/path/api-load.json
```

This short local API smoke is separate from the production synthetic latency
acceptance sweep. A follow-up after the regression work completed scored all
6,000 requests at an offered 200 requests/second, achieved 185.1 requests/second
including queue drain, and measured p99 latency of 883.5 ms. It does **not** meet
the production 120 ms p99 target. The earlier contended trial had three client
errors and substantially higher latency. Its cause was not isolated. Both
[measured trials](evidence/learning-demo/README.md) are retained.
At a lighter offered 30 requests/second, all 900 requests scored successfully,
achieved throughput was 30.0 requests/second and p99 latency was 30.2 ms. This is
a separate short demo smoke, with no change to the production acceptance target.

[Aggregate training evidence](evidence/learning-demo/training-evidence.json)
and native UI screenshots retain the proof without individual source records.
