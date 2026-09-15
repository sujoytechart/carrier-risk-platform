# Real-data execution evidence

These screenshots were captured from the local services after the successful
manual Airflow run on September 11, 2026 (America/Detroit). The run completed
at 03:27 UTC on September 12. They show actual service screens. The results
chart is generated separately from aggregate measured outcomes.

| Proof | Artifact |
|---|---|
| Extraction, contracted feature build and training all succeeded on attempt 1 | [Airflow screenshot](airflow-run.png), [task execution record](airflow-run.json) |
| Held-out metrics and fixed parameters from the Airflow-trained estimator | [MLflow metrics screenshot](mlflow-metrics.png), [aggregate training evidence](training-evidence.json) |
| Separate registered model version 2, with the `demo` alias | [Registry screenshot](mlflow-registry.png) |
| HTTP 200 from the real version 2 model. Example identifier withheld | [API response screenshot](api-response.png) |
| Contract and boundary checks passed in the real feature build | [dbt result records](dbt-results.json) |
| Baseline comparison, precision, recall and confusion counts | [Generated results chart](model-results.png) |
| Failed 200 requests/second trial while other work was running | [Contended load result](api-load-contended-200rps.json) |
| All 6,000 requests scored after regression work completed. Latency target unmet | [Follow-up 200 requests/second result](api-load-followup-200rps.json) |
| Lighter 30 requests/second smoke: all 900 scored, p99 30.2 ms | [Lighter load result](api-load-30rps.json) |
| Full suite: 464 passed. Final serving/demo batch: 134 passed. Quality gates unchanged | [Local verification record](verification.json), [coverage gate output](coverage-gate.txt) |

The MLflow run is `507954c89ec0407088e56e252b9898d2`. Its dataset fingerprint is
`a9f38fb38210923a6871df4dab7b3848060814bd751e5bd4a7f093765ecd419b`.
An earlier direct run produced the same fingerprint, threshold and evaluation
metrics. Full source files, individual carrier records and service credentials
remain outside the repository.

The [experiment report](../../learning-demo.md) describes the retained-snapshot
target, historical limitations, reproduction commands and interpretation of
the measurements. Successful execution establishes an experimental end-to-end
path. It does not establish prospective accuracy or production latency.
