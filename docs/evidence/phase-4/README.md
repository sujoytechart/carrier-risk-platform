# Phase 4 evidence

See the [verification report](../../phase-4-verification.md) for the result,
scope, reproduction instructions and limitations.

| Artifact | What it establishes |
|---|---|
| [Chart provenance](chart-provenance.json) and [renderer](render_charts.py) | Input hashes, numerical reconciliation, chart regeneration and output hashes |
| [Chart explanation](chart-provenance.md) | Source-proxy row distribution and local synthetic benchmark scope |
| [Before repair](before-repair.json) | A mapped loader in retry state with its queue message still in flight |
| [After first backfill](after-first-backfill.json) | Counts, modeled-table hashes and an empty queue after recovery |
| [Recovery and replay](recovery-and-replay.json) | The loader's third-attempt success and identical backfill outputs |
| [Original task-log excerpts](task-log-excerpts.json) | Actual failure/recovery events and hashes of the original local JSON logs |
| [Quality checks](quality-checks.json) | Full-suite and focused results, initial coverage failure, final coverage and source/log fingerprints |
| [Local cleanup](cleanup.json) | Removal of the Phase 4 services, closed loopback ports and screenshot hashes |
| [Retry state](airflow-up-for-retry.png), [error](airflow-missing-object.png), [recovery](airflow-recovered.png), [backfills](airflow-backfills.png) | Unedited screenshots of the actual local Airflow UI |

The local four-row fixture establishes behavior, not federal-data model quality
or production scale. The previous cloud proof remains in the Phase 1/2 reports.
Screenshots display local time (UTC−04:00); machine records use UTC. Receipt
handles, credentials, host paths, full trace frames and raw federal records are
absent from the published task excerpts. File hashes identify the original
logs; those full local logs are not published.
