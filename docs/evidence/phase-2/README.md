# Phase 2 evidence

**No Phase 2 screenshots have been saved.** This index records the evidence still
required; it does not claim that the listed operations have run. Current local
results and limitations are in the [verification record](../../phase-2-verification.md).

| Required capture | What it must demonstrate | Status |
|---|---|---|
| PostgreSQL/RDS and SQS recovery | The disposable spine is running; arrival, redrive, and replay/recovery work | Pending |
| Glue catalog | Actual schemas and selected acquisition-date partitions point only to validated raw objects | Pending |
| Athena reconciliation and lag query | Manifest/count agreement, parse/exclusion counts, result scope, scanned bytes, and query cost | Pending |
| ECR | Immutable-tag settings, the bounded acceptance image, push/pull behavior, and scan outcome | Pending |
| Remote Terraform state | Two real competing operations show lock contention and normal release | Pending |
| dbt targets and temporal failures | Real builds/contracts pass on both targets; each temporal detector rejects a deliberate violation | Pending |
| Final teardown | Empty disposable-stack state and live inventories show removal, including versions, images, queues, database, and bootstrap resources | Pending |

Each saved capture must show an actual console or terminal state, have a clear
caption explaining its scope, and link back to the relevant command/result in the
verification record. Crop safe panels or apply opaque redaction to credentials,
account/role identifiers, endpoints, and other sensitive metadata, then inspect
the final pixels. Original captures stay outside version control. Generated
images, diagrams, and reconstructed logs cannot serve as execution evidence.

The final teardown evidence must distinguish the disposable acceptance stack from
the separately retained original raw snapshots. It must not imply that the entire
AWS account is empty.
