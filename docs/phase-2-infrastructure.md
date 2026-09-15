# Phase 2 infrastructure and portability

Phase 2 adds remote Terraform state, ECR, validated Parquet analytics in Athena,
and a Snowflake transformation target. Live acceptance and disposable AWS cleanup
passed, with documented harness/state recovery and security limitations. See the
[verification record](phase-2-verification.md).

## Resource boundaries

| Component | Responsibility | Retention and cleanup |
|---|---|---|
| `infra/state-bootstrap` | Private, encrypted, versioned state bucket and narrowly scoped deployment access | Secured local bootstrap state; remote versions require explicit cleanup |
| `infra/base` | Raw storage, ECR, query catalog, and optional PostgreSQL/SQS spine | One application root with an S3 backend and native lockfiles |
| ECR publisher | Push and pull from one immutable-tag repository | Ten-image retention by default; application destroy removes all images |
| Catalog publisher | Validate complete raw objects, then publish exact-object pointers and JSON metadata | No raw overwrite or deletion; matching publication is idempotent |
| Athena | Reconcile and describe validated derivatives of selected raw snapshots | Enforced encrypted results and bounded query scans |
| Snowflake | Optional verification of shared transformation semantics | Full synthetic build/history parity verified; compute suspended; no production loader |

The retained original raw stack is separate from the disposable acceptance
stack. An acceptance teardown must identify and remove its own resources without
removing the original immutable federal observations.

## Remote state

Follow the [bootstrap guide](../infra/state-bootstrap/README.md) before the first
base initialization. It covers securing existing local state, applying the
bootstrap root, migrating state without changing lineage/resource addresses,
actual two-process lock contention, and final removal order.

The state key is fixed at `carrier-risk/base/terraform.tfstate`. Use a separate
backend bucket for a separate disposable stack; do not repurpose an existing
stack's state or create an alternate Terraform workspace. Real backend and
variable files, state, private keys, and plans are ignored by git.

Runtime identities cannot read Terraform state. The deployment identity can read
and write the exact state object and can delete only its lock object. It cannot
use those lock permissions to delete raw snapshots.

ECR images are disposable deployment artifacts. `terraform destroy` deletes every
image in this repository. The state bucket deliberately has `force_destroy=false`
so destroying it cannot silently discard recovery history.

## Raw queries

The [Athena operator guide](../analytics/athena/README.md) describes publication,
reader limitations, selected partition dates, and reconciliation before lag
analysis. Full CSV validation is required even for a matching retry. Measure the
compressed bytes and allow for S3 transfer and request charges before starting.

Each raw table reads symlink pointers to `snapshot.csv.gz`. A normal table over
the landing directory would also read the adjacent `manifest.json`; filtering
rows after scanning does not provide the required file isolation.

Run the manifest/count/parse reconciliation query before the descriptive lag
query. Use one initial complete snapshot per feed and keep `observed_at` separate
from the source-add timestamp plus one publication day. This analysis describes
source rows and does not establish the incident-level label-maturity watermark.

## Cost-controlled acceptance

A live session requires confirmed account identity, credit eligibility and sharing,
current budget headroom, and a reviewed resource plan. Credit balances and budget
alerts are not hard spending limits. Do not create another alarm or email path.

Measure selected S3 object sizes, query scan estimates, and compressed image layers
before setting the session budget. Check registry-level basic/enhanced scanning
before pushing an image; repository scan-on-push alone does not prove basic mode.
Count submitted queries, retries, downloaded bytes, and running time. Athena can
scan beyond a configured cutoff before cancellation takes effect.

The default query cutoff is 1 GiB. Configuration accepts a whole number of bytes
from 10 MiB through 10 GiB, but that range is not permission to increase the live
limit. Include both selected files and repeated scans from multiple aggregate
branches in the query estimate. Validate full CSV compatibility before creating
query resources so an unsupported file can stop acceptance early.

Create the optional `db.t4g.micro` spine only for the bounded acceptance window.
Capture configuration, replay/recovery, query results/scanned bytes, image/scan,
and lock evidence while the relevant state is live. Begin teardown early enough
to verify removal, including database backups/secrets, queues, network interfaces,
image manifests, S3 versions/delete markers, and unfinished multipart uploads.
Stopping RDS or waiting for object lifecycle expiration does not prove removal.

Keep original captures outside git. Crop safe panels or apply opaque redaction,
then inspect the saved pixels before publishing. Evidence must come from actual
console/terminal states; a reconstructed diagram or rendered log is not a live
acceptance screenshot.

## Local checks

The Terraform tests initialize each root with an isolated `TF_DATA_DIR` and
`-backend=false`, and remove AWS credentials before provider execution. Set
`TERRAFORM_PLUGIN_DIR` to an existing trusted provider cache for fully offline
initialization. Mock-provider success validates configuration contracts only.

The normal Python test suite exercises a disposable local PostgreSQL database and
credential-free compilation for both adapters. The [adapter guide](adapter-differences.md)
distinguishes compilation, encoding checks, warehouse execution, and transaction
proof. No local or mocked result substitutes for the remaining cloud acceptance.
