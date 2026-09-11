# Athena raw snapshot operator guide

This directory contains the two queries used to reconcile and describe one
complete immutable crash snapshot and one complete immutable inspection snapshot
in place. The catalog publisher validates the original gzip objects and creates
small indexes for Athena; it never rewrites, normalizes, or copies the raw CSV.

Read the [source schema notes](../../docs/source-schemas.md) for the verified
column contracts and the [adapter differences](../../docs/adapter-differences.md)
for the current portability boundary. AWS resource enablement and cost controls
follow the [infrastructure operator guide](../../docs/phase-2-infrastructure.md).

## Select and publish snapshots

Choose one completed UTC acquisition partition per feed. Each partition must
already contain both of these immutable landing objects:

```text
raw/feed=<feed>/acquisition_date=<YYYY-MM-DD>/snapshot.csv.gz
raw/feed=<feed>/acquisition_date=<YYYY-MM-DD>/manifest.json
```

The Python API uses the credentials and region already available through the
standard AWS SDK configuration. Supply the deployed raw bucket through the
environment; do not put bucket names, account identifiers, role identifiers, or
credentials in source files.

```python
import os

import boto3

from ingest.athena_catalog import RawCatalogPublisher, S3CatalogStore
from ingest.object_store import S3SnapshotObjectStore

raw_bucket = os.environ["CARRIER_RISK_RAW_BUCKET"]
s3_client = boto3.client("s3")
publisher = RawCatalogPublisher(
    bucket=raw_bucket,
    catalog_store=S3CatalogStore(raw_bucket, s3_client),
    snapshot_store=S3SnapshotObjectStore(raw_bucket, s3_client),
)

selected_snapshots = {
    "crashes": "<CRASH_ACQUISITION_DATE: YYYY-MM-DD>",
    "inspections": "<INSPECTION_ACQUISITION_DATE: YYYY-MM-DD>",
}

for feed, acquisition_date in selected_snapshots.items():
    publication = publisher.publish(feed, acquisition_date)
    print(publication)
```

Grant `s3:GetObject` for the selected `raw/` snapshot and manifest and for
idempotency checks under `catalog/` and `catalog-metadata/`. Include
`s3:GetObjectVersion` for versioned raw snapshots. Conditional `s3:PutObject`
permission belongs only on `catalog/*` and `catalog-metadata/*`. The publisher
needs no delete permission and no permission to write or replace `raw/*`.
`S3SnapshotObjectStore` binds its streamed GET to the version returned by HEAD
when versioning metadata is present.

Publication reads and decompresses the entire snapshot to verify compressed and
uncompressed byte counts, both checksums, header order, row widths, and row
count. This is a deliberate network and CPU preflight, including on an
idempotent retry; inspect the selected manifest size and allow the full stream to
finish. The operation holds one CSV record at a time rather than loading the
snapshot into memory.

Successful publication creates:

```text
catalog/feed=<feed>/acquisition_date=<YYYY-MM-DD>/snapshot.txt
catalog-metadata/feed=<feed>/acquisition_date=<YYYY-MM-DD>/manifest.jsonl
```

The symlink file contains exactly one S3 URI ending in `snapshot.csv.gz`, which
keeps the adjacent `manifest.json` out of the CSV table. Compact metadata keeps
`batch_id`, `feed`, `observed_at`, `content_sha256`, `object_sha256`,
`row_count`, and `schema_fingerprint`. Metadata is written first and the
Athena-visible pointer last. Repeating the same publication returns an
idempotent result; conflicting existing content fails. If a run stops after
metadata creation, rerun the same feed and date to validate the full object again
and finish the pointer without replacing either object.

## CSV compatibility gate

The downloader accepts general CSV, while Athena's `OpenCSVSerde` supports a
narrower dialect. Publication stops before writing catalog objects when a record
contains any of these constructs:

- a NUL character, because the catalog configures NUL as OpenCSV's escape marker
  so ordinary backslashes remain literal;
- CR, LF, or CRLF embedded inside a quoted field;
- a quote inside unquoted text; or
- characters after a closing quote before the next delimiter.

A UTF-8 BOM, quoted commas, and doubled quotes remain valid. Treat a rejection as
evidence that the selected raw source is not compatible with the configured
Athena reader. Do not remove the row, change its bytes, or label a transformed
copy as raw-in-place analysis. Stop and make an explicit source-format or catalog
design decision.

## Catalog table contract

The Glue tables use lowercase column names while preserving the exact positional
order of the uppercase source headers:

| Table | Data | Partition columns |
|---|---|---|
| `raw_crashes` | 59 string columns from the crash schema | `acquisition_date` string |
| `raw_inspections` | 63 string columns from the inspection schema | `acquisition_date` string |
| `raw_snapshot_metadata` | compact JSONL lineage fields | `feed`, `acquisition_date` strings |

The two raw tables use `SymlinkTextInputFormat`, `OpenCSVSerde`, gzip detection,
and one skipped header line. Their locations are the feed-specific `catalog/`
prefixes, so each partition resolves only the exact URI in `snapshot.txt`.
Metadata partitions resolve the corresponding `catalog-metadata/` directories.

## Run reconciliation before lag analysis

Both SQL files contain `1970-01-01` sentinels in a `selected_snapshots` CTE.
Replace both feed dates in both files with the same crash and inspection
partitions published above. Select exactly one initial complete snapshot per feed;
combining daily snapshots would count repeated appearances as independent source
rows.

Execute the queries in the deployment's configured catalog database and enforced
Athena workgroup. Preserve its result-location, encryption, and scan controls.

Run [`validate_raw.sql`](validate_raw.sql) first. Require one metadata row per
feed and a zero difference between `manifest_row_count` and
`catalog_row_count`. Review the reported missing and invalid event dates,
missing and invalid source-add timestamps, excluded rows, negative lags, and
valid lag sample size before interpreting a distribution. Record Athena's
scanned bytes and query cost with the result.

Only after reconciliation, run [`report_lag.sql`](report_lag.sql) with the same
two dates. The query reports source-row percentiles and fixed histogram buckets.
For historical rows it defines reported time conservatively as `ADD_DATE + 1
day` for crashes and `MCMIS_ADD_DATE + 1 day` for inspections, labels the result
`availability_quality = 'source_proxy'`, and keeps manifest `observed_at`
separate. It does not use `CHANGE_DATE`, deduplicate crash incidents, or calculate
the later incident-level label-maturity watermark.

## Verification status

The publisher and queries have local fixture and contract coverage, including
failure paths and idempotent retry behavior. No live Athena execution or
real-feed OpenCSV compatibility proof has been recorded yet. The project status
and remaining live boundaries are tracked in the [main README](../../README.md#status).
