"""Publish a tiny synthetic acceptance fixture to local MinIO and ElasticMQ.

Run explicitly with ``python -m tests.local_airflow_fixture``. Endpoints are
intentionally fixed to loopback so this helper cannot create AWS side effects.
The SQS message is an equivalent S3 notification, not proof of AWS delivery.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import tempfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote_plus

import boto3

from ingest.contracts import FeedSchema
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.storage import S3SnapshotStore

BUCKET = "carrier-risk-raw"
QUEUE_URL = "http://localhost:9324/000000000000/carrier-risk-arrivals"


def source_rows(feed_name: str) -> list[dict[str, str]]:
    """Return two inspection reports or two vehicles from one crash incident."""
    if feed_name == "inspections":
        return [
            {
                "INSPECTION_ID": "9001",
                "DOT_NUMBER": "123456",
                "INSP_DATE": "20260615",
                "MCMIS_ADD_DATE": "20260616 1200",
                "REPORT_STATE": "MI",
                "VIOL_TOTAL": "3",
                "OOS_TOTAL": "1",
            },
            {
                "INSPECTION_ID": "9002",
                "DOT_NUMBER": "123456",
                "INSP_DATE": "20260820",
                "MCMIS_ADD_DATE": "20260902 1200",
                "REPORT_STATE": "MI",
                "VIOL_TOTAL": "5",
                "OOS_TOTAL": "2",
            },
        ]
    incident = {
        "DOT_NUMBER": "123456",
        "REPORT_DATE": "20260701",
        "ADD_DATE": "20260702 1200",
        "REPORT_STATE": "MI",
        "REPORT_NUMBER": "SYNTHETIC-001",
        "REPORT_TIME": "1015",
        "INJURIES": "1",
        "TOW_AWAY": "Y",
        "FEDERAL_RECORDABLE": "Y",
    }
    return [
        incident | {"CRASH_ID": "9003", "REPORT_SEQ_NO": "1", "FATALITIES": "1"},
        incident | {"CRASH_ID": "9004", "REPORT_SEQ_NO": "2", "FATALITIES": "2"},
    ]


def main() -> None:
    """Land both immutable fixture files and publish their queue references."""
    session = boto3.Session(
        aws_access_key_id="minio",
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )
    s3 = session.client("s3", endpoint_url="http://localhost:9000")
    sqs = session.client("sqs", endpoint_url="http://localhost:9324")
    store = S3SnapshotStore(BUCKET, s3)
    for name, feed in FEEDS.items():
        schema = FeedSchema.load_configured(name)
        rows = source_rows(name)
        text = io.StringIO(newline="")
        writer = csv.DictWriter(text, fieldnames=schema.columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        payload = text.getvalue().encode()
        archive = gzip.compress(payload, mtime=0)
        location = SnapshotLocation.for_daily_snapshot(
            feed, datetime(2026, 9, 4, 12, tzinfo=UTC)
        )
        manifest = SnapshotManifest.from_download(
            feed,
            location,
            DownloadedSnapshot(
                row_count=len(rows),
                uncompressed_bytes=len(payload),
                compressed_bytes=len(archive),
                content_sha256=hashlib.sha256(payload).hexdigest(),
                object_sha256=hashlib.sha256(archive).hexdigest(),
                schema_fingerprint=schema.schema_fingerprint,
                columns=schema.columns,
            ),
        )
        manifest = replace(manifest, source_url=f"https://example.test/{name}.csv")
        with tempfile.TemporaryDirectory(prefix="carrier-risk-smoke-") as temporary:
            path = Path(temporary) / "snapshot.csv.gz"
            path.write_bytes(archive)
            store.store_snapshot_object(location, path, manifest)
            store.publish_manifest(location, manifest)
        message = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": BUCKET},
                        "object": {"key": quote_plus(location.manifest_key)},
                    },
                }
            ]
        }
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(message))
        print(
            f"{name}: published {len(rows)} synthetic rows; batch={manifest.batch_id}"
        )


if __name__ == "__main__":
    main()
