from __future__ import annotations

import json
from dataclasses import dataclass, replace

import pytest

from ingest.models import SnapshotManifest
from ingest.raw_loader import RawLoadResult
from orchestration.load_service import (
    LoadDependencies,
    QueueMessage,
    process_queue_message,
)
from orchestration.messages import ManifestReference


def _manifest() -> SnapshotManifest:
    return SnapshotManifest(
        feed_name="crashes",
        dataset_id="aayw-vxb3",
        source_url="https://example.test/crashes.csv",
        observed_at="2026-09-04T12:00:00+00:00",
        object_key="raw/feed=crashes/acquisition_date=2026-09-04/snapshot.csv.gz",
        row_count=1,
        uncompressed_bytes=100,
        compressed_bytes=50,
        content_sha256="1" * 64,
        object_sha256="2" * 64,
        schema_fingerprint="3" * 64,
        columns=("CRASH_ID",),
    )


def _message_body() -> str:
    return json.dumps(
        {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": "raw-bucket"},
                        "object": {
                            "key": (
                                "raw%2Ffeed%3Dcrashes%2F"
                                "acquisition_date%3D2026-09-04%2Fmanifest.json"
                            )
                        },
                    },
                }
            ]
        }
    )


@dataclass
class StubManifestRepository:
    manifest: SnapshotManifest

    def fetch(self, reference: ManifestReference) -> SnapshotManifest:
        assert reference.bucket == "raw-bucket"
        return self.manifest


class RecordingLoader:
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.failure = failure
        self.loaded: list[SnapshotManifest] = []

    def load(self, manifest: SnapshotManifest) -> RawLoadResult:
        self.loaded.append(manifest)
        if self.failure is not None:
            raise self.failure
        return RawLoadResult(manifest.batch_id, manifest.row_count, False)


class RecordingAcknowledger:
    def __init__(self) -> None:
        self.receipts: list[str] = []

    def acknowledge(self, receipt_handle: str) -> None:
        self.receipts.append(receipt_handle)


def test_acknowledges_only_after_manifest_load_commits() -> None:
    manifest = _manifest()
    loader = RecordingLoader()
    acknowledger = RecordingAcknowledger()

    results = process_queue_message(
        QueueMessage(body=_message_body(), receipt_handle="receipt-1"),
        LoadDependencies(
            manifest_repository=StubManifestRepository(manifest),
            loader=loader,
            acknowledger=acknowledger,
        ),
    )

    assert results == (RawLoadResult(manifest.batch_id, 1, False),)
    assert loader.loaded == [manifest]
    assert acknowledger.receipts == ["receipt-1"]


def test_failed_load_leaves_message_unacknowledged() -> None:
    acknowledger = RecordingAcknowledger()
    dependencies = LoadDependencies(
        manifest_repository=StubManifestRepository(_manifest()),
        loader=RecordingLoader(failure=RuntimeError("database unavailable")),
        acknowledger=acknowledger,
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        process_queue_message(
            QueueMessage(body=_message_body(), receipt_handle="receipt-2"),
            dependencies,
        )

    assert acknowledger.receipts == []


def test_unexpected_bucket_is_rejected_before_remote_reads() -> None:
    acknowledger = RecordingAcknowledger()
    dependencies = LoadDependencies(
        manifest_repository=StubManifestRepository(_manifest()),
        loader=RecordingLoader(),
        acknowledger=acknowledger,
        expected_bucket="different-bucket",
    )

    with pytest.raises(ValueError, match="unexpected bucket"):
        process_queue_message(
            QueueMessage(body=_message_body(), receipt_handle="receipt-3"),
            dependencies,
        )

    assert acknowledger.receipts == []


def test_manifest_payload_must_belong_to_the_notified_partition() -> None:
    manifest = replace(
        _manifest(),
        object_key="raw/feed=crashes/acquisition_date=2026-09-05/snapshot.csv.gz",
        batch_id="",
    )
    loader = RecordingLoader()
    acknowledger = RecordingAcknowledger()
    dependencies = LoadDependencies(
        manifest_repository=StubManifestRepository(manifest),
        loader=loader,
        acknowledger=acknowledger,
    )

    with pytest.raises(ValueError, match="manifest partition"):
        process_queue_message(
            QueueMessage(body=_message_body(), receipt_handle="wrong-partition"),
            dependencies,
        )

    assert loader.loaded == []
    assert acknowledger.receipts == []
