from __future__ import annotations

import hashlib
import io
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from types_boto3_s3 import S3Client

from ingest.models import (
    DownloadedSnapshot,
    FeedDefinition,
    SnapshotLocation,
    SnapshotManifest,
)
from ingest.storage import S3SnapshotStore


class FakeS3Client:
    """In-memory test double for the three S3 operations used by the adapter."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        del Bucket
        if Key not in self.objects:
            raise _missing_key("GetObject")
        body, _ = self.objects[Key]
        return {"Body": io.BytesIO(body)}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        del Bucket
        if Key not in self.objects:
            raise _missing_key("HeadObject")
        _, metadata = self.objects[Key]
        return {"Metadata": metadata}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes | io.BufferedReader,
        ContentType: str,
        ContentEncoding: str | None = None,
        Metadata: dict[str, str] | None = None,
        IfNoneMatch: str | None = None,
    ) -> None:
        del Bucket, ContentType, ContentEncoding
        if IfNoneMatch == "*" and Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        payload = Body if isinstance(Body, bytes) else Body.read()
        self.objects[Key] = (payload, Metadata or {})


def _missing_key(operation: str) -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey"}}, operation)


def _snapshot() -> tuple[SnapshotLocation, SnapshotManifest]:
    feed = FeedDefinition("crashes", "aayw-vxb3")
    location = SnapshotLocation.for_daily_snapshot(
        feed, datetime(2026, 9, 2, 12, tzinfo=UTC)
    )
    manifest = SnapshotManifest.from_download(
        feed,
        location,
        DownloadedSnapshot(
            row_count=1,
            uncompressed_bytes=10,
            compressed_bytes=8,
            content_sha256=hashlib.sha256(b"content").hexdigest(),
            object_sha256=hashlib.sha256(b"snapshot").hexdigest(),
            schema_fingerprint=hashlib.sha256(b'["crash_id","dot_number"]').hexdigest(),
            columns=("crash_id", "dot_number"),
        ),
    )
    return location, manifest


def test_store_writes_object_metadata_and_completed_manifest(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = S3SnapshotStore("raw-bucket", cast(S3Client, client))
    location, manifest = _snapshot()
    archive = tmp_path / "snapshot.csv.gz"
    archive.write_bytes(b"snapshot")

    store.store_snapshot_object(location, archive, manifest)
    store.publish_manifest(location, manifest)

    stored_body, stored_metadata = client.objects[location.object_key]
    assert stored_body == b"snapshot"
    assert stored_metadata["row-count"] == "1"
    assert store.read_completed_manifest(location) == manifest


def test_store_accepts_matching_object_from_an_interrupted_run(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = S3SnapshotStore("raw-bucket", cast(S3Client, client))
    location, manifest = _snapshot()
    client.objects[location.object_key] = (
        b"existing",
        {"object-sha256": manifest.object_sha256},
    )

    store.store_snapshot_object(location, tmp_path / "unused", manifest)

    assert client.objects[location.object_key][0] == b"existing"


def test_store_refuses_to_replace_a_different_object(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = S3SnapshotStore("raw-bucket", cast(S3Client, client))
    location, manifest = _snapshot()
    client.objects[location.object_key] = (
        b"different",
        {"object-sha256": hashlib.sha256(b"different").hexdigest()},
    )

    with pytest.raises(RuntimeError, match="Refusing to replace"):
        store.store_snapshot_object(location, tmp_path / "unused", manifest)


def test_missing_manifest_is_not_a_completed_acquisition() -> None:
    store = S3SnapshotStore("raw-bucket", cast(S3Client, FakeS3Client()))
    location, _ = _snapshot()

    assert store.read_completed_manifest(location) is None


def test_concurrent_manifest_publish_returns_the_first_commit() -> None:
    client = FakeS3Client()
    store = S3SnapshotStore("raw-bucket", cast(S3Client, client))
    location, first_manifest = _snapshot()
    client.objects[location.manifest_key] = (first_manifest.to_json(), {})
    later_observation = replace(
        first_manifest,
        observed_at="2026-09-02T13:00:00+00:00",
        batch_id="",
    )

    committed = store.publish_manifest(location, later_observation)

    assert committed == first_manifest


def test_concurrent_manifest_publish_rejects_different_snapshot() -> None:
    client = FakeS3Client()
    store = S3SnapshotStore("raw-bucket", cast(S3Client, client))
    location, first_manifest = _snapshot()
    client.objects[location.manifest_key] = (first_manifest.to_json(), {})
    different_snapshot = replace(
        first_manifest,
        object_sha256=hashlib.sha256(b"different").hexdigest(),
        batch_id="",
    )

    with pytest.raises(RuntimeError, match="different daily manifest"):
        store.publish_manifest(location, different_snapshot)
