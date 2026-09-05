from __future__ import annotations

import hashlib
import io
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.object_store import FileSnapshotObjectStore, S3SnapshotObjectStore


class FakeS3Client:
    """Serve one object with the metadata shape returned by S3."""

    def __init__(self, payload: bytes, object_sha256: str) -> None:
        self.payload = payload
        self.object_sha256 = object_sha256

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket, Key
        return {
            "ContentLength": len(self.payload),
            "Metadata": {"object-sha256": self.object_sha256},
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket, Key
        return {"Body": io.BytesIO(self.payload)}


def _manifest(payload: bytes) -> SnapshotManifest:
    feed = FEEDS["crashes"]
    location = SnapshotLocation.for_daily_snapshot(
        feed,
        datetime(2026, 9, 3, 12, tzinfo=UTC),
    )
    return SnapshotManifest.from_download(
        feed,
        location,
        DownloadedSnapshot(
            row_count=1,
            uncompressed_bytes=10,
            compressed_bytes=len(payload),
            content_sha256="1" * 64,
            object_sha256=hashlib.sha256(payload).hexdigest(),
            schema_fingerprint="2" * 64,
            columns=("CRASH_ID",),
        ),
    )


def test_file_store_streams_a_verified_snapshot(tmp_path: Path) -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    object_path = tmp_path / manifest.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(payload)

    store = FileSnapshotObjectStore(tmp_path)

    with store.open_snapshot(manifest) as snapshot:
        assert snapshot.read(10) == b"compressed"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("compressed_bytes", 1, "size"),
        ("object_sha256", "0" * 64, "checksum"),
    ],
)
def test_file_store_rejects_object_evidence_mismatch(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    object_path = tmp_path / manifest.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(payload)
    change: dict[str, object] = {field: value}
    if field == "object_sha256":
        change["batch_id"] = ""

    with (
        pytest.raises(ValueError, match=message),
        FileSnapshotObjectStore(tmp_path).open_snapshot(replace(manifest, **change)),
    ):
        pass


def test_file_store_rejects_a_path_outside_its_root(tmp_path: Path) -> None:
    manifest = replace(
        _manifest(b"snapshot"),
        object_key="../snapshot.csv.gz",
        batch_id="",
    )

    with (
        pytest.raises(ValueError, match="outside"),
        FileSnapshotObjectStore(tmp_path).open_snapshot(manifest),
    ):
        pass


def test_s3_store_streams_an_object_with_matching_metadata() -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    client = FakeS3Client(payload, manifest.object_sha256)

    with S3SnapshotObjectStore("raw-bucket", client).open_snapshot(
        manifest
    ) as snapshot:
        assert snapshot.read() == payload


@pytest.mark.parametrize(
    ("payload", "checksum", "message"),
    [
        (b"short", hashlib.sha256(b"short").hexdigest(), "size"),
        (b"compressed snapshot", "0" * 64, "checksum"),
    ],
)
def test_s3_store_rejects_metadata_mismatch(
    payload: bytes, checksum: str, message: str
) -> None:
    manifest = _manifest(b"compressed snapshot")

    with (
        pytest.raises(ValueError, match=message),
        S3SnapshotObjectStore(
            "raw-bucket", FakeS3Client(payload, checksum)
        ).open_snapshot(manifest),
    ):
        pass
