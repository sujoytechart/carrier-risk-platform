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

    def __init__(
        self,
        payload: bytes,
        object_sha256: str,
        *,
        head_content_length: int | None = None,
        head_version_id: str | None = None,
        get_version_id: str | None = None,
    ) -> None:
        self.payload = payload
        self.object_sha256 = object_sha256
        self.head_content_length = head_content_length
        self.head_version_id = head_version_id
        self.get_version_id = get_version_id
        self.get_requests: list[dict[str, str]] = []
        self.body: io.BytesIO | None = None

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket, Key
        response: dict[str, object] = {
            "ContentLength": (
                len(self.payload)
                if self.head_content_length is None
                else self.head_content_length
            ),
            "Metadata": {"object-sha256": self.object_sha256},
        }
        if self.head_version_id is not None:
            response["VersionId"] = self.head_version_id
        return response

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        VersionId: str | None = None,
    ) -> dict[str, object]:
        request = {"Bucket": Bucket, "Key": Key}
        if VersionId is not None:
            request["VersionId"] = VersionId
        self.get_requests.append(request)
        self.body = io.BytesIO(self.payload)
        response: dict[str, object] = {"Body": self.body}
        if self.get_version_id is not None:
            response["VersionId"] = self.get_version_id
        return response


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


def test_file_store_consumes_the_same_open_descriptor_it_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    committed_payload = b"committed snapshot"
    replacement_payload = b"replacement snap!!"
    manifest = _manifest(committed_payload)
    object_path = tmp_path / manifest.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(committed_payload)
    replacement_path = object_path.with_name("replacement.csv.gz")
    replacement_path.write_bytes(replacement_payload)

    original_open = Path.open
    binary_open_count = 0

    def replace_before_second_binary_open(
        path: Path,
        mode: str = "r",
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal binary_open_count
        if path == object_path and mode == "rb":
            binary_open_count += 1
            if binary_open_count == 2:
                replacement_path.replace(object_path)
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", replace_before_second_binary_open)

    with FileSnapshotObjectStore(tmp_path).open_snapshot(manifest) as snapshot:
        loaded_payload = snapshot.read()

    assert loaded_payload == committed_payload
    assert binary_open_count == 1


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


def test_s3_store_rejects_changed_bytes_despite_retained_checksum_metadata() -> None:
    committed_payload = b"compressed snapshot"
    changed_payload = b"compressed snapshou"
    manifest = _manifest(committed_payload)
    client = FakeS3Client(changed_payload, manifest.object_sha256)

    with (
        pytest.raises(ValueError, match="checksum"),
        S3SnapshotObjectStore("raw-bucket", client).open_snapshot(manifest) as snapshot,
    ):
        assert snapshot.read() == changed_payload

    assert client.body is not None
    assert client.body.closed


def test_s3_store_drains_and_verifies_after_normal_partial_consumption() -> None:
    committed_payload = b"same prefix: committed"
    changed_payload = b"same prefix: corruptee"
    manifest = _manifest(committed_payload)
    client = FakeS3Client(changed_payload, manifest.object_sha256)

    with (
        pytest.raises(ValueError, match="checksum"),
        S3SnapshotObjectStore("raw-bucket", client).open_snapshot(manifest) as snapshot,
    ):
        assert snapshot.read(13) == b"same prefix: "


def test_s3_store_verifies_actual_bytes_when_head_size_is_incorrect() -> None:
    committed_payload = b"compressed snapshot"
    truncated_payload = committed_payload[:-1]
    manifest = _manifest(committed_payload)
    client = FakeS3Client(
        truncated_payload,
        manifest.object_sha256,
        head_content_length=manifest.compressed_bytes,
    )

    with (
        pytest.raises(ValueError, match="byte count"),
        S3SnapshotObjectStore("raw-bucket", client).open_snapshot(manifest) as snapshot,
    ):
        snapshot.read()


def test_s3_store_binds_get_to_the_head_version() -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    client = FakeS3Client(
        payload,
        manifest.object_sha256,
        head_version_id="version-17",
        get_version_id="version-17",
    )

    with S3SnapshotObjectStore("raw-bucket", client).open_snapshot(
        manifest
    ) as snapshot:
        snapshot.read()

    assert client.get_requests == [
        {
            "Bucket": "raw-bucket",
            "Key": manifest.object_key,
            "VersionId": "version-17",
        }
    ]


def test_s3_store_rejects_a_different_returned_version() -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    client = FakeS3Client(
        payload,
        manifest.object_sha256,
        head_version_id="version-17",
        get_version_id="version-18",
    )

    with (
        pytest.raises(ValueError, match="version"),
        S3SnapshotObjectStore("raw-bucket", client).open_snapshot(manifest),
    ):
        pass

    assert client.body is not None
    assert client.body.closed


def test_s3_store_closes_body_when_consumer_raises() -> None:
    payload = b"compressed snapshot"
    manifest = _manifest(payload)
    client = FakeS3Client(payload, manifest.object_sha256)

    with (
        pytest.raises(RuntimeError, match="consumer failed"),
        S3SnapshotObjectStore("raw-bucket", client).open_snapshot(manifest) as snapshot,
    ):
        snapshot.read(1)
        raise RuntimeError("consumer failed")

    assert client.body is not None
    assert client.body.closed
