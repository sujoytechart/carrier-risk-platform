"""Publication makes validated Parquet visible only with matching lineage."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ingest.contracts import FeedSchema
from ingest.object_store import FileSnapshotObjectStore
from ingest.parquet_catalog import DerivedCatalogPublisher
from ingest.parquet_converter import ParquetSnapshotConverter
from tests.test_parquet_converter import _snapshot


class MemoryDerivedStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self.fail_data = False

    def read_object(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def file_sha256(self, key: str) -> str | None:
        content = self.objects.get(key)
        return None if content is None else hashlib.sha256(content).hexdigest()

    def write_object_if_absent(
        self, key: str, content: bytes, *, content_type: str
    ) -> bool:
        if key in self.objects:
            return False
        self.objects[key] = content
        self.writes.append(key)
        return True

    def write_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        if self.fail_data:
            raise OSError("upload interrupted")
        content = path.read_bytes()
        assert hashlib.sha256(content).hexdigest() == sha256
        return self.write_object_if_absent(
            key, content, content_type="application/octet-stream"
        )


def test_derived_commit_is_last_and_retry_does_not_duplicate(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [["\n"] + [""] * (len(schema.columns) - 1)])
    store = MemoryDerivedStore()
    publisher = DerivedCatalogPublisher(
        store, ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    )
    first = publisher.publish(manifest, tmp_path / "converted")
    count = len(store.writes)
    second = publisher.publish(manifest, tmp_path / "converted")
    assert first.created and not second.created
    assert len(store.writes) == count == 3
    assert store.writes[-1] == first.metadata_key
    assert "/data/" in first.parquet_key
    assert first.metadata_key not in first.parquet_key


def test_interrupted_upload_cannot_publish_metadata_and_retry_recovers(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [[""] * len(schema.columns)])
    store = MemoryDerivedStore()
    publisher = DerivedCatalogPublisher(
        store, ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    )
    store.fail_data = True
    with pytest.raises(OSError, match="interrupted"):
        publisher.publish(manifest, tmp_path / "converted")
    assert not any(key.startswith("derived-metadata/") for key in store.objects)
    store.fail_data = False
    assert publisher.publish(manifest, tmp_path / "converted").created


def test_conflicting_existing_data_is_not_overwritten_or_committed(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [[""] * len(schema.columns)])
    store = MemoryDerivedStore()
    key = "derived/v1/feed=crashes/acquisition_date=2026-09-11/data/snapshot.parquet"
    store.objects[key] = b"different"
    publisher = DerivedCatalogPublisher(
        store, ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    )
    with pytest.raises(FileExistsError, match="Conflicting"):
        publisher.publish(manifest, tmp_path / "converted")
    assert store.objects == {key: b"different"}


@pytest.mark.parametrize("kind", ["lineage", "metadata"])
def test_conflicting_documents_are_rejected_before_writes(
    tmp_path: Path, kind: str
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [[""] * len(schema.columns)])
    store = MemoryDerivedStore()
    key = (
        "derived/v1/feed=crashes/acquisition_date=2026-09-11/lineage.json"
        if kind == "lineage"
        else (
            "derived-metadata/v1/feed=crashes/"
            "acquisition_date=2026-09-11/manifest.jsonl"
        )
    )
    store.objects[key] = b"conflict"
    publisher = DerivedCatalogPublisher(
        store, ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    )
    with pytest.raises(FileExistsError, match="Conflicting"):
        publisher.publish(manifest, tmp_path / "converted")
    assert store.writes == []


def test_s3_file_boundary_checks_checksum_and_streams_existing_content(
    tmp_path: Path,
) -> None:
    import base64
    import io

    import boto3
    from botocore.response import StreamingBody
    from botocore.stub import ANY, Stubber

    from ingest.parquet_catalog import S3DerivedStore

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    store = S3DerivedStore("test-bucket", client)
    source = tmp_path / "snapshot.parquet"
    source.write_bytes(b"parquet-data")
    digest = hashlib.sha256(source.read_bytes()).digest()
    with Stubber(client) as stub:
        stub.add_client_error(
            "get_object",
            "NoSuchKey",
            http_status_code=404,
            expected_params={"Bucket": "test-bucket", "Key": "missing"},
        )
        assert store.file_sha256("missing") is None
        stub.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(b"parquet-data"), 12)},
            {"Bucket": "test-bucket", "Key": "existing"},
        )
        assert store.file_sha256("existing") == digest.hex()
        expected = {
            "Bucket": "test-bucket",
            "Key": "output",
            "Body": ANY,
            "ContentType": "application/vnd.apache.parquet",
            "IfNoneMatch": "*",
            "ChecksumSHA256": base64.b64encode(digest).decode(),
        }
        stub.add_response("put_object", {}, expected)
        assert store.write_file_if_absent("output", source, digest.hex())
        stub.add_client_error(
            "put_object",
            "PreconditionFailed",
            http_status_code=412,
            expected_params=expected,
        )
        assert not store.write_file_if_absent("output", source, digest.hex())
        stub.assert_no_pending_responses()


@pytest.mark.parametrize("operation", ["get_object", "put_object"])
def test_s3_unexpected_errors_are_not_treated_as_existing_or_missing(
    tmp_path: Path, operation: str
) -> None:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.stub import Stubber

    from ingest.parquet_catalog import S3DerivedStore

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    store = S3DerivedStore("test-bucket", client)
    source = tmp_path / "file"
    source.write_bytes(b"data")
    with Stubber(client) as stub:
        stub.add_client_error(operation, "AccessDenied", http_status_code=403)
        with pytest.raises(ClientError):
            if operation == "get_object":
                store.file_sha256("key")
            else:
                store.write_file_if_absent(
                    "key", source, hashlib.sha256(b"data").hexdigest()
                )


def test_single_upload_ceiling_rejects_oversized_files_before_network(
    tmp_path: Path,
) -> None:
    import boto3
    from botocore.stub import Stubber

    from ingest.parquet_catalog import S3DerivedStore

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    path = tmp_path / "oversized"
    with path.open("wb") as file:
        file.truncate(5_000_000_001)
    with Stubber(client), pytest.raises(ValueError, match="size limit"):
        S3DerivedStore("test-bucket", client).write_file_if_absent(
            "key", path, "0" * 64
        )
