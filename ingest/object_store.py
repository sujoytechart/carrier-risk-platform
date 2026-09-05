"""Streaming read boundary for snapshot objects committed by a manifest."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from types_boto3_s3 import S3Client

from ingest.models import SnapshotManifest

HASH_BUFFER_BYTES = 1024 * 1024


class SnapshotObjectStore(Protocol):
    """Open complete snapshot bytes after verifying manifest evidence."""

    def open_snapshot(
        self, manifest: SnapshotManifest
    ) -> AbstractContextManager[BinaryIO]:
        """Yield a readable binary stream or reject inconsistent evidence."""


class FileSnapshotObjectStore:
    """Read verified snapshot objects beneath a configured filesystem root."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    @contextmanager
    def open_snapshot(self, manifest: SnapshotManifest) -> Iterator[BinaryIO]:
        """Verify and stream a local object without permitting path traversal."""
        object_path = (self._root / manifest.object_key).resolve()
        if not object_path.is_relative_to(self._root):
            raise ValueError(
                "Snapshot object path resolves outside its configured root"
            )
        if object_path.stat().st_size != manifest.compressed_bytes:
            raise ValueError("Snapshot object size does not match its manifest")
        if _file_sha256(object_path) != manifest.object_sha256:
            raise ValueError("Snapshot object checksum does not match its manifest")

        with object_path.open("rb") as snapshot:
            yield snapshot


class S3SnapshotObjectStore:
    """Read snapshot objects from S3 after checking committed object metadata."""

    def __init__(self, bucket: str, client: S3Client) -> None:
        self._bucket = bucket
        self._client = client

    @contextmanager
    def open_snapshot(self, manifest: SnapshotManifest) -> Iterator[BinaryIO]:
        """Verify S3 metadata before yielding the object's streaming body."""
        metadata = self._client.head_object(
            Bucket=self._bucket,
            Key=manifest.object_key,
        )
        if metadata["ContentLength"] != manifest.compressed_bytes:
            raise ValueError("Snapshot object size does not match its manifest")
        object_sha256 = metadata.get("Metadata", {}).get("object-sha256")
        if object_sha256 != manifest.object_sha256:
            raise ValueError(
                "Snapshot object checksum metadata does not match manifest"
            )

        response = self._client.get_object(
            Bucket=self._bucket,
            Key=manifest.object_key,
        )
        body = cast(BinaryIO, response["Body"])
        try:
            yield body
        finally:
            body.close()


def _file_sha256(path: Path) -> str:
    """Hash a local object in bounded memory for manifest verification."""
    digest = hashlib.sha256()
    with path.open("rb") as snapshot:
        while chunk := snapshot.read(HASH_BUFFER_BYTES):
            digest.update(chunk)
    return digest.hexdigest()
