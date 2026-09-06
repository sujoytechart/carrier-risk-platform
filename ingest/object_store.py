"""Streaming read boundary for snapshot objects committed by a manifest."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Buffer, Iterator
from contextlib import AbstractContextManager, contextmanager
from io import RawIOBase
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast

from ingest.models import SnapshotManifest

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client

HASH_BUFFER_BYTES = 1024 * 1024


class _BinaryReadStream(Protocol):
    def read(self, size: int = -1) -> bytes:
        """Read at most size bytes, or all remaining bytes for a negative size."""


class ContentIntegrityStream(RawIOBase):
    """Measure bytes as they are read and verify the complete stream on demand."""

    def __init__(
        self,
        source: _BinaryReadStream,
        *,
        expected_bytes: int,
        expected_sha256: str,
        content_name: str,
    ) -> None:
        self._source = source
        self._expected_bytes = expected_bytes
        self._expected_sha256 = expected_sha256
        self._content_name = content_name
        self._bytes_read = 0
        self._digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        """Read from the source while updating byte-count and checksum evidence."""
        if self.closed:
            raise ValueError("I/O operation on closed integrity stream")
        chunk = self._source.read(size)
        self._record(chunk)
        return chunk

    def readinto(self, buffer: Buffer, /) -> int:
        """Support buffered consumers without bypassing integrity measurements."""
        if self.closed:
            raise ValueError("I/O operation on closed integrity stream")
        destination = memoryview(buffer).cast("B")
        chunk = self._source.read(len(destination))
        destination[: len(chunk)] = chunk
        self._record(chunk)
        return len(chunk)

    def readable(self) -> bool:
        """Report that the integrity wrapper supports reads."""
        return True

    def verify_complete(self) -> None:
        """Drain unread bytes in bounded chunks, then verify their measurements."""
        while self.read(HASH_BUFFER_BYTES):
            pass
        if self._bytes_read != self._expected_bytes:
            raise ValueError(
                f"{self._content_name} byte count does not match its manifest: "
                f"expected={self._expected_bytes}, actual={self._bytes_read}"
            )
        if self._digest.hexdigest() != self._expected_sha256:
            raise ValueError(
                f"{self._content_name} checksum does not match its manifest"
            )

    def _record(self, chunk: bytes) -> None:
        self._bytes_read += len(chunk)
        self._digest.update(chunk)


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
        with object_path.open("rb") as snapshot:
            if os.fstat(snapshot.fileno()).st_size != manifest.compressed_bytes:
                raise ValueError("Snapshot object size does not match its manifest")
            if _stream_sha256(snapshot) != manifest.object_sha256:
                raise ValueError("Snapshot object checksum does not match its manifest")
            snapshot.seek(0)
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

        version_id = metadata.get("VersionId")
        if version_id is None:
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=manifest.object_key,
            )
        elif isinstance(version_id, str):
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=manifest.object_key,
                VersionId=version_id,
            )
        else:
            raise ValueError("Snapshot object version metadata must be a string")

        body = cast(BinaryIO, response["Body"])
        verified_body = ContentIntegrityStream(
            body,
            expected_bytes=manifest.compressed_bytes,
            expected_sha256=manifest.object_sha256,
            content_name="Snapshot object",
        )
        try:
            if version_id is not None and response.get("VersionId") != version_id:
                raise ValueError("Snapshot GET returned a different object version")
            yield cast(BinaryIO, verified_body)
            verified_body.verify_complete()
        finally:
            try:
                verified_body.close()
            finally:
                body.close()


def _stream_sha256(snapshot: BinaryIO) -> str:
    """Hash an open stream in bounded memory, leaving it positioned at EOF."""
    digest = hashlib.sha256()
    while chunk := snapshot.read(HASH_BUFFER_BYTES):
        digest.update(chunk)
    return digest.hexdigest()
