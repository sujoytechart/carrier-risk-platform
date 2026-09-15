"""Conditionally publish validated Parquet and a final Athena lineage marker."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from botocore.exceptions import ClientError

from ingest.athena_catalog import CatalogMetadata, CatalogStore, S3CatalogStore
from ingest.models import SnapshotManifest
from ingest.parquet_converter import CONVERTER_VERSION, ParquetSnapshotConverter


class DerivedStore(CatalogStore, Protocol):
    """Immutable file operations in addition to small lineage documents."""

    def file_sha256(self, key: str) -> str | None:
        """Hash all existing file bytes, or return None for a missing object."""

    def write_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        """Conditionally create one file with server-verified content integrity."""


class S3DerivedStore(S3CatalogStore):
    """Stream bounded single-PUT artifacts without buffering them in memory."""

    def file_sha256(self, key: str) -> str | None:
        """Read and hash existing bytes rather than trusting mutable metadata."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return None
            raise
        body = response["Body"]
        digest = hashlib.sha256()
        try:
            while chunk := body.read(1024 * 1024):
                digest.update(chunk)
        finally:
            body.close()
        return digest.hexdigest()

    def write_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        """Keep single-PUT publication bounded; S3 verifies SHA-256 at upload."""
        if path.stat().st_size > 5_000_000_000:
            raise ValueError("Derived file exceeds the single-upload size limit")
        with path.open("rb") as content:
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=content,
                    ContentType="application/vnd.apache.parquet",
                    IfNoneMatch="*",
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(sha256)).decode(),
                )
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") in {
                    "412",
                    "PreconditionFailed",
                }:
                    return False
                raise
        return True


@dataclass(frozen=True)
class DerivedPublication:
    """One immutable derived dataset and its Athena completion marker."""

    batch_id: str
    parquet_key: str
    metadata_key: str
    created: bool


class DerivedCatalogPublisher:
    """Publish verified local data with metadata last as the completion marker.

    Query consumers must reconcile against exactly one metadata row before using
    the dataset. A data-only interrupted upload is recoverable, not committed.
    Raw source objects are never written by this publisher.
    """

    def __init__(
        self, store: DerivedStore, converter: ParquetSnapshotConverter
    ) -> None:
        self._store = store
        self._converter = converter

    def publish(
        self, manifest: SnapshotManifest, local_directory: Path
    ) -> DerivedPublication:
        """Validate/reuse the local artifact and publish without overwriting.

        Conflicts fail before new writes when already visible; conditional writes
        also detect competing publishers. An exact retry completes partial work.
        """
        artifact = self._converter.convert(manifest, local_directory)
        acquisition_date = (
            datetime.fromisoformat(manifest.observed_at).astimezone(UTC).date()
        )
        partition = f"feed={manifest.feed_name}/acquisition_date={acquisition_date}"
        prefix = f"derived/v1/{partition}"
        parquet_key = f"{prefix}/data/snapshot.parquet"
        lineage_key = f"{prefix}/lineage.json"
        metadata_key = f"derived-metadata/v1/{partition}/manifest.jsonl"
        lineage = artifact.lineage_path.read_bytes()
        document = json.loads(lineage)
        metadata = json.loads(CatalogMetadata.from_manifest(manifest).to_jsonl())
        metadata.update(
            converter_version=CONVERTER_VERSION,
            parquet_sha256=artifact.parquet_sha256,
            values_sha256=document["reconciliation"]["source_value_sha256"],
        )
        metadata_bytes = (
            json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        for key, expected in ((lineage_key, lineage), (metadata_key, metadata_bytes)):
            existing = self._store.read_object(key)
            if existing is not None and existing != expected:
                raise FileExistsError(f"Conflicting derived document at {key}")
        existing_hash = self._store.file_sha256(parquet_key)
        if existing_hash is not None and existing_hash != artifact.parquet_sha256:
            raise FileExistsError(f"Conflicting derived data at {parquet_key}")
        created = self._publish_document(lineage_key, lineage)
        if self._store.write_file_if_absent(
            parquet_key, artifact.parquet_path, artifact.parquet_sha256
        ):
            created = True
        elif self._store.file_sha256(parquet_key) != artifact.parquet_sha256:
            raise FileExistsError(f"Conflicting derived data at {parquet_key}")
        created = self._publish_document(metadata_key, metadata_bytes) or created
        return DerivedPublication(manifest.batch_id, parquet_key, metadata_key, created)

    def _publish_document(self, key: str, content: bytes) -> bool:
        created = self._store.write_object_if_absent(
            key, content, content_type="application/json"
        )
        if not created and self._store.read_object(key) != content:
            raise FileExistsError(f"Conflicting derived document at {key}")
        return created
