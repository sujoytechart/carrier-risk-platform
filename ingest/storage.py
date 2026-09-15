"""Persistence boundary for immutable snapshots and their commit manifests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from botocore.exceptions import ClientError

from ingest.models import SnapshotLocation, SnapshotManifest

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


class SnapshotStore(Protocol):
    """Storage operations required by the daily landing workflow."""

    def read_completed_manifest(
        self, location: SnapshotLocation
    ) -> SnapshotManifest | None:
        """Return the manifest when the daily acquisition is already complete."""

    def store_snapshot_object(
        self, location: SnapshotLocation, archive: Path, manifest: SnapshotManifest
    ) -> None:
        """Store or verify the immutable data object without replacing it."""

    def publish_manifest(
        self, location: SnapshotLocation, manifest: SnapshotManifest
    ) -> SnapshotManifest:
        """Publish and return the canonical commit marker for the acquisition."""


def _is_missing_object(error: ClientError) -> bool:
    """Identify only S3's expected object-not-found responses."""
    return error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}


def _is_write_conflict(error: ClientError) -> bool:
    """Identify a conditional write lost to a concurrent acquisition."""
    return error.response.get("Error", {}).get("Code") in {
        "412",
        "PreconditionFailed",
    }


class S3SnapshotStore:
    """S3 implementation of the snapshot storage boundary.

    A manifest is the commit marker for a daily acquisition. The data object is
    written first and the manifest last. If execution stops between those writes,
    a rerun verifies the existing object's checksum before completing the manifest.
    """

    def __init__(self, bucket: str, client: S3Client) -> None:
        """Bind the adapter to one bucket and an already-configured S3 client."""
        self._bucket = bucket
        self._client = client

    def read_completed_manifest(
        self, location: SnapshotLocation
    ) -> SnapshotManifest | None:
        """Read a completed manifest, or return ``None`` when it does not exist."""
        try:
            response = self._client.get_object(
                Bucket=self._bucket, Key=location.manifest_key
            )
        except ClientError as error:
            if _is_missing_object(error):
                return None
            raise
        return SnapshotManifest.from_json(response["Body"].read())

    def store_snapshot_object(
        self, location: SnapshotLocation, archive: Path, manifest: SnapshotManifest
    ) -> None:
        """Write a new object or verify an interrupted run wrote identical bytes."""
        existing_checksum = self._existing_object_checksum(location)
        if existing_checksum is not None:
            self._require_matching_checksum(
                location, existing_checksum, manifest.object_sha256
            )
            return

        try:
            with archive.open("rb") as body:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=location.object_key,
                    Body=body,
                    ContentType="text/csv",
                    ContentEncoding="gzip",
                    Metadata={
                        "observed-at": manifest.observed_at,
                        "content-sha256": manifest.content_sha256,
                        "object-sha256": manifest.object_sha256,
                        "row-count": str(manifest.row_count),
                        "source-url": manifest.source_url,
                        "schema-fingerprint": manifest.schema_fingerprint,
                    },
                    IfNoneMatch="*",
                )
        except ClientError as error:
            if not _is_write_conflict(error):
                raise
            self._require_matching_checksum(
                location,
                self._existing_object_checksum(location),
                manifest.object_sha256,
            )

    def publish_manifest(
        self, location: SnapshotLocation, manifest: SnapshotManifest
    ) -> SnapshotManifest:
        """Write the commit marker once, even under concurrent acquisition runs."""
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=location.manifest_key,
                Body=manifest.to_json(),
                ContentType="application/json",
                IfNoneMatch="*",
            )
            return manifest
        except ClientError as error:
            if not _is_write_conflict(error):
                raise

        committed = self.read_completed_manifest(location)
        if committed is None or committed.object_sha256 != manifest.object_sha256:
            raise RuntimeError(
                "A concurrent acquisition published a different daily manifest"
            )
        return committed

    def _existing_object_checksum(self, location: SnapshotLocation) -> str | None:
        """Return the stored object checksum without downloading the snapshot."""
        try:
            response = self._client.head_object(
                Bucket=self._bucket, Key=location.object_key
            )
        except ClientError as error:
            if _is_missing_object(error):
                return None
            raise
        return response.get("Metadata", {}).get("object-sha256")

    def _require_matching_checksum(
        self,
        location: SnapshotLocation,
        existing_checksum: str | None,
        expected_checksum: str,
    ) -> None:
        """Reject any attempt to change bytes assigned to an immutable daily key."""
        if existing_checksum == expected_checksum:
            return
        object_uri = f"s3://{self._bucket}/{location.object_key}"
        raise RuntimeError(
            f"Refusing to replace existing immutable object {object_uri}"
        )
