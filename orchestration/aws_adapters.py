"""AWS implementations of orchestration object and queue boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ingest.models import SnapshotManifest
from orchestration.messages import ManifestReference

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client
    from types_boto3_sqs import SQSClient


class S3ManifestRepository:
    """Retrieve snapshot manifests from their immutable S3 object versions."""

    def __init__(self, client: S3Client) -> None:
        self._client = client

    def fetch(self, reference: ManifestReference) -> SnapshotManifest:
        """Read and deserialize the exact manifest referenced by an event."""
        if reference.version_id is None:
            response = self._client.get_object(
                Bucket=reference.bucket,
                Key=reference.key,
            )
        else:
            response = self._client.get_object(
                Bucket=reference.bucket,
                Key=reference.key,
                VersionId=reference.version_id,
            )
        body = response["Body"]
        try:
            document = body.read()
        finally:
            body.close()
        return SnapshotManifest.from_json(document)


class SqsMessageAcknowledger:
    """Delete successfully processed deliveries from one configured SQS queue."""

    def __init__(self, *, client: SQSClient, queue_url: str) -> None:
        self._client = client
        self._queue_url = queue_url

    def acknowledge(self, receipt_handle: str) -> None:
        """Delete one delivery using its current receipt handle."""
        self._client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )
