"""Queue-to-warehouse application service with explicit commit boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ingest.models import SnapshotManifest
from ingest.raw_loader import RawLoadResult
from orchestration.messages import ManifestReference, parse_s3_manifest_message


class ManifestRepository(Protocol):
    """Retrieve immutable manifests from an object store."""

    def fetch(self, reference: ManifestReference) -> SnapshotManifest:
        """Return the manifest stored at the exact referenced object version."""


class SnapshotLoader(Protocol):
    """Commit one complete snapshot to the raw warehouse."""

    def load(self, manifest: SnapshotManifest) -> RawLoadResult:
        """Load the snapshot transactionally and return its replay-safe result."""


class MessageAcknowledger(Protocol):
    """Acknowledge a queue delivery after its durable side effects commit."""

    def acknowledge(self, receipt_handle: str) -> None:
        """Remove the delivered message identified by its receipt handle."""


@dataclass(frozen=True)
class QueueMessage:
    """Fields needed to process and acknowledge one SQS delivery."""

    body: str
    receipt_handle: str


@dataclass(frozen=True)
class LoadDependencies:
    """Injected remote boundaries used by the queue load workflow."""

    manifest_repository: ManifestRepository
    loader: SnapshotLoader
    acknowledger: MessageAcknowledger
    expected_bucket: str | None = None


def load_manifest(
    reference: ManifestReference,
    dependencies: LoadDependencies,
) -> RawLoadResult:
    """Fetch and transactionally load one committed manifest reference."""
    if (
        dependencies.expected_bucket is not None
        and reference.bucket != dependencies.expected_bucket
    ):
        raise ValueError(
            f"Manifest arrived from unexpected bucket {reference.bucket!r}"
        )
    manifest = dependencies.manifest_repository.fetch(reference)
    expected_manifest_key = manifest.object_key.rpartition("/")[0] + "/manifest.json"
    if reference.key != expected_manifest_key:
        raise ValueError("Snapshot payload does not belong to the manifest partition")
    return dependencies.loader.load(manifest)


def process_queue_message(
    message: QueueMessage,
    dependencies: LoadDependencies,
) -> tuple[RawLoadResult, ...]:
    """Load every manifest in a delivery before acknowledging it once.

    Exceptions from parsing, object retrieval, or database loading deliberately
    escape. In those cases acknowledgement is never attempted and SQS can retry
    the complete idempotent workflow.
    """
    references = parse_s3_manifest_message(message.body)
    results = tuple(load_manifest(reference, dependencies) for reference in references)
    dependencies.acknowledger.acknowledge(message.receipt_handle)
    return results
