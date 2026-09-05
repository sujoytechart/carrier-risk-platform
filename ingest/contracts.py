"""Versioned source-schema contracts for immutable snapshot manifests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files
from typing import Self, cast

from ingest.models import SnapshotManifest


@dataclass(frozen=True)
class FeedSchema:
    """The exact source columns accepted for one feed contract version."""

    version: int
    feed_name: str
    dataset_id: str
    columns: tuple[str, ...]

    @classmethod
    def load_configured(cls, feed_name: str) -> Self:
        """Load a packaged contract, rejecting feeds without an explicit schema."""
        if feed_name not in {"crashes", "inspections"}:
            raise ValueError(f"Unknown feed {feed_name!r}; no schema is configured")
        document = files("ingest").joinpath("schemas", f"{feed_name}.json").read_text()
        payload = cast(dict[str, object], json.loads(document))
        return cls(
            version=cast(int, payload["version"]),
            feed_name=cast(str, payload["feed_name"]),
            dataset_id=cast(str, payload["dataset_id"]),
            columns=tuple(cast(list[str], payload["columns"])),
        )

    @property
    def schema_fingerprint(self) -> str:
        """Return the fingerprint produced by the Phase 0 CSV inspector."""
        document = json.dumps(self.columns, separators=(",", ":")).encode()
        return hashlib.sha256(document).hexdigest()


def validate_manifest(manifest: SnapshotManifest, schema: FeedSchema) -> None:
    """Reject manifest evidence that does not satisfy its feed contract."""
    if manifest.feed_name != schema.feed_name:
        raise ValueError(
            f"Manifest feed {manifest.feed_name!r} does not match {schema.feed_name!r}"
        )
    if manifest.dataset_id != schema.dataset_id:
        raise ValueError(
            f"Manifest dataset {manifest.dataset_id!r} does not match "
            f"{schema.dataset_id!r}"
        )
    if manifest.columns != schema.columns:
        raise ValueError(
            f"Manifest columns do not match {schema.feed_name} schema version "
            f"{schema.version}"
        )
    if manifest.schema_fingerprint != schema.schema_fingerprint:
        raise ValueError("Manifest schema fingerprint does not match its columns")

    observed_at = datetime.fromisoformat(manifest.observed_at)
    if observed_at.tzinfo is None:
        raise ValueError("Manifest observed_at must include a timezone")
    acquisition_date = observed_at.astimezone(UTC).date().isoformat()
    expected_key = (
        f"raw/feed={schema.feed_name}/acquisition_date={acquisition_date}/"
        "snapshot.csv.gz"
    )
    if manifest.object_key != expected_key:
        raise ValueError(
            f"Manifest object key {manifest.object_key!r} does not match "
            f"{expected_key!r}"
        )
