"""Domain objects shared by feed download and snapshot storage."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Self, cast

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def derive_batch_id(
    *,
    feed_name: str,
    dataset_id: str,
    observed_at: str,
    object_key: str,
    object_sha256: str,
) -> str:
    """Return the stable identity of one immutable snapshot acquisition.

    Components are length-prefixed before hashing so different tuple boundaries
    can never produce the same input document. The object checksum participates
    in identity, while event payload hashes remain change detectors only.
    """
    digest = hashlib.sha256()
    for component in (
        feed_name,
        dataset_id,
        observed_at,
        object_key,
        object_sha256,
    ):
        encoded = component.encode("utf-8")
        digest.update(f"{len(encoded)}:".encode())
        digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True)
class FeedDefinition:
    """Identify one complete FMCSA export and its stable download URL."""

    name: str
    dataset_id: str

    @property
    def source_url(self) -> str:
        """Return the Socrata endpoint that exports the complete dataset."""
        return (
            "https://data.transportation.gov/api/views/"
            f"{self.dataset_id}/rows.csv?accessType=DOWNLOAD"
        )


FEEDS = {
    "crashes": FeedDefinition(name="crashes", dataset_id="aayw-vxb3"),
    "inspections": FeedDefinition(name="inspections", dataset_id="fx4q-ay7w"),
}


@dataclass(frozen=True)
class DownloadedSnapshot:
    """Measurements derived while downloading and inspecting a source export."""

    row_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    content_sha256: str
    object_sha256: str
    schema_fingerprint: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class SnapshotLocation:
    """Deterministic S3 keys and knowledge time for one daily acquisition."""

    observed_at: datetime
    object_key: str
    manifest_key: str

    @classmethod
    def for_daily_snapshot(
        cls, feed: FeedDefinition, observed_at: datetime
    ) -> SnapshotLocation:
        """Build the one immutable destination assigned to a feed on a UTC day."""
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must include a timezone")

        observed_at = observed_at.astimezone(UTC)
        acquisition_date = observed_at.date().isoformat()
        prefix = f"raw/feed={feed.name}/acquisition_date={acquisition_date}"
        return cls(
            observed_at=observed_at,
            object_key=f"{prefix}/snapshot.csv.gz",
            manifest_key=f"{prefix}/manifest.json",
        )


@dataclass(frozen=True)
class SnapshotManifest:
    """Audit record proving what arrived, when it arrived, and what it contained."""

    feed_name: str
    dataset_id: str
    source_url: str
    observed_at: str
    object_key: str
    row_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    content_sha256: str
    object_sha256: str
    schema_fingerprint: str
    columns: tuple[str, ...]
    batch_id: str = ""

    def __post_init__(self) -> None:
        """Derive legacy batch identities and reject inconsistent manifests."""
        expected_batch_id = derive_batch_id(
            feed_name=self.feed_name,
            dataset_id=self.dataset_id,
            observed_at=self.observed_at,
            object_key=self.object_key,
            object_sha256=self.object_sha256,
        )
        if not self.batch_id:
            object.__setattr__(self, "batch_id", expected_batch_id)
        elif self.batch_id != expected_batch_id:
            raise ValueError("batch_id does not match the immutable manifest fields")

    @classmethod
    def from_download(
        cls,
        feed: FeedDefinition,
        location: SnapshotLocation,
        downloaded: DownloadedSnapshot,
    ) -> SnapshotManifest:
        """Combine source measurements with their final immutable destination."""
        return cls(
            feed_name=feed.name,
            dataset_id=feed.dataset_id,
            source_url=feed.source_url,
            observed_at=location.observed_at.isoformat(),
            object_key=location.object_key,
            row_count=downloaded.row_count,
            uncompressed_bytes=downloaded.uncompressed_bytes,
            compressed_bytes=downloaded.compressed_bytes,
            content_sha256=downloaded.content_sha256,
            object_sha256=downloaded.object_sha256,
            schema_fingerprint=downloaded.schema_fingerprint,
            columns=downloaded.columns,
        )

    def to_json(self) -> bytes:
        """Serialize a stable, human-readable manifest for S3."""
        return json.dumps(asdict(self), indent=2, sort_keys=True).encode("utf-8")

    @classmethod
    def from_json(cls, document: bytes) -> Self:
        """Deserialize a manifest after validating its untrusted JSON values."""
        decoded = cast(object, json.loads(document))
        if not isinstance(decoded, dict):
            raise ValueError("Snapshot manifest must be a JSON object")
        payload = cast(dict[str, object], decoded)

        observed_at = _required_string(payload, "observed_at")
        try:
            parsed_observed_at = datetime.fromisoformat(observed_at)
        except ValueError as error:
            raise ValueError(
                "Manifest field 'observed_at' must be an ISO 8601 timestamp"
            ) from error
        if parsed_observed_at.tzinfo is None or parsed_observed_at.utcoffset() is None:
            raise ValueError("Manifest field 'observed_at' must include a timezone")

        feed_name = _required_string(payload, "feed_name")
        dataset_id = _required_string(payload, "dataset_id")

        return cls(
            feed_name=feed_name,
            dataset_id=dataset_id,
            source_url=_source_url(payload, feed_name, dataset_id),
            observed_at=observed_at,
            object_key=_required_string(payload, "object_key"),
            row_count=_required_non_negative_integer(payload, "row_count"),
            uncompressed_bytes=_required_non_negative_integer(
                payload, "uncompressed_bytes"
            ),
            compressed_bytes=_required_non_negative_integer(
                payload, "compressed_bytes"
            ),
            content_sha256=_required_sha256(payload, "content_sha256"),
            object_sha256=_required_sha256(payload, "object_sha256"),
            schema_fingerprint=_required_sha256(payload, "schema_fingerprint"),
            columns=_required_columns(payload),
            batch_id=_optional_string(payload, "batch_id"),
        )


def _required_value(payload: dict[str, object], field_name: str) -> object:
    try:
        return payload[field_name]
    except KeyError as error:
        raise ValueError(f"Manifest field {field_name!r} is required") from error


def _required_string(payload: dict[str, object], field_name: str) -> str:
    value = _required_value(payload, field_name)
    if not isinstance(value, str):
        raise ValueError(f"Manifest field {field_name!r} must be a string")
    return value


def _optional_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name, "")
    if not isinstance(value, str):
        raise ValueError(f"Manifest field {field_name!r} must be a string")
    return value


def _source_url(
    payload: dict[str, object],
    feed_name: str,
    dataset_id: str,
) -> str:
    if "source_url" in payload:
        return _required_string(payload, "source_url")

    configured_feed = FEEDS.get(feed_name)
    if configured_feed is None or configured_feed.dataset_id != dataset_id:
        raise ValueError(
            "Legacy manifest without 'source_url' does not match a configured feed"
        )
    return configured_feed.source_url


def _required_non_negative_integer(payload: dict[str, object], field_name: str) -> int:
    value = _required_value(payload, field_name)
    if type(value) is not int:
        raise ValueError(f"Manifest field {field_name!r} must be an integer")
    if value < 0:
        raise ValueError(
            f"Manifest field {field_name!r} must be a non-negative integer"
        )
    return value


def _required_sha256(payload: dict[str, object], field_name: str) -> str:
    value = _required_string(payload, field_name)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"Manifest field {field_name!r} must be a 64-character lowercase "
            "SHA-256 hex digest"
        )
    return value


def _required_columns(payload: dict[str, object]) -> tuple[str, ...]:
    value = _required_value(payload, "columns")
    if not isinstance(value, list):
        raise ValueError("Manifest field 'columns' must be a list")
    if not all(isinstance(column, str) for column in value):
        raise ValueError("Manifest field 'columns' must contain only strings")

    columns = tuple(cast(list[str], value))
    if len(set(columns)) != len(columns):
        raise ValueError("Manifest field 'columns' contains duplicate names")
    return columns
