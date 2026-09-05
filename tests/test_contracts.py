from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from ingest.contracts import FeedSchema, validate_manifest
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest


def _manifest_for(schema: FeedSchema) -> SnapshotManifest:
    feed_name = schema.feed_name
    feed = FEEDS[feed_name]
    columns = schema.columns
    schema_document = json.dumps(columns, separators=(",", ":")).encode()
    location = SnapshotLocation.for_daily_snapshot(
        feed,
        datetime(2026, 9, 3, 12, tzinfo=UTC),
    )
    return SnapshotManifest.from_download(
        feed,
        location,
        DownloadedSnapshot(
            row_count=2,
            uncompressed_bytes=20,
            compressed_bytes=10,
            content_sha256="1" * 64,
            object_sha256="2" * 64,
            schema_fingerprint=hashlib.sha256(schema_document).hexdigest(),
            columns=columns,
        ),
    )


def test_configured_schema_accepts_a_matching_manifest() -> None:
    schema = FeedSchema.load_configured("crashes")

    validate_manifest(_manifest_for(schema), schema)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"dataset_id": "wrong"}, "dataset"),
        ({"feed_name": "inspections"}, "feed"),
        ({"columns": ("WRONG",)}, "columns"),
        ({"schema_fingerprint": "0" * 64}, "fingerprint"),
        (
            {"object_key": "raw/feed=crashes/acquisition_date=2026-09-02/file.gz"},
            "object key",
        ),
    ],
)
def test_manifest_contract_rejects_inconsistent_evidence(
    change: dict[str, object], message: str
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _manifest_for(schema)
    changed_identity = any(
        field in change
        for field in (
            "feed_name",
            "dataset_id",
            "observed_at",
            "object_key",
            "object_sha256",
        )
    )
    if changed_identity:
        change["batch_id"] = ""

    with pytest.raises(ValueError, match=message):
        validate_manifest(replace(manifest, **change), schema)


def test_unknown_feed_has_no_implicit_schema() -> None:
    with pytest.raises(ValueError, match="Unknown feed"):
        FeedSchema.load_configured("unknown")


def test_manifest_partition_is_resolved_from_observed_at_in_utc() -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = replace(
        _manifest_for(schema),
        observed_at="2026-09-02T22:00:00-04:00",
        batch_id="",
    )

    validate_manifest(manifest, schema)
