from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from ingest.models import (
    DownloadedSnapshot,
    FeedDefinition,
    SnapshotLocation,
    SnapshotManifest,
)


def test_daily_location_uses_utc_date_and_partitioned_keys() -> None:
    eastern = timezone(timedelta(hours=-4))
    location = SnapshotLocation.for_daily_snapshot(
        FeedDefinition("crashes", "aayw-vxb3"),
        datetime(2026, 9, 2, 22, tzinfo=eastern),
    )

    assert location.observed_at == datetime(2026, 9, 3, 2, tzinfo=UTC)
    assert location.object_key == (
        "raw/feed=crashes/acquisition_date=2026-09-03/snapshot.csv.gz"
    )
    assert location.manifest_key.endswith("/manifest.json")


def test_daily_location_rejects_an_ambiguous_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone"):
        SnapshotLocation.for_daily_snapshot(
            FeedDefinition("crashes", "aayw-vxb3"), datetime(2026, 9, 2, 12)
        )


def test_manifest_json_round_trip_preserves_tuple_columns() -> None:
    feed = FeedDefinition("inspections", "fx4q-ay7w")
    location = SnapshotLocation.for_daily_snapshot(
        feed, datetime(2026, 9, 2, 12, tzinfo=UTC)
    )
    manifest = SnapshotManifest.from_download(
        feed,
        location,
        DownloadedSnapshot(
            row_count=10,
            uncompressed_bytes=100,
            compressed_bytes=50,
            content_sha256="content",
            object_sha256="object",
            schema_fingerprint="schema",
            columns=("inspection_id", "dot_number"),
        ),
    )

    assert SnapshotManifest.from_json(manifest.to_json()) == manifest
