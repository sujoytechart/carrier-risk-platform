from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

import ingest.models as models
from ingest.models import (
    DownloadedSnapshot,
    FeedDefinition,
    SnapshotLocation,
    SnapshotManifest,
)


def test_batch_id_uses_unambiguous_identity_components() -> None:
    first = models.derive_batch_id(
        feed_name="ab",
        dataset_id="c",
        observed_at="2026-09-03T00:00:00+00:00",
        object_key="raw/object",
        object_sha256="0" * 64,
    )
    second = models.derive_batch_id(
        feed_name="a",
        dataset_id="bc",
        observed_at="2026-09-03T00:00:00+00:00",
        object_key="raw/object",
        object_sha256="0" * 64,
    )

    assert first == "aafc524aa6edff4fced45eaafbb58407ddcd13be244b0e3471c352a1f79a2a0d"
    assert second == "8afb1b8a267c67b80965f2e5688595afcf6711f2cc5a6e32b65246b21a60b9b7"
    assert first != second


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

    restored = SnapshotManifest.from_json(manifest.to_json())

    assert restored == manifest
    assert restored.batch_id == (
        "a2859cf022dcb97c1c436282facc151c6aad186d44a55666bc8edcfb682d4c4c"
    )


def test_legacy_manifest_derives_its_batch_id_when_deserialized() -> None:
    legacy_document = b"""{
      "columns": ["crash_id"],
      "compressed_bytes": 8,
      "content_sha256": "content",
      "dataset_id": "aayw-vxb3",
      "feed_name": "crashes",
      "object_key": "raw/feed=crashes/acquisition_date=2026-09-03/snapshot.csv.gz",
      "object_sha256": "object",
      "observed_at": "2026-09-03T00:07:10+00:00",
      "row_count": 1,
      "schema_fingerprint": "schema",
      "source_url": "https://example.test/crashes.csv",
      "uncompressed_bytes": 10
    }"""

    manifest = SnapshotManifest.from_json(legacy_document)

    assert manifest.batch_id == (
        "4aac9195424e18697c6259fae2a0671d9064809774ce59c3744e1d37c127dd9f"
    )


def test_manifest_rejects_an_invalid_explicit_batch_id() -> None:
    invalid_document = b"""{
      "batch_id": "NOT-A-SHA256",
      "columns": ["inspection_id"],
      "compressed_bytes": 8,
      "content_sha256": "content",
      "dataset_id": "fx4q-ay7w",
      "feed_name": "inspections",
      "object_key": "raw/feed=inspections/acquisition_date=2026-09-03/snapshot.csv.gz",
      "object_sha256": "object",
      "observed_at": "2026-09-03T00:14:48+00:00",
      "row_count": 1,
      "schema_fingerprint": "schema",
      "source_url": "https://example.test/inspections.csv",
      "uncompressed_bytes": 10
    }"""

    with pytest.raises(ValueError, match="batch_id"):
        SnapshotManifest.from_json(invalid_document)
