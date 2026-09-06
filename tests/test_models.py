from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

import ingest.models as models
from ingest.models import (
    FEEDS,
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
            content_sha256="1" * 64,
            object_sha256="2" * 64,
            schema_fingerprint="3" * 64,
            columns=("inspection_id", "dot_number"),
        ),
    )

    restored = SnapshotManifest.from_json(manifest.to_json())

    assert restored == manifest
    assert restored.batch_id == (
        "f80c1cc855a61502ad4c11cc0afef5ba1cc812b42b03bae74dbfb438d4666b5b"
    )


def test_legacy_manifest_derives_its_batch_id_when_deserialized() -> None:
    payload = _valid_manifest_payload()
    payload["columns"] = ["crash_id"]
    payload["compressed_bytes"] = 8
    payload["uncompressed_bytes"] = 10
    legacy_document = json.dumps(payload).encode()

    manifest = SnapshotManifest.from_json(legacy_document)

    assert manifest.batch_id == (
        "bdb93550f652e4dc7516217e2fbfa77c35cfb347da044cf57435828e5e1ce0f0"
    )


def test_legacy_manifest_without_source_url_uses_configured_lineage() -> None:
    payload = _valid_manifest_payload()
    del payload["source_url"]

    manifest = SnapshotManifest.from_json(json.dumps(payload).encode())

    assert manifest.source_url == FEEDS["crashes"].source_url


def test_manifest_rejects_an_invalid_explicit_batch_id() -> None:
    payload = _valid_manifest_payload()
    payload.update(
        {
            "batch_id": "NOT-A-SHA256",
            "columns": ["inspection_id"],
            "compressed_bytes": 8,
            "dataset_id": "fx4q-ay7w",
            "feed_name": "inspections",
            "object_key": (
                "raw/feed=inspections/acquisition_date=2026-09-03/snapshot.csv.gz"
            ),
            "observed_at": "2026-09-03T00:14:48+00:00",
            "source_url": "https://example.test/inspections.csv",
            "uncompressed_bytes": 10,
        }
    )

    with pytest.raises(ValueError, match="batch_id"):
        SnapshotManifest.from_json(json.dumps(payload).encode())


def _valid_manifest_payload() -> dict[str, object]:
    return {
        "columns": ["CRASH_ID", "REPORT_STATE"],
        "compressed_bytes": 80,
        "content_sha256": "1" * 64,
        "dataset_id": "aayw-vxb3",
        "feed_name": "crashes",
        "object_key": ("raw/feed=crashes/acquisition_date=2026-09-03/snapshot.csv.gz"),
        "object_sha256": "2" * 64,
        "observed_at": "2026-09-03T00:07:10+00:00",
        "row_count": 1,
        "schema_fingerprint": "3" * 64,
        "source_url": "https://example.test/crashes.csv",
        "uncompressed_bytes": 100,
    }


def test_manifest_json_must_contain_an_object() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        SnapshotManifest.from_json(b"[]")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("feed_name", 1, "feed_name.*string"),
        ("source_url", None, "source_url.*string"),
        ("row_count", True, "row_count.*integer"),
        ("compressed_bytes", -1, "compressed_bytes.*non-negative"),
        ("uncompressed_bytes", -1, "uncompressed_bytes.*non-negative"),
        ("columns", "CRASH_ID", "columns.*list"),
        ("columns", ["CRASH_ID", 2], "columns.*strings"),
        ("columns", ["CRASH_ID", "CRASH_ID"], "columns.*duplicate"),
        ("content_sha256", "not-a-checksum", "content_sha256.*SHA-256"),
        ("object_sha256", "2" * 63, "object_sha256.*SHA-256"),
        ("schema_fingerprint", "G" * 64, "schema_fingerprint.*SHA-256"),
        ("observed_at", "yesterday", "observed_at.*ISO 8601"),
        ("observed_at", "2026-09-03T00:07:10", "observed_at.*timezone"),
    ],
)
def test_manifest_json_rejects_invalid_field_values(
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _valid_manifest_payload()
    payload[field] = value

    with pytest.raises(ValueError, match=message):
        SnapshotManifest.from_json(json.dumps(payload).encode())


def test_manifest_json_reports_missing_required_fields() -> None:
    payload = _valid_manifest_payload()
    del payload["dataset_id"]

    with pytest.raises(ValueError, match="dataset_id.*required"):
        SnapshotManifest.from_json(json.dumps(payload).encode())
