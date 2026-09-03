from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingest.landing import _build_s3_client, _parse_observed_at, land_daily_snapshot
from ingest.models import (
    DownloadedSnapshot,
    FeedDefinition,
    SnapshotLocation,
    SnapshotManifest,
)


class MemorySnapshotStore:
    """Small storage test double that records workflow ordering."""

    def __init__(self, completed: SnapshotManifest | None = None) -> None:
        self.completed = completed
        self.operations: list[str] = []

    def read_completed_manifest(
        self, location: SnapshotLocation
    ) -> SnapshotManifest | None:
        self.operations.append(f"read:{location.manifest_key}")
        return self.completed

    def store_snapshot_object(
        self, location: SnapshotLocation, archive: Path, manifest: SnapshotManifest
    ) -> None:
        assert archive.exists()
        assert location.object_key == manifest.object_key
        self.operations.append("store-object")

    def publish_manifest(
        self, location: SnapshotLocation, manifest: SnapshotManifest
    ) -> SnapshotManifest:
        assert location.object_key == manifest.object_key
        self.operations.append("publish-manifest")
        self.completed = manifest
        return manifest


def _download_fixture(feed: FeedDefinition, destination: Path) -> DownloadedSnapshot:
    destination.write_bytes(b"compressed fixture")
    return DownloadedSnapshot(
        row_count=1,
        uncompressed_bytes=20,
        compressed_bytes=18,
        content_sha256="content-checksum",
        object_sha256="object-checksum",
        schema_fingerprint="schema-checksum",
        columns=("inspection_id", "dot_number", "insp_date"),
    )


def test_daily_landing_publishes_manifest_after_snapshot_object() -> None:
    store = MemorySnapshotStore()

    result = land_daily_snapshot(
        feed=FeedDefinition("inspections", "fx4q-ay7w"),
        observed_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
        store=store,
        downloader=_download_fixture,
    )

    assert result.created is True
    assert result.manifest.row_count == 1
    assert result.manifest.observed_at == "2026-09-02T12:00:00+00:00"
    assert store.operations[-2:] == ["store-object", "publish-manifest"]


def test_completed_daily_landing_does_not_download_again(tmp_path: Path) -> None:
    feed = FeedDefinition("crashes", "aayw-vxb3")
    location = SnapshotLocation.for_daily_snapshot(
        feed, datetime(2026, 9, 2, 12, tzinfo=UTC)
    )
    completed = SnapshotManifest.from_download(
        feed, location, _download_fixture(feed, tmp_path / "fixture")
    )
    store = MemorySnapshotStore(completed=completed)

    def unexpected_download(
        feed: FeedDefinition, destination: Path
    ) -> DownloadedSnapshot:
        raise AssertionError("completed acquisition must not download again")

    result = land_daily_snapshot(
        feed=feed,
        observed_at=location.observed_at,
        store=store,
        downloader=unexpected_download,
    )

    assert result.created is False
    assert result.manifest == completed
    assert len(store.operations) == 1


def test_observed_at_requires_a_timezone() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="timezone"):
        _parse_observed_at("2026-09-02T12:00:00")


def test_s3_client_uses_assumed_role_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class StubStsClient:
        def assume_role(self, **kwargs: object) -> dict[str, object]:
            calls.append(("assume-role", kwargs))
            return {
                "Credentials": {
                    "AccessKeyId": "temporary-access-key",
                    "SecretAccessKey": "temporary-secret-key",
                    "SessionToken": "temporary-session-token",
                }
            }

    s3_client = object()

    def client(service_name: str, **kwargs: object) -> object:
        calls.append((service_name, kwargs))
        return StubStsClient() if service_name == "sts" else s3_client

    monkeypatch.setattr("ingest.landing.boto3.client", client)

    result = _build_s3_client(
        region="us-east-1",
        role_arn="arn:aws:iam::123456789012:role/carrier-risk-ingest-dev",
    )

    assert result is s3_client
    assert calls == [
        ("sts", {"region_name": "us-east-1"}),
        (
            "assume-role",
            {
                "RoleArn": ("arn:aws:iam::123456789012:role/carrier-risk-ingest-dev"),
                "RoleSessionName": "carrier-risk-ingest",
            },
        ),
        (
            "s3",
            {
                "region_name": "us-east-1",
                "aws_access_key_id": "temporary-access-key",
                "aws_secret_access_key": "temporary-secret-key",
                "aws_session_token": "temporary-session-token",
            },
        ),
    ]
