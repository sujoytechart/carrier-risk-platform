"""Command-line entry point for landing immutable daily FMCSA snapshots."""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from ingest.models import (
    FEEDS,
    DownloadedSnapshot,
    FeedDefinition,
    SnapshotLocation,
    SnapshotManifest,
)
from ingest.source import download_feed
from ingest.storage import S3SnapshotStore, SnapshotStore

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client

FeedDownloader = Callable[[FeedDefinition, Path], DownloadedSnapshot]


@dataclass(frozen=True)
class LandingResult:
    """Outcome of a landing attempt, including whether it performed new work."""

    manifest: SnapshotManifest
    created: bool


def land_daily_snapshot(
    *,
    feed: FeedDefinition,
    observed_at: datetime,
    store: SnapshotStore,
    downloader: FeedDownloader = download_feed,
) -> LandingResult:
    """Land one complete daily feed snapshot through a manifest-last workflow.

    The deterministic daily key makes a completed rerun a no-op. The snapshot is
    downloaded to temporary local storage, fully counted and fingerprinted, and
    only then sent to the store. The manifest is published last and acts as the
    commit marker, so downstream consumers never treat a partial upload as ready.
    """
    location = SnapshotLocation.for_daily_snapshot(feed, observed_at)
    completed_manifest = store.read_completed_manifest(location)
    if completed_manifest is not None:
        return LandingResult(manifest=completed_manifest, created=False)

    with tempfile.TemporaryDirectory(prefix=f"carrier-risk-{feed.name}-") as temp_dir:
        archive = Path(temp_dir) / "snapshot.csv.gz"
        downloaded = downloader(feed, archive)
        manifest = SnapshotManifest.from_download(feed, location, downloaded)
        store.store_snapshot_object(location, archive, manifest)
        manifest = store.publish_manifest(location, manifest)

    return LandingResult(manifest=manifest, created=True)


def _parse_observed_at(value: str | None) -> datetime:
    """Parse an explicit knowledge time, defaulting to the current UTC time."""
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--observed-at must include a timezone")
    return parsed


def _build_s3_client(*, region: str, role_arn: str) -> S3Client:
    """Assume the least-privilege ingest role and return its S3 client.

    The operator's SSO session is used only to obtain short-lived credentials.
    Snapshot reads and writes then run through the dedicated role whose policy is
    restricted to the raw prefix and deliberately has no delete permission.
    """
    sts_client = boto3.client("sts", region_name=region)
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="carrier-risk-ingest",
    )
    credentials = response["Credentials"]
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def build_parser() -> argparse.ArgumentParser:
    """Describe the stable CLI contract used by operators and future DAG tasks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feed",
        choices=sorted(FEEDS),
        required=True,
        help="complete FMCSA event feed to acquire",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("CARRIER_RISK_RAW_BUCKET"),
        help="destination S3 bucket; defaults to CARRIER_RISK_RAW_BUCKET",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="AWS region; defaults to AWS_REGION or us-east-1",
    )
    parser.add_argument(
        "--role-arn",
        default=os.getenv("CARRIER_RISK_INGEST_ROLE_ARN"),
        help=(
            "least-privilege role to assume; defaults to CARRIER_RISK_INGEST_ROLE_ARN"
        ),
    )
    parser.add_argument(
        "--observed-at",
        help="ISO-8601 acquisition time; defaults to the current UTC time",
    )
    return parser


def main() -> int:
    """Run one landing attempt and print a concise operational summary."""
    args = build_parser().parse_args()
    if not args.bucket:
        raise SystemExit(
            "Set CARRIER_RISK_RAW_BUCKET or pass --bucket with the Terraform output."
        )
    if not args.role_arn:
        raise SystemExit(
            "Set CARRIER_RISK_INGEST_ROLE_ARN or pass --role-arn with the "
            "Terraform output."
        )

    s3_client = _build_s3_client(region=args.region, role_arn=args.role_arn)
    result = land_daily_snapshot(
        feed=FEEDS[args.feed],
        observed_at=_parse_observed_at(args.observed_at),
        store=S3SnapshotStore(bucket=args.bucket, client=s3_client),
    )
    disposition = "landed" if result.created else "already complete"
    manifest = result.manifest
    print(
        f"{manifest.feed_name}: {disposition}; rows={manifest.row_count}; "
        f"s3://{args.bucket}/{manifest.object_key}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
