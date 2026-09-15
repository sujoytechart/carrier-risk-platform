"""Discovery of immutable manifests for a bounded historical replay."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from ingest.models import FEEDS
from orchestration.messages import ManifestReference

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


def discover_backfill_manifests(
    *,
    client: S3Client,
    bucket: str,
    feed_name: str,
    start_date: date,
    end_date: date,
) -> tuple[ManifestReference, ...]:
    """List manifest commit markers within an inclusive acquisition-date range."""
    if feed_name not in FEEDS:
        raise ValueError(f"Unknown feed {feed_name!r}")
    if start_date > end_date:
        raise ValueError("Backfill start_date must not be after end_date")

    prefix = f"raw/feed={feed_name}/acquisition_date="
    references: list[ManifestReference] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            acquisition_date = _acquisition_date(key, prefix)
            if (
                key.endswith("/manifest.json")
                and acquisition_date is not None
                and start_date <= acquisition_date <= end_date
            ):
                references.append(ManifestReference(bucket=bucket, key=key))
    return tuple(sorted(references, key=lambda reference: reference.key))


def _acquisition_date(key: str, prefix: str) -> date | None:
    partition = key.removeprefix(prefix).partition("/")
    if not partition[1]:
        return None
    try:
        return date.fromisoformat(partition[0])
    except ValueError:
        return None
