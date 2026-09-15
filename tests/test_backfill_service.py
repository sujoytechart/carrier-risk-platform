from __future__ import annotations

from datetime import date
from typing import cast

import pytest
from types_boto3_s3 import S3Client

from orchestration.backfill_service import discover_backfill_manifests
from orchestration.messages import ManifestReference


class StubPaginator:
    def paginate(self, **request: str) -> list[dict[str, object]]:
        assert request == {
            "Bucket": "raw-bucket",
            "Prefix": "raw/feed=crashes/acquisition_date=",
        }
        return [
            {
                "Contents": [
                    {
                        "Key": (
                            "raw/feed=crashes/acquisition_date=2026-09-03/manifest.json"
                        )
                    },
                    {
                        "Key": (
                            "raw/feed=crashes/acquisition_date=2026-09-02/"
                            "snapshot.csv.gz"
                        )
                    },
                    {
                        "Key": (
                            "raw/feed=crashes/acquisition_date=2026-09-02/manifest.json"
                        )
                    },
                    {
                        "Key": (
                            "raw/feed=crashes/acquisition_date=2026-08-31/manifest.json"
                        )
                    },
                ]
            }
        ]


class StubS3Client:
    def get_paginator(self, operation_name: str) -> StubPaginator:
        assert operation_name == "list_objects_v2"
        return StubPaginator()


def test_discovers_only_ordered_manifests_inside_inclusive_bounds() -> None:
    references = discover_backfill_manifests(
        client=cast(S3Client, StubS3Client()),
        bucket="raw-bucket",
        feed_name="crashes",
        start_date=date(2026, 9, 1),
        end_date=date(2026, 9, 3),
    )

    assert references == (
        ManifestReference(
            bucket="raw-bucket",
            key="raw/feed=crashes/acquisition_date=2026-09-02/manifest.json",
        ),
        ManifestReference(
            bucket="raw-bucket",
            key="raw/feed=crashes/acquisition_date=2026-09-03/manifest.json",
        ),
    )


def test_rejects_unknown_feeds_and_reversed_bounds() -> None:
    client = cast(S3Client, StubS3Client())

    with pytest.raises(ValueError, match="Unknown feed"):
        discover_backfill_manifests(
            client=client,
            bucket="raw-bucket",
            feed_name="unknown",
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 3),
        )

    with pytest.raises(ValueError, match="must not be after"):
        discover_backfill_manifests(
            client=client,
            bucket="raw-bucket",
            feed_name="crashes",
            start_date=date(2026, 9, 3),
            end_date=date(2026, 9, 1),
        )
