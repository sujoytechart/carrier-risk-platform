from __future__ import annotations

import pytest

from ingest.contracts import FeedSchema, validate_manifest
from ingest.models import FEEDS
from tests.local_airflow_fixture import build_snapshot


@pytest.mark.parametrize("feed_name", sorted(FEEDS))
def test_local_fixture_uses_valid_configured_lineage(feed_name: str) -> None:
    """The acceptance fixture must satisfy the same manifest contract as AWS."""
    _, manifest, _ = build_snapshot(feed_name)

    validate_manifest(manifest, FeedSchema.load_configured(feed_name))

    assert manifest.source_url == FEEDS[feed_name].source_url
