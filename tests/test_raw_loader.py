from __future__ import annotations

import gzip
import hashlib
import importlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from ingest.contracts import FeedSchema
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.object_store import FileSnapshotObjectStore

POSTGRES_DSN = os.getenv(
    "CARRIER_RISK_TEST_DATABASE_URL",
    "postgresql://carrier_risk:carrier_risk@localhost:5432/carrier_risk",
)


@pytest.fixture(autouse=True)
def empty_raw_schema() -> None:
    """Give every loader test an independent warehouse transaction history."""
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute("drop schema if exists raw cascade")


def _snapshot_fixture(
    root: Path,
    *,
    rows: tuple[tuple[str, str], ...] = (("1", "alpha"), ("2", "beta")),
) -> tuple[FeedSchema, SnapshotManifest]:
    schema = FeedSchema(
        version=1,
        feed_name="crashes",
        dataset_id="aayw-vxb3",
        columns=("CRASH_ID", "REPORT_STATE"),
    )
    source = "CRASH_ID,REPORT_STATE\n" + "".join(
        f"{crash_id},{state}\n" for crash_id, state in rows
    )
    source_bytes = source.encode()
    compressed = gzip.compress(source_bytes, mtime=0)
    location = SnapshotLocation.for_daily_snapshot(
        FEEDS["crashes"], datetime(2026, 9, 3, 12, tzinfo=UTC)
    )
    object_path = root / location.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(compressed)
    schema_document = json.dumps(schema.columns, separators=(",", ":")).encode()
    manifest = SnapshotManifest.from_download(
        FEEDS["crashes"],
        location,
        DownloadedSnapshot(
            row_count=len(rows),
            uncompressed_bytes=len(source_bytes),
            compressed_bytes=len(compressed),
            content_sha256=hashlib.sha256(source_bytes).hexdigest(),
            object_sha256=hashlib.sha256(compressed).hexdigest(),
            schema_fingerprint=hashlib.sha256(schema_document).hexdigest(),
            columns=schema.columns,
        ),
    )
    return schema, manifest


def test_loader_commits_each_complete_batch_once(tmp_path: Path) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path)
    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={"crashes": schema},
    )

    first = loader.load(manifest)
    replay = loader.load(manifest)

    assert first == raw_loader.RawLoadResult(
        batch_id=manifest.batch_id,
        inserted_rows=2,
        already_loaded=False,
    )
    assert replay == raw_loader.RawLoadResult(
        batch_id=manifest.batch_id,
        inserted_rows=0,
        already_loaded=True,
    )
    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute("select count(*) from raw.crash_rows").fetchone() == (
            2,
        )
        assert connection.execute(
            "select status, row_count from raw.snapshot_batches"
        ).fetchone() == ("loaded", 2)


def test_row_count_mismatch_rolls_back_the_entire_batch(tmp_path: Path) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path, rows=(("1", "alpha"),))
    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={"crashes": schema},
    )

    with pytest.raises(ValueError, match="row count"):
        loader.load(replace(manifest, row_count=2))

    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute(
            "select to_regclass('raw.snapshot_batches')"
        ).fetchone() == (None,)
