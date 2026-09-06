from __future__ import annotations

import gzip
import hashlib
import importlib
import io
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from ingest.contracts import FeedSchema
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.object_store import FileSnapshotObjectStore, S3SnapshotObjectStore

POSTGRES_DSN = os.environ["CARRIER_RISK_TEST_DATABASE_URL"]
POSTGRES_PARAMETERS = conninfo_to_dict(POSTGRES_DSN)
if POSTGRES_PARAMETERS != {
    "dbname": "carrier_risk_raw_test",
    "host": "localhost",
    "port": "5432",
    "user": "carrier_risk",
}:
    raise RuntimeError(
        "Raw-loader tests require exact libpq conninfo for carrier_risk_raw_test "
        "on localhost:5432 as carrier_risk"
    )


@pytest.fixture(autouse=True)
def empty_raw_schema() -> None:
    """Give every loader test an independent warehouse transaction history."""
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute("drop schema if exists raw cascade")


def _snapshot_fixture(
    root: Path,
    *,
    feed_name: str = "crashes",
    observed_at: datetime = datetime(2026, 9, 3, 12, tzinfo=UTC),
    rows: tuple[tuple[str, str], ...] = (("1", "alpha"), ("2", "beta")),
) -> tuple[FeedSchema, SnapshotManifest]:
    feed = FEEDS[feed_name]
    identifier_column = "CRASH_ID" if feed_name == "crashes" else "INSPECTION_ID"
    schema = FeedSchema(
        version=1,
        feed_name=feed_name,
        dataset_id=feed.dataset_id,
        columns=(identifier_column, "REPORT_STATE"),
    )
    source = f"{identifier_column},REPORT_STATE\n" + "".join(
        f"{record_id},{state}\n" for record_id, state in rows
    )
    source_bytes = source.encode()
    compressed = gzip.compress(source_bytes, mtime=0)
    location = SnapshotLocation.for_daily_snapshot(
        feed,
        observed_at,
    )
    object_path = root / location.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(compressed)
    schema_document = json.dumps(schema.columns, separators=(",", ":")).encode()
    manifest = SnapshotManifest.from_download(
        feed,
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
            "select status, row_count, source_url from raw.snapshot_batches"
        ).fetchone() == ("loaded", 2, manifest.source_url)


def test_replay_rejects_conflicting_source_lineage(tmp_path: Path) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path)
    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={"crashes": schema},
    )
    loader.load(manifest)

    conflicting_source_url = FEEDS["inspections"].source_url
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "update raw.snapshot_batches set source_url = %s where batch_id = %s",
            (conflicting_source_url, manifest.batch_id),
        )

    with pytest.raises(ValueError, match="source_url"):
        loader.load(manifest)

    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute(
            "select batch_id, source_url from raw.snapshot_batches"
        ).fetchall() == [(manifest.batch_id, conflicting_source_url)]


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
        ).fetchone() == ("raw.snapshot_batches",)
        assert connection.execute(
            "select count(*) from raw.snapshot_batches"
        ).fetchone() == (0,)
        assert connection.execute("select count(*) from raw.crash_rows").fetchone() == (
            0,
        )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"content_sha256": "0" * 64}, "content checksum"),
        ({"uncompressed_bytes": 1}, "content byte count"),
    ],
)
def test_uncompressed_integrity_failure_rolls_back_the_batch(
    tmp_path: Path,
    change: dict[str, object],
    message: str,
) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path)
    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={"crashes": schema},
    )

    with pytest.raises(ValueError, match=message):
        loader.load(replace(manifest, **change))

    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute(
            "select to_regclass('raw.snapshot_batches')"
        ).fetchone() == ("raw.snapshot_batches",)
        assert connection.execute(
            "select count(*) from raw.snapshot_batches"
        ).fetchone() == (0,)
        assert connection.execute("select count(*) from raw.crash_rows").fetchone() == (
            0,
        )


class _SynchronizedConnectionFactory:
    """Start both loader transactions together to exercise first-use DDL."""

    def __init__(self) -> None:
        self._barrier = threading.Barrier(2)

    def __call__(self) -> psycopg.Connection[tuple[object, ...]]:
        connection = psycopg.connect(POSTGRES_DSN)
        try:
            self._barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            connection.close()
            raise
        return connection


def test_first_concurrent_feed_loads_initialize_raw_relations_once(
    tmp_path: Path,
) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    crash_schema, crash_manifest = _snapshot_fixture(
        tmp_path,
        feed_name="crashes",
        rows=(("crash-1", "MI"),),
    )
    inspection_schema, inspection_manifest = _snapshot_fixture(
        tmp_path,
        feed_name="inspections",
        rows=(("inspection-1", "OH"),),
    )
    loader = raw_loader.RawSnapshotLoader(
        connection_factory=_SynchronizedConnectionFactory(),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={
            "crashes": crash_schema,
            "inspections": inspection_schema,
        },
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(loader.load, manifest)
            for manifest in (crash_manifest, inspection_manifest)
        ]
        results = [future.result(timeout=20) for future in futures]

    assert {result.batch_id for result in results} == {
        crash_manifest.batch_id,
        inspection_manifest.batch_id,
    }
    assert all(result.inserted_rows == 1 for result in results)
    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute(
            "select count(*) from raw.snapshot_batches"
        ).fetchone() == (2,)
        assert connection.execute("select count(*) from raw.crash_rows").fetchone() == (
            1,
        )
        assert connection.execute(
            "select count(*) from raw.inspection_rows"
        ).fetchone() == (1,)


def test_loader_refuses_to_mutate_an_incompatible_existing_registry(
    tmp_path: Path,
) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute("create schema raw")
        connection.execute(
            "create table raw.snapshot_batches (batch_id text primary key)"
        )

    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=FileSnapshotObjectStore(tmp_path),
        schemas={"crashes": schema},
    )

    with pytest.raises(ValueError, match="additive migration.*source_url"):
        loader.load(manifest)

    with psycopg.connect(POSTGRES_DSN) as connection:
        columns = connection.execute(
            """
            select column_name
              from information_schema.columns
             where table_schema = 'raw'
               and table_name = 'snapshot_batches'
            """
        ).fetchall()
    assert columns == [("batch_id",)]


class _RetainedMetadataS3Client:
    """Return changed bytes under the immutable object's original metadata."""

    def __init__(self, manifest: SnapshotManifest, changed_payload: bytes) -> None:
        self._manifest = manifest
        self._changed_payload = changed_payload

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket, Key
        return {
            "ContentLength": self._manifest.compressed_bytes,
            "Metadata": {"object-sha256": self._manifest.object_sha256},
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket, Key
        return {"Body": io.BytesIO(self._changed_payload)}


def test_changed_compressed_body_rolls_back_copied_rows(tmp_path: Path) -> None:
    raw_loader = importlib.import_module("ingest.raw_loader")
    schema, manifest = _snapshot_fixture(tmp_path)
    committed_path = tmp_path / manifest.object_key
    committed_payload = committed_path.read_bytes()
    changed_payload = gzip.compress(gzip.decompress(committed_payload), mtime=1)
    assert len(changed_payload) == len(committed_payload)
    assert changed_payload != committed_payload

    loader = raw_loader.RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(POSTGRES_DSN),
        object_store=S3SnapshotObjectStore(
            "raw-bucket",
            _RetainedMetadataS3Client(manifest, changed_payload),
        ),
        schemas={"crashes": schema},
    )

    with pytest.raises(ValueError, match="object checksum"):
        loader.load(manifest)

    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute(
            "select count(*) from raw.snapshot_batches"
        ).fetchone() == (0,)
        assert connection.execute("select count(*) from raw.crash_rows").fetchone() == (
            0,
        )
