from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, cast

import pytest
from botocore.exceptions import ClientError

from ingest.athena_catalog import RawCatalogPublisher, S3CatalogStore
from ingest.contracts import FeedSchema
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.object_store import FileSnapshotObjectStore

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


class MemoryCatalogStore:
    """Keep manifests and catalog documents in memory for publisher tests."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.writes: list[tuple[str, bytes, str]] = []

    def read_object(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def write_object_if_absent(
        self, key: str, content: bytes, *, content_type: str
    ) -> bool:
        if key in self.objects:
            return False
        self.objects[key] = content
        self.writes.append((key, content, content_type))
        return True


class UnverifiedSnapshotStore:
    """Expose bytes without object checks so content checks can be isolated."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    @contextmanager
    def open_snapshot(self, manifest: SnapshotManifest) -> Iterator[BinaryIO]:
        del manifest
        yield io.BytesIO(self.payload)


class FakeCatalogS3Client:
    """Implement the small S3 surface required by ``S3CatalogStore``."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_requests: list[dict[str, object]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, **request: object) -> dict[str, object]:
        key = request["Key"]
        assert isinstance(key, str)
        if key in self.objects:
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed", "Message": "exists"}},
                "PutObject",
            )
        body = request["Body"]
        assert isinstance(body, bytes)
        self.objects[key] = body
        self.put_requests.append(request)
        return {}


def _snapshot_fixture(
    tmp_path: Path,
    *,
    feed_name: str = "crashes",
    rows: list[list[str]] | None = None,
    raw_csv: bytes | None = None,
) -> tuple[SnapshotManifest, Path]:
    schema = FeedSchema.load_configured(feed_name)
    if raw_csv is None:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(schema.columns)
        default_row = [
            "1",
            'carrier, "quoted"',
            *([""] * (len(schema.columns) - 2)),
        ]
        writer.writerows(rows or [default_row])
        raw_csv = output.getvalue().encode("utf-8")
    compressed = gzip.compress(raw_csv, mtime=0)
    observed_at = datetime(2026, 9, 3, 12, tzinfo=UTC)
    feed = FEEDS[feed_name]
    location = SnapshotLocation.for_daily_snapshot(feed, observed_at)
    manifest = SnapshotManifest.from_download(
        feed,
        location,
        DownloadedSnapshot(
            row_count=len(rows) if rows is not None else 1,
            uncompressed_bytes=len(raw_csv),
            compressed_bytes=len(compressed),
            content_sha256=hashlib.sha256(raw_csv).hexdigest(),
            object_sha256=hashlib.sha256(compressed).hexdigest(),
            schema_fingerprint=schema.schema_fingerprint,
            columns=schema.columns,
        ),
    )
    object_path = tmp_path / manifest.object_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(compressed)
    return manifest, compressed


def _publisher(
    tmp_path: Path,
    manifest: SnapshotManifest,
    *,
    store: MemoryCatalogStore | None = None,
    snapshot_store: FileSnapshotObjectStore | UnverifiedSnapshotStore | None = None,
) -> tuple[RawCatalogPublisher, MemoryCatalogStore]:
    catalog_store = store or MemoryCatalogStore(
        {
            manifest.object_key.replace("snapshot.csv.gz", "manifest.json"): (
                manifest.to_json()
            )
        }
    )
    return (
        RawCatalogPublisher(
            bucket="carrier-risk-raw-test",
            catalog_store=catalog_store,
            snapshot_store=snapshot_store or FileSnapshotObjectStore(tmp_path),
        ),
        catalog_store,
    )


def test_publish_validates_complete_snapshot_then_writes_metadata_and_pointer(
    tmp_path: Path,
) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    publisher, store = _publisher(tmp_path, manifest)

    result = publisher.publish("crashes", "2026-09-03")

    assert result.created is True
    assert result.pointer_key == (
        "catalog/feed=crashes/acquisition_date=2026-09-03/snapshot.txt"
    )
    assert result.metadata_key == (
        "catalog-metadata/feed=crashes/acquisition_date=2026-09-03/manifest.jsonl"
    )
    assert [write[0] for write in store.writes] == [
        result.metadata_key,
        result.pointer_key,
    ]
    assert store.objects[result.pointer_key] == (
        b"s3://carrier-risk-raw-test/raw/feed=crashes/"
        b"acquisition_date=2026-09-03/snapshot.csv.gz\n"
    )
    assert json.loads(store.objects[result.metadata_key]) == {
        "batch_id": manifest.batch_id,
        "content_sha256": manifest.content_sha256,
        "feed": "crashes",
        "object_sha256": manifest.object_sha256,
        "observed_at": manifest.observed_at,
        "row_count": 1,
        "schema_fingerprint": manifest.schema_fingerprint,
    }


def test_publish_accepts_a_utf8_bom_and_csv_quoting(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("inspections")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(schema.columns)
    writer.writerow(["1", 'carrier, "quoted"', *([""] * (len(schema.columns) - 2))])
    manifest, _ = _snapshot_fixture(
        tmp_path,
        feed_name="inspections",
        raw_csv=b"\xef\xbb\xbf" + output.getvalue().encode(),
        rows=[["fixture"]],
    )
    publisher, _ = _publisher(tmp_path, manifest)

    result = publisher.publish("inspections", "2026-09-03")

    assert result.created is True


def test_publish_accepts_a_backslash_before_a_closing_csv_quote(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    raw_csv = (
        ",".join(schema.columns)
        + "\n"
        + '1,"carrier,\\",'
        + ",".join([""] * (len(schema.columns) - 2))
        + "\n"
    ).encode()
    manifest, _ = _snapshot_fixture(tmp_path, raw_csv=raw_csv, rows=[["fixture"]])
    publisher, store = _publisher(tmp_path, manifest)

    result = publisher.publish("crashes", "2026-09-03")

    assert result.created is True
    assert json.loads(store.objects[result.metadata_key])["row_count"] == 1


def test_publish_accepts_a_literal_backslash_with_disabled_escaping(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    row = [""] * len(schema.columns)
    row[schema.columns.index("REPORT_DATE")] = "2026\\0903"
    row[schema.columns.index("ADD_DATE")] = "20260903 1200"
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(schema.columns)
    writer.writerow(row)
    manifest, _ = _snapshot_fixture(
        tmp_path,
        raw_csv=output.getvalue().encode(),
        rows=[row],
    )
    publisher, store = _publisher(tmp_path, manifest)

    result = publisher.publish("crashes", "2026-09-03")

    assert result.created is True
    assert json.loads(store.objects[result.metadata_key])["content_sha256"] == (
        manifest.content_sha256
    )


@pytest.mark.parametrize("feed_name", ["crashes", "inspections"])
@pytest.mark.parametrize("value", ["carrier\0name", "carrier,\0name"])
def test_publish_rejects_nul_before_any_catalog_publication(
    tmp_path: Path, feed_name: str, value: str
) -> None:
    schema = FeedSchema.load_configured(feed_name)
    row = ["1", value, *([""] * (len(schema.columns) - 2))]
    manifest, _ = _snapshot_fixture(tmp_path, feed_name=feed_name, rows=[row])
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="Source row 1.*NUL.*OpenCSVSerde"):
        publisher.publish(feed_name, "2026-09-03")

    assert store.writes == []


def test_publish_rejects_a_quote_that_opencsvserde_opens_inside_unquoted_text(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    raw_csv = (
        ",".join(schema.columns)
        + "\n"
        + '1,carrier"x,'
        + ",".join([""] * (len(schema.columns) - 2))
        + "\n"
    ).encode()
    manifest, _ = _snapshot_fixture(tmp_path, raw_csv=raw_csv, rows=[["fixture"]])
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="quote inside unquoted.*OpenCSVSerde"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_rejects_a_missing_completed_manifest(tmp_path: Path) -> None:
    publisher = RawCatalogPublisher(
        bucket="carrier-risk-raw-test",
        catalog_store=MemoryCatalogStore(),
        snapshot_store=FileSnapshotObjectStore(tmp_path),
    )

    with pytest.raises(FileNotFoundError, match="completed manifest"):
        publisher.publish("crashes", "2026-09-03")


def test_publish_binds_manifest_observation_date_to_requested_partition(
    tmp_path: Path,
) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    store = MemoryCatalogStore(
        {
            "raw/feed=crashes/acquisition_date=2026-09-04/manifest.json": (
                manifest.to_json()
            )
        }
    )
    publisher, store = _publisher(tmp_path, manifest, store=store)

    with pytest.raises(ValueError, match="observation date.*requested partition"):
        publisher.publish("crashes", "2026-09-04")

    assert store.writes == []


def test_s3_catalog_store_reads_and_conditionally_creates_small_documents() -> None:
    client = FakeCatalogS3Client()
    store = S3CatalogStore("raw-bucket", cast("S3Client", client))

    assert store.read_object("missing") is None
    assert store.write_object_if_absent(
        "catalog/snapshot.txt", b"pointer\n", content_type="text/plain"
    )
    assert not store.write_object_if_absent(
        "catalog/snapshot.txt", b"replacement\n", content_type="text/plain"
    )
    assert store.read_object("catalog/snapshot.txt") == b"pointer\n"
    assert client.put_requests == [
        {
            "Bucket": "raw-bucket",
            "Key": "catalog/snapshot.txt",
            "Body": b"pointer\n",
            "ContentType": "text/plain",
            "IfNoneMatch": "*",
        }
    ]


@pytest.mark.parametrize("acquisition_date", ["2026-9-3", "not-a-date"])
def test_publish_rejects_a_noncanonical_acquisition_date(
    tmp_path: Path, acquisition_date: str
) -> None:
    publisher = RawCatalogPublisher(
        bucket="carrier-risk-raw-test",
        catalog_store=MemoryCatalogStore(),
        snapshot_store=FileSnapshotObjectStore(tmp_path),
    )

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        publisher.publish("crashes", acquisition_date)


def test_publish_rejects_manifest_schema_drift_before_reading_snapshot(
    tmp_path: Path,
) -> None:
    manifest, compressed = _snapshot_fixture(tmp_path)
    drifted = replace(
        manifest,
        columns=(*manifest.columns[:-1], "UNEXPECTED_COLUMN"),
        schema_fingerprint="0" * 64,
    )
    publisher, store = _publisher(
        tmp_path,
        drifted,
        snapshot_store=UnverifiedSnapshotStore(compressed),
    )

    with pytest.raises(ValueError, match="columns"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_rejects_csv_header_order_drift(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    changed_header = (schema.columns[1], schema.columns[0], *schema.columns[2:])
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(changed_header)
    writer.writerow(["1"] * len(schema.columns))
    manifest, _ = _snapshot_fixture(
        tmp_path, raw_csv=output.getvalue().encode(), rows=[["fixture"]]
    )
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="header.*order"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


@pytest.mark.parametrize("line_break", ["\n", "\r", "\r\n"])
def test_publish_rejects_embedded_line_breaks_unsupported_by_opencsvserde(
    tmp_path: Path, line_break: str
) -> None:
    schema = FeedSchema.load_configured("crashes")
    raw_csv = (
        ",".join(schema.columns)
        + "\n"
        + f'1,"first{line_break}second",'
        + ",".join([""] * (len(schema.columns) - 3))
        + "\n"
    ).encode()
    manifest, _ = _snapshot_fixture(tmp_path, raw_csv=raw_csv, rows=[["fixture"]])
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="Source row 1.*embedded CR/LF.*OpenCSVSerde"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_rejects_a_row_with_the_wrong_column_count(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(schema.columns)
    writer.writerow(["too", "short"])
    manifest, _ = _snapshot_fixture(
        tmp_path, raw_csv=output.getvalue().encode(), rows=[["fixture"]]
    )
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="Source row 1 has 2 columns; expected 59"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_reports_malformed_csv_before_cataloging(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    raw_csv = (",".join(schema.columns) + '\n1,"unterminated').encode()
    manifest, _ = _snapshot_fixture(tmp_path, raw_csv=raw_csv, rows=[["fixture"]])
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="CSV parsing failed.*source row 1"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_reports_a_malformed_header_before_cataloging(tmp_path: Path) -> None:
    manifest, _ = _snapshot_fixture(
        tmp_path,
        raw_csv=b'"unterminated',
        rows=[],
    )
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="CSV header parsing failed"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_rejects_a_manifest_row_count_mismatch(tmp_path: Path) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    manifest = replace(manifest, row_count=2)
    publisher, store = _publisher(tmp_path, manifest)

    with pytest.raises(ValueError, match="row count.*expected=2, actual=1"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"uncompressed_bytes": 1}, "content byte count"),
        ({"content_sha256": "0" * 64}, "content checksum"),
        ({"compressed_bytes": 1}, "object size"),
        ({"object_sha256": "0" * 64, "batch_id": ""}, "object checksum"),
    ],
)
def test_publish_rejects_snapshot_integrity_mismatches(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    changed = replace(manifest, **change)
    publisher, store = _publisher(tmp_path, changed)

    with pytest.raises(ValueError, match=message):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_publish_is_idempotent_for_matching_catalog_documents(tmp_path: Path) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    publisher, store = _publisher(tmp_path, manifest)
    first = publisher.publish("crashes", "2026-09-03")

    second = publisher.publish("crashes", "2026-09-03")

    assert first.created is True
    assert second.created is False
    assert len(store.writes) == 2


@pytest.mark.parametrize("conflict", ["metadata", "pointer"])
def test_publish_rejects_conflicting_existing_catalog_content_before_writing(
    tmp_path: Path, conflict: str
) -> None:
    manifest, _ = _snapshot_fixture(tmp_path)
    publisher, store = _publisher(tmp_path, manifest)
    location = publisher.location_for("crashes", "2026-09-03")
    key = location.metadata_key if conflict == "metadata" else location.pointer_key
    store.objects[key] = b"conflicting content\n"

    with pytest.raises(RuntimeError, match=f"Conflicting {conflict}"):
        publisher.publish("crashes", "2026-09-03")

    assert store.writes == []


def test_athena_sql_uses_stable_tables_and_explicit_snapshot_partitions() -> None:
    sql_root = Path(__file__).parents[1] / "analytics" / "athena"
    validation_sql = (sql_root / "validate_raw.sql").read_text()
    lag_sql = (sql_root / "report_lag.sql").read_text()

    for sql in (validation_sql, lag_sql):
        assert "raw_crashes" in sql
        assert "raw_inspections" in sql
        assert "raw_snapshot_metadata" in sql
        assert "acquisition_date" in sql
        assert "selected_snapshots" in sql
        assert "${" not in sql
        assert "CHANGE_DATE" not in sql.upper()
        assert sql.count("between 1 and 9999") == 2
        assert "substr(trim(raw_event_date), 1, 4)" in sql
        assert "substr(trim(raw_source_add_at), 1, 4)" in sql

    assert "metadata_row_count" in validation_sql
    assert "catalog_row_count" in validation_sql
    assert "invalid_event_date_count" in validation_sql
    assert "negative_lag_count" in validation_sql
    assert "availability_quality" in lag_sql
    assert "source_proxy" in lag_sql
    assert "r.add_date" in lag_sql
    assert "r.mcmis_add_date" in lag_sql
    assert "interval '1' day" in lag_sql
    assert "approx_percentile" in lag_sql
    assert "lag_bucket" in lag_sql
