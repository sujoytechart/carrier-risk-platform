from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as parquet
import pytest

from ingest.contracts import FeedSchema
from ingest.models import FEEDS, DownloadedSnapshot, SnapshotLocation, SnapshotManifest
from ingest.object_store import FileSnapshotObjectStore
from ingest.parquet_converter import ParquetSnapshotConverter


def _snapshot(
    tmp_path: Path,
    rows: list[list[str]],
    *,
    raw_csv: bytes | None = None,
) -> SnapshotManifest:
    schema = FeedSchema.load_configured("crashes")
    if raw_csv is None:
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator="\r\n")
        writer.writerow(schema.columns)
        writer.writerows(rows)
        raw_csv = text.getvalue().encode("utf-8")
    compressed = gzip.compress(raw_csv, mtime=0)
    location = SnapshotLocation.for_daily_snapshot(
        FEEDS["crashes"], datetime(2026, 9, 11, 12, tzinfo=UTC)
    )
    manifest = SnapshotManifest.from_download(
        FEEDS["crashes"],
        location,
        DownloadedSnapshot(
            row_count=len(rows),
            uncompressed_bytes=len(raw_csv),
            compressed_bytes=len(compressed),
            content_sha256=hashlib.sha256(raw_csv).hexdigest(),
            object_sha256=hashlib.sha256(compressed).hexdigest(),
            schema_fingerprint=schema.schema_fingerprint,
            columns=schema.columns,
        ),
    )
    source = tmp_path / manifest.object_key
    source.parent.mkdir(parents=True)
    source.write_bytes(compressed)
    return manifest


def test_convert_preserves_every_source_value_and_records_lineage(
    tmp_path: Path,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    first = ["00017", "", "literal\\path", "café", "line one\r\nline two"]
    rows = [
        first + [""] * (len(schema.columns) - len(first)),
        ['comma, and "quote"'] + [""] * (len(schema.columns) - 1),
    ]
    manifest = _snapshot(tmp_path, rows)
    destination = tmp_path / "derived" / manifest.batch_id
    converter = ParquetSnapshotConverter(
        snapshot_store=FileSnapshotObjectStore(tmp_path), batch_row_count=1
    )

    result = converter.convert(manifest, destination)

    table = parquet.read_table(result.parquet_path)
    assert table.schema.names == list(schema.columns)
    assert table.to_pylist() == [
        dict(zip(schema.columns, row, strict=True)) for row in rows
    ]
    assert all(field.type == "string" for field in table.schema)
    assert sum(column.null_count for column in table.columns) == 0
    lineage = json.loads(result.lineage_path.read_text())
    assert lineage["source_manifest"] == json.loads(manifest.to_json())
    assert lineage["observed_at"] == manifest.observed_at
    assert lineage["schema"] == {
        "columns": list(schema.columns),
        "fingerprint": schema.schema_fingerprint,
        "version": schema.version,
    }
    reconciliation = lineage["reconciliation"]
    assert reconciliation["parquet_null_count"] == 0
    assert reconciliation["parquet_row_count"] == 2
    assert reconciliation["parquet_value_count"] == 2 * len(schema.columns)
    assert reconciliation["source_row_count"] == 2
    assert reconciliation["source_value_count"] == 2 * len(schema.columns)
    assert (
        reconciliation["parquet_value_sha256"] == reconciliation["source_value_sha256"]
    )
    assert (
        lineage["parquet_sha256"]
        == hashlib.sha256(result.parquet_path.read_bytes()).hexdigest()
    )


def test_convert_is_idempotent_and_rejects_existing_conflict(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [["1"] + [""] * (len(schema.columns) - 1)])
    destination = tmp_path / "derived" / manifest.batch_id
    converter = ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))

    first = converter.convert(manifest, destination)
    second = converter.convert(manifest, destination)

    assert first.created is True
    assert second.created is False
    (destination / "lineage.json").write_text("{}")
    with pytest.raises(FileExistsError, match="conflicts"):
        converter.convert(manifest, destination)


def test_convert_rejects_corrupt_existing_parquet(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [["1"] + [""] * (len(schema.columns) - 1)])
    destination = tmp_path / "derived" / manifest.batch_id
    converter = ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    result = converter.convert(manifest, destination)
    result.parquet_path.write_bytes(b"corrupt")

    with pytest.raises(FileExistsError, match="conflicts"):
        converter.convert(manifest, destination)


def test_convert_rejects_compressed_checksum_mismatch(tmp_path: Path) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [["1"] + [""] * (len(schema.columns) - 1)])
    invalid = replace(manifest, object_sha256="0" * 64, batch_id="")
    destination = tmp_path / "derived" / invalid.batch_id

    with pytest.raises(ValueError, match="object checksum"):
        ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path)).convert(
            invalid, destination
        )

    assert not destination.exists()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda manifest: replace(manifest, row_count=2), "row count"),
        (
            lambda manifest: replace(manifest, content_sha256="0" * 64),
            "checksum",
        ),
        (
            lambda manifest: replace(manifest, uncompressed_bytes=1),
            "byte count",
        ),
        (lambda manifest: replace(manifest, compressed_bytes=1), "object size"),
    ],
)
def test_convert_rejects_bad_manifest_reconciliation_without_publication(
    tmp_path: Path,
    mutation: Callable[[SnapshotManifest], SnapshotManifest],
    message: str,
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [["1"] + [""] * (len(schema.columns) - 1)])
    invalid = mutation(manifest)
    destination = tmp_path / "derived" / manifest.batch_id

    with pytest.raises(ValueError, match=message):
        ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path)).convert(
            invalid, destination
        )

    assert not destination.exists()


@pytest.mark.parametrize("failure", ["header", "width"])
def test_convert_rejects_invalid_csv_without_publication(
    tmp_path: Path, failure: str
) -> None:
    schema = FeedSchema.load_configured("crashes")
    header = list(schema.columns)
    row = ["1"] + [""] * (len(schema.columns) - 1)
    if failure == "header":
        header[0] = "WRONG"
    else:
        row.pop()
    text = io.StringIO(newline="")
    writer = csv.writer(text, lineterminator="\n")
    writer.writerow(header)
    writer.writerow(row)
    manifest = _snapshot(tmp_path, [row], raw_csv=text.getvalue().encode())
    destination = tmp_path / "derived" / manifest.batch_id

    with pytest.raises(ValueError, match="header|width"):
        ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path)).convert(
            manifest, destination
        )

    assert not destination.exists()


def test_publication_rejects_a_concurrent_empty_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [[""] * len(schema.columns)])
    destination = tmp_path / "derived"
    converter = ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    reconcile = converter._reconcile_parquet

    def competing_publication(
        path: Path, schema: FeedSchema
    ) -> tuple[int, int, int, str]:
        result = reconcile(path, schema)
        destination.mkdir()
        return result

    monkeypatch.setattr(converter, "_reconcile_parquet", competing_publication)
    with pytest.raises(FileExistsError, match="conflicts"):
        converter.convert(manifest, destination)
    assert list(destination.iterdir()) == []


@pytest.mark.parametrize("malformation", ["nulls", "names", "numbers"])
def test_readable_malformed_parquet_is_an_unchanged_publication_conflict(
    tmp_path: Path, malformation: str
) -> None:
    import pyarrow as pa

    schema = FeedSchema.load_configured("crashes")
    manifest = _snapshot(tmp_path, [[""] * len(schema.columns)])
    destination = tmp_path / "derived"
    converter = ParquetSnapshotConverter(FileSnapshotObjectStore(tmp_path))
    result = converter.convert(manifest, destination)
    names = list(schema.columns)
    if malformation == "names":
        names[0] = "WRONG"
    value = None if malformation == "nulls" else 7
    table = pa.table({name: [value] for name in names})
    parquet.write_table(table, result.parquet_path)
    before = result.parquet_path.read_bytes()
    with pytest.raises(FileExistsError, match="conflicts"):
        converter.convert(manifest, destination)
    assert result.parquet_path.read_bytes() == before
