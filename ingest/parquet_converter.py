"""Validated, bounded-memory derivation of raw CSV snapshots to Parquet."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from io import BufferedReader, TextIOWrapper
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from ingest.contracts import FeedSchema, validate_manifest
from ingest.models import SnapshotManifest
from ingest.object_store import ContentIntegrityStream, SnapshotObjectStore

CONVERTER_VERSION = "1.0.0"
PARQUET_FILE_NAME = "snapshot.parquet"
LINEAGE_FILE_NAME = "lineage.json"
_HASH_BUFFER_BYTES = 1024 * 1024


class _Digest(Protocol):
    def update(self, content: bytes) -> None:
        """Add bytes to the digest."""


@dataclass(frozen=True)
class ParquetConversionResult:
    """Paths and identity of one complete local derived publication."""

    batch_id: str
    parquet_path: Path
    lineage_path: Path
    parquet_sha256: str
    created: bool


class ParquetSnapshotConverter:
    """Convert a manifest-verified gzip CSV snapshot into immutable Parquet.

    Source access is injected and conversion holds at most ``batch_row_count``
    rows in memory. A destination is a publication directory: its Parquet data
    is complete only when its lineage commit marker is present. Exclusive directory
    creation prevents replacing a competing publication.
    """

    def __init__(
        self,
        snapshot_store: SnapshotObjectStore,
        *,
        schemas: Mapping[str, FeedSchema] | None = None,
        batch_row_count: int = 10_000,
    ) -> None:
        if batch_row_count < 1:
            raise ValueError("batch_row_count must be positive")
        self._snapshot_store = snapshot_store
        self._schemas = schemas
        self._batch_row_count = batch_row_count

    def convert(
        self, manifest: SnapshotManifest, destination: Path
    ) -> ParquetConversionResult:
        """Validate, derive, reconcile, and publish one snapshot with a commit marker.

        A matching complete destination is an idempotent success. Any existing
        destination that cannot prove the same source identity and derived bytes
        is rejected rather than replaced.
        """
        schema = self._schema_for(manifest.feed_name)
        validate_manifest(manifest, schema)
        destination = destination.resolve()
        if destination.exists():
            return self._existing_result(manifest, schema, destination)

        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent)
        )
        try:
            parquet_path = temporary / PARQUET_FILE_NAME
            source_rows, source_values, source_value_hash = self._write_parquet(
                manifest, schema, parquet_path
            )
            parquet_rows, parquet_values, parquet_nulls, parquet_value_hash = (
                self._reconcile_parquet(parquet_path, schema)
            )
            if (
                parquet_rows != source_rows
                or parquet_values != source_values
                or parquet_nulls != 0
                or parquet_value_hash != source_value_hash
            ):
                raise ValueError("Derived Parquet value reconciliation failed")
            parquet_sha256 = _file_sha256(parquet_path)
            lineage = self._lineage(
                manifest=manifest,
                schema=schema,
                parquet_sha256=parquet_sha256,
                source_rows=source_rows,
                source_values=source_values,
                source_value_hash=source_value_hash,
                parquet_rows=parquet_rows,
                parquet_values=parquet_values,
                parquet_nulls=parquet_nulls,
                parquet_value_hash=parquet_value_hash,
            )
            (temporary / LINEAGE_FILE_NAME).write_bytes(_json_bytes(lineage))
            try:
                destination.mkdir()
            except FileExistsError:
                return self._existing_result(manifest, schema, destination)
            # Reserve the destination without replacement. The lineage file is
            # the commit marker and moves last; interruption leaves an explicit
            # incomplete conflict, never a successful or overwritten publication.
            os.replace(parquet_path, destination / PARQUET_FILE_NAME)
            os.replace(temporary / LINEAGE_FILE_NAME, destination / LINEAGE_FILE_NAME)
            return ParquetConversionResult(
                batch_id=manifest.batch_id,
                parquet_path=destination / PARQUET_FILE_NAME,
                lineage_path=destination / LINEAGE_FILE_NAME,
                parquet_sha256=parquet_sha256,
                created=True,
            )
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def _schema_for(self, feed_name: str) -> FeedSchema:
        if self._schemas is None:
            return FeedSchema.load_configured(feed_name)
        try:
            return self._schemas[feed_name]
        except KeyError as error:
            raise ValueError(f"No raw schema configured for {feed_name!r}") from error

    def _write_parquet(
        self,
        manifest: SnapshotManifest,
        schema: FeedSchema,
        parquet_path: Path,
    ) -> tuple[int, int, str]:
        arrow_schema = pa.schema(
            [pa.field(column, pa.string(), nullable=False) for column in schema.columns]
        )
        value_digest = hashlib.sha256()
        row_count = 0
        columns: list[list[str]] = [[] for _ in schema.columns]
        with (
            self._snapshot_store.open_snapshot(manifest) as object_stream,
            gzip.GzipFile(fileobj=object_stream, mode="rb") as archive,
            ContentIntegrityStream(
                archive,
                expected_bytes=manifest.uncompressed_bytes,
                expected_sha256=manifest.content_sha256,
                content_name="Snapshot content",
            ) as verified_content,
            BufferedReader(verified_content) as buffered,
            TextIOWrapper(buffered, encoding="utf-8-sig", newline="") as text,
            pq.ParquetWriter(
                parquet_path, arrow_schema, compression="snappy"
            ) as writer,
        ):
            reader = csv.reader(text, strict=True)
            try:
                header = next(reader)
            except StopIteration as error:
                raise ValueError("Snapshot contained no CSV header") from error
            except csv.Error as error:
                raise ValueError(
                    f"Snapshot CSV header parsing failed: {error}"
                ) from error
            if tuple(header) != schema.columns:
                raise ValueError(
                    "Snapshot CSV header names or order do not match the configured "
                    "feed schema"
                )
            try:
                for row in reader:
                    row_count += 1
                    if len(row) != len(schema.columns):
                        raise ValueError(
                            f"Snapshot source row {row_count} width does not match "
                            f"header: expected={len(schema.columns)}, actual={len(row)}"
                        )
                    _update_value_hash(value_digest, row)
                    for values, value in zip(columns, row, strict=True):
                        values.append(value)
                    if len(columns[0]) == self._batch_row_count:
                        writer.write_batch(
                            pa.record_batch(columns, schema=arrow_schema)
                        )
                        columns = [[] for _ in schema.columns]
            except csv.Error as error:
                raise ValueError(
                    f"Snapshot CSV parsing failed near source row {row_count + 1}: "
                    f"{error}"
                ) from error
            if columns[0]:
                writer.write_batch(pa.record_batch(columns, schema=arrow_schema))
            verified_content.verify_complete()
        if row_count != manifest.row_count:
            raise ValueError(
                "Snapshot row count does not match its manifest: "
                f"expected={manifest.row_count}, actual={row_count}"
            )
        return row_count, row_count * len(schema.columns), value_digest.hexdigest()

    def _reconcile_parquet(
        self, parquet_path: Path, schema: FeedSchema
    ) -> tuple[int, int, int, str]:
        parquet_file = pq.ParquetFile(parquet_path)
        expected_schema = pa.schema(
            [pa.field(column, pa.string(), nullable=False) for column in schema.columns]
        )
        if not parquet_file.schema_arrow.equals(expected_schema):
            raise ValueError(
                "Derived Parquet schema does not preserve required strings"
            )
        digest = hashlib.sha256()
        row_count = 0
        value_count = 0
        null_count = 0
        for batch in parquet_file.iter_batches(batch_size=self._batch_row_count):
            row_count += batch.num_rows
            value_count += batch.num_rows * batch.num_columns
            null_count += sum(column.null_count for column in batch.columns)
            if null_count:
                raise ValueError("Derived Parquet contains unexpected null values")
            rows = zip(*(column.to_pylist() for column in batch.columns), strict=True)
            for row in rows:
                _update_value_hash(digest, row)
        return row_count, value_count, null_count, digest.hexdigest()

    def _lineage(
        self,
        *,
        manifest: SnapshotManifest,
        schema: FeedSchema,
        parquet_sha256: str,
        source_rows: int,
        source_values: int,
        source_value_hash: str,
        parquet_rows: int,
        parquet_values: int,
        parquet_nulls: int,
        parquet_value_hash: str,
    ) -> dict[str, object]:
        return {
            "converter_version": CONVERTER_VERSION,
            "observed_at": manifest.observed_at,
            "parquet_sha256": parquet_sha256,
            "reconciliation": {
                "parquet_null_count": parquet_nulls,
                "parquet_row_count": parquet_rows,
                "parquet_value_count": parquet_values,
                "parquet_value_sha256": parquet_value_hash,
                "source_row_count": source_rows,
                "source_value_count": source_values,
                "source_value_sha256": source_value_hash,
            },
            "schema": {
                "columns": list(schema.columns),
                "fingerprint": schema.schema_fingerprint,
                "version": schema.version,
            },
            "source_manifest": json.loads(manifest.to_json()),
        }

    def _existing_result(
        self,
        manifest: SnapshotManifest,
        schema: FeedSchema,
        destination: Path,
    ) -> ParquetConversionResult:
        parquet_path = destination / PARQUET_FILE_NAME
        lineage_path = destination / LINEAGE_FILE_NAME
        try:
            lineage = json.loads(lineage_path.read_bytes())
            parquet_sha256 = _file_sha256(parquet_path)
            parquet_rows, parquet_values, parquet_nulls, parquet_value_hash = (
                self._reconcile_parquet(parquet_path, schema)
            )
        except (
            FileNotFoundError,
            ValueError,
            OSError,
            pa.ArrowException,
        ) as error:
            raise FileExistsError(
                f"Existing Parquet publication at {destination} conflicts"
            ) from error
        expected_values = manifest.row_count * len(schema.columns)
        expected_lineage = self._lineage(
            manifest=manifest,
            schema=schema,
            parquet_sha256=parquet_sha256,
            source_rows=manifest.row_count,
            source_values=expected_values,
            source_value_hash=parquet_value_hash,
            parquet_rows=parquet_rows,
            parquet_values=parquet_values,
            parquet_nulls=parquet_nulls,
            parquet_value_hash=parquet_value_hash,
        )
        if (
            lineage != expected_lineage
            or parquet_rows != manifest.row_count
            or parquet_values != expected_values
            or parquet_nulls != 0
        ):
            raise FileExistsError(
                f"Existing Parquet publication at {destination} conflicts"
            )
        return ParquetConversionResult(
            batch_id=manifest.batch_id,
            parquet_path=parquet_path,
            lineage_path=lineage_path,
            parquet_sha256=parquet_sha256,
            created=False,
        )


def _update_value_hash(digest: _Digest, values: Sequence[str]) -> None:
    """Hash value boundaries and UTF-8 bytes without ambiguous concatenation."""
    for value in values:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_HASH_BUFFER_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(document: object) -> bytes:
    return (
        json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")
