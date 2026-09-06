"""Transactional loading of complete immutable snapshots into PostgreSQL."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from io import BufferedReader, TextIOWrapper

from ingest.contracts import FeedSchema, validate_manifest
from ingest.database import (
    DatabaseConnection,
    initialize_raw_storage,
    raw_copy_statement,
)
from ingest.models import SnapshotManifest
from ingest.object_store import ContentIntegrityStream, SnapshotObjectStore

ConnectionFactory = Callable[[], DatabaseConnection]


@dataclass(frozen=True)
class RawLoadResult:
    """Observable outcome of one idempotent raw batch load."""

    batch_id: str
    inserted_rows: int
    already_loaded: bool


class RawSnapshotLoader:
    """Stream complete snapshots into raw tables in one database transaction."""

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        object_store: SnapshotObjectStore,
        schemas: Mapping[str, FeedSchema] | None = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._object_store = object_store
        self._schemas = schemas

    def load(self, manifest: SnapshotManifest) -> RawLoadResult:
        """Load one batch, or return a no-op result when it already committed.

        Raw relations are initialized in a short, separately committed transaction.
        The batch marker, copied rows, and final reconciliation share the following
        transaction, so any exception leaves no partial batch effects.
        """
        schema = self._schema_for(manifest.feed_name)
        validate_manifest(manifest, schema)

        with self._connection_factory() as initialization_connection:
            initialize_raw_storage(initialization_connection, schema)

        with self._connection_factory() as connection:
            self._lock_batch(connection, manifest.batch_id)
            if self._is_loaded(connection, manifest):
                return RawLoadResult(manifest.batch_id, 0, True)

            self._insert_loading_batch(connection, manifest)
            inserted_rows = self._copy_rows(connection, manifest, schema)
            if inserted_rows != manifest.row_count:
                raise ValueError(
                    f"Snapshot row count mismatch: manifest={manifest.row_count}, "
                    f"copied={inserted_rows}"
                )
            connection.execute(
                """
                update raw.snapshot_batches
                   set status = 'loaded', loaded_at = current_timestamp
                 where batch_id = %s
                """,
                (manifest.batch_id,),
            )

        return RawLoadResult(manifest.batch_id, inserted_rows, False)

    def _schema_for(self, feed_name: str) -> FeedSchema:
        if self._schemas is None:
            return FeedSchema.load_configured(feed_name)
        try:
            return self._schemas[feed_name]
        except KeyError as error:
            raise ValueError(f"No raw schema configured for {feed_name!r}") from error

    @staticmethod
    def _lock_batch(connection: DatabaseConnection, batch_id: str) -> None:
        connection.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (batch_id,),
        )

    @staticmethod
    def _is_loaded(
        connection: DatabaseConnection,
        manifest: SnapshotManifest,
    ) -> bool:
        row = connection.execute(
            "select status, source_url from raw.snapshot_batches where batch_id = %s",
            (manifest.batch_id,),
        ).fetchone()
        if row is not None and row[1] != manifest.source_url:
            raise ValueError(
                "Loaded batch has conflicting source_url lineage for "
                f"batch_id {manifest.batch_id}"
            )
        return row is not None and row[0] == "loaded"

    @staticmethod
    def _insert_loading_batch(
        connection: DatabaseConnection,
        manifest: SnapshotManifest,
    ) -> None:
        connection.execute(
            """
            insert into raw.snapshot_batches (
                batch_id, feed_name, dataset_id, source_url, observed_at, object_key,
                row_count, uncompressed_bytes, compressed_bytes,
                content_sha256, object_sha256, schema_fingerprint,
                source_columns, status
            ) values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'loading'
            )
            """,
            (
                manifest.batch_id,
                manifest.feed_name,
                manifest.dataset_id,
                manifest.source_url,
                datetime.fromisoformat(manifest.observed_at),
                manifest.object_key,
                manifest.row_count,
                manifest.uncompressed_bytes,
                manifest.compressed_bytes,
                manifest.content_sha256,
                manifest.object_sha256,
                manifest.schema_fingerprint,
                json.dumps(manifest.columns),
            ),
        )

    def _copy_rows(
        self,
        connection: DatabaseConnection,
        manifest: SnapshotManifest,
        schema: FeedSchema,
    ) -> int:
        inserted_rows = 0
        with (
            self._object_store.open_snapshot(manifest) as object_stream,
            gzip.GzipFile(fileobj=object_stream, mode="rb") as archive,
            ContentIntegrityStream(
                archive,
                expected_bytes=manifest.uncompressed_bytes,
                expected_sha256=manifest.content_sha256,
                content_name="Snapshot content",
            ) as verified_content,
            BufferedReader(verified_content) as buffered_content,
            TextIOWrapper(buffered_content, encoding="utf-8-sig", newline="") as text,
        ):
            reader = csv.reader(text)
            header = tuple(next(reader))
            if header != schema.columns:
                raise ValueError("Snapshot CSV header does not match its manifest")

            with connection.cursor().copy(raw_copy_statement(schema)) as copy:
                for source_row_number, row in enumerate(reader, start=1):
                    if len(row) != len(schema.columns):
                        raise ValueError(
                            f"Source row {source_row_number} has {len(row)} "
                            f"columns; expected {len(schema.columns)}"
                        )
                    copy.write_row((manifest.batch_id, source_row_number, *row))
                    inserted_rows += 1
            verified_content.verify_complete()
        return inserted_rows
