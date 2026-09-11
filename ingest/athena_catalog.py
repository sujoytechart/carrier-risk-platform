"""Validate immutable raw snapshots before publishing Athena catalog indexes."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from io import BufferedReader, TextIOWrapper
from typing import TYPE_CHECKING, Protocol

from botocore.exceptions import ClientError

from ingest.contracts import FeedSchema, validate_manifest
from ingest.models import SnapshotManifest
from ingest.object_store import ContentIntegrityStream, SnapshotObjectStore

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


class CatalogStore(Protocol):
    """Read commit manifests and conditionally publish immutable catalog files."""

    def read_object(self, key: str) -> bytes | None:
        """Return an object's bytes, or ``None`` when the key does not exist."""

    def write_object_if_absent(
        self, key: str, content: bytes, *, content_type: str
    ) -> bool:
        """Create an object atomically, returning whether this call created it."""


class S3CatalogStore:
    """S3 adapter for manifest reads and immutable catalog document writes."""

    def __init__(self, bucket: str, client: S3Client) -> None:
        """Bind catalog operations to one bucket and configured S3 client."""
        self._bucket = bucket
        self._client = client

    def read_object(self, key: str) -> bytes | None:
        """Read a small document, returning ``None`` only for a missing key."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            if _client_error_code(error) in {"404", "NoSuchKey"}:
                return None
            raise

        body = response["Body"]
        try:
            return body.read()
        finally:
            body.close()

    def write_object_if_absent(
        self, key: str, content: bytes, *, content_type: str
    ) -> bool:
        """Use S3 conditional creation so concurrent publishers cannot overwrite."""
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                IfNoneMatch="*",
            )
        except ClientError as error:
            if _client_error_code(error) in {"412", "PreconditionFailed"}:
                return False
            raise
        return True


def _client_error_code(error: ClientError) -> object:
    """Return the service error code without treating other failures as absence."""
    return error.response.get("Error", {}).get("Code")


@dataclass(frozen=True)
class CatalogLocation:
    """Deterministic raw input and catalog output keys for one feed partition."""

    manifest_key: str
    pointer_key: str
    metadata_key: str


@dataclass(frozen=True)
class CatalogPublication:
    """Describe the immutable Athena catalog files for a validated snapshot."""

    batch_id: str
    pointer_key: str
    metadata_key: str
    created: bool


@dataclass(frozen=True)
class CatalogMetadata:
    """Compact snapshot lineage exposed to Athena alongside raw source rows."""

    batch_id: str
    feed: str
    observed_at: str
    content_sha256: str
    object_sha256: str
    row_count: int
    schema_fingerprint: str

    @classmethod
    def from_manifest(cls, manifest: SnapshotManifest) -> CatalogMetadata:
        """Select only stable reconciliation and lineage fields."""
        return cls(
            batch_id=manifest.batch_id,
            feed=manifest.feed_name,
            observed_at=manifest.observed_at,
            content_sha256=manifest.content_sha256,
            object_sha256=manifest.object_sha256,
            row_count=manifest.row_count,
            schema_fingerprint=manifest.schema_fingerprint,
        )

    def to_jsonl(self) -> bytes:
        """Return one deterministic JSON Lines record."""
        return (
            json.dumps(asdict(self), separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")


class RawCatalogPublisher:
    """Publish Athena-visible indexes only after complete raw validation.

    The pointer is written last because it makes the raw object visible to the
    symlink-backed Athena table. All object access is injected so validation and
    idempotency can be tested without cloud access.
    """

    def __init__(
        self,
        *,
        bucket: str,
        catalog_store: CatalogStore,
        snapshot_store: SnapshotObjectStore,
        schemas: Mapping[str, FeedSchema] | None = None,
    ) -> None:
        """Bind the publisher to one raw bucket and its storage boundaries."""
        if not bucket or any(character in bucket for character in "/\r\n"):
            raise ValueError("bucket must be a non-empty S3 bucket name")
        self._bucket = bucket
        self._catalog_store = catalog_store
        self._snapshot_store = snapshot_store
        self._schemas = schemas

    @staticmethod
    def location_for(feed_name: str, acquisition_date: str) -> CatalogLocation:
        """Return canonical keys after validating the feed and UTC date partition."""
        FeedSchema.load_configured(feed_name)
        try:
            parsed_date = date.fromisoformat(acquisition_date)
        except ValueError as error:
            raise ValueError("acquisition_date must use YYYY-MM-DD") from error
        if parsed_date.isoformat() != acquisition_date:
            raise ValueError("acquisition_date must use YYYY-MM-DD")

        raw_prefix = f"raw/feed={feed_name}/acquisition_date={acquisition_date}"
        catalog_prefix = f"catalog/feed={feed_name}/acquisition_date={acquisition_date}"
        metadata_prefix = (
            f"catalog-metadata/feed={feed_name}/acquisition_date={acquisition_date}"
        )
        return CatalogLocation(
            manifest_key=f"{raw_prefix}/manifest.json",
            pointer_key=f"{catalog_prefix}/snapshot.txt",
            metadata_key=f"{metadata_prefix}/manifest.jsonl",
        )

    def publish(self, feed_name: str, acquisition_date: str) -> CatalogPublication:
        """Validate and idempotently expose one completed snapshot to Athena.

        Missing manifests, drifted schemas, corrupt bytes, incompatible CSV, and
        conflicting prior publications fail before a new catalog object is made
        visible. Matching prior content is an idempotent no-op.
        """
        location = self.location_for(feed_name, acquisition_date)
        manifest_document = self._catalog_store.read_object(location.manifest_key)
        if manifest_document is None:
            raise FileNotFoundError(
                f"No completed manifest exists at {location.manifest_key}"
            )

        manifest = SnapshotManifest.from_json(manifest_document)
        schema = self._schema_for(feed_name)
        validate_manifest(manifest, schema)
        manifest_acquisition_date = (
            datetime.fromisoformat(manifest.observed_at)
            .astimezone(UTC)
            .date()
            .isoformat()
        )
        if manifest_acquisition_date != acquisition_date:
            raise ValueError(
                f"Manifest observation date {manifest_acquisition_date} does not "
                f"match requested partition {acquisition_date}"
            )
        self._validate_snapshot_csv(manifest, schema)

        metadata = CatalogMetadata.from_manifest(manifest).to_jsonl()
        pointer = f"s3://{self._bucket}/{manifest.object_key}\n".encode()
        self._require_compatible_existing(
            location.metadata_key, metadata, document_name="metadata"
        )
        self._require_compatible_existing(
            location.pointer_key, pointer, document_name="pointer"
        )

        metadata_created = self._publish_exact(
            location.metadata_key,
            metadata,
            content_type="application/x-ndjson",
            document_name="metadata",
        )
        pointer_created = self._publish_exact(
            location.pointer_key,
            pointer,
            content_type="text/plain; charset=utf-8",
            document_name="pointer",
        )
        return CatalogPublication(
            batch_id=manifest.batch_id,
            pointer_key=location.pointer_key,
            metadata_key=location.metadata_key,
            created=metadata_created or pointer_created,
        )

    def _schema_for(self, feed_name: str) -> FeedSchema:
        if self._schemas is None:
            return FeedSchema.load_configured(feed_name)
        try:
            return self._schemas[feed_name]
        except KeyError as error:
            raise ValueError(f"No raw schema configured for {feed_name!r}") from error

    def _validate_snapshot_csv(
        self, manifest: SnapshotManifest, schema: FeedSchema
    ) -> None:
        row_count = 0
        with (
            self._snapshot_store.open_snapshot(manifest) as object_stream,
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
            reader = csv.reader(self._opencsv_compatible_lines(text), strict=True)
            try:
                header = next(reader)
            except StopIteration as error:
                raise ValueError("Snapshot contained no CSV header") from error
            except csv.Error as error:
                if reader.line_num > 1:
                    self._reject_multiline_record(
                        previous_line=0,
                        current_line=reader.line_num,
                        record_name="CSV header",
                    )
                raise ValueError(
                    f"Snapshot CSV header parsing failed: {error}"
                ) from error
            self._reject_multiline_record(
                previous_line=0,
                current_line=reader.line_num,
                record_name="CSV header",
            )
            if tuple(header) != schema.columns:
                raise ValueError(
                    "Snapshot CSV header names or order do not match the configured "
                    "feed schema"
                )

            previous_line = reader.line_num
            while True:
                try:
                    row = next(reader)
                except StopIteration:
                    break
                except csv.Error as error:
                    if reader.line_num > previous_line + 1:
                        self._reject_multiline_record(
                            previous_line=previous_line,
                            current_line=reader.line_num,
                            record_name=f"Source row {row_count + 1}",
                        )
                    raise ValueError(
                        "Snapshot CSV parsing failed near source row "
                        f"{row_count + 1}: {error}"
                    ) from error
                row_count += 1
                self._reject_multiline_record(
                    previous_line=previous_line,
                    current_line=reader.line_num,
                    record_name=f"Source row {row_count}",
                )
                previous_line = reader.line_num
                if len(row) != len(schema.columns):
                    raise ValueError(
                        f"Source row {row_count} has {len(row)} columns; "
                        f"expected {len(schema.columns)}"
                    )
            verified_content.verify_complete()

        if row_count != manifest.row_count:
            raise ValueError(
                "Snapshot row count does not match its manifest: "
                f"expected={manifest.row_count}, actual={row_count}"
            )

    @staticmethod
    def _reject_multiline_record(
        *, previous_line: int, current_line: int, record_name: str
    ) -> None:
        if current_line != previous_line + 1:
            raise ValueError(
                f"{record_name} contains embedded CR/LF and cannot be read by "
                "Athena OpenCSVSerde"
            )

    @classmethod
    def _opencsv_compatible_lines(cls, text: TextIOWrapper) -> Iterator[str]:
        """Yield physical CSV lines after rejecting OpenCSV dialect ambiguities."""
        for physical_line_number, line in enumerate(text, start=1):
            cls._validate_opencsv_line(line, physical_line_number)
            yield line

    @staticmethod
    def _validate_opencsv_line(line: str, physical_line_number: int) -> None:
        """Require syntax decoded identically by Python CSV and OpenCSVSerde.

        OpenCSV consumes backslash as an escape marker while Python's default CSV
        dialect preserves it. OpenCSV also lets a quote within unquoted text change
        quote state. Both constructs are rejected so a snapshot cannot validate
        under one parser and shift columns or source values under Athena.
        """
        if line.endswith("\r\n"):
            content = line[:-2]
            has_line_ending = True
        elif line.endswith(("\r", "\n")):
            content = line[:-1]
            has_line_ending = True
        else:
            content = line
            has_line_ending = False

        in_quotes = False
        after_closing_quote = False
        at_field_start = True
        index = 0
        while index < len(content):
            character = content[index]
            next_character = content[index + 1] if index + 1 < len(content) else None

            if character == "\\":
                record_name = _record_name_for_physical_line(physical_line_number)
                raise ValueError(
                    f"{record_name} contains a backslash that Athena OpenCSVSerde "
                    "removes or reinterprets"
                )

            if in_quotes:
                if character == '"':
                    if next_character == '"':
                        index += 2
                        continue
                    in_quotes = False
                    after_closing_quote = True
                index += 1
                continue

            if after_closing_quote:
                if character != ",":
                    record_name = _record_name_for_physical_line(physical_line_number)
                    raise ValueError(
                        f"{record_name} has characters after a closing quote and "
                        "is incompatible with Athena OpenCSVSerde"
                    )
                after_closing_quote = False
                at_field_start = True
                index += 1
                continue

            if at_field_start:
                if character == '"':
                    in_quotes = True
                    at_field_start = False
                elif character != ",":
                    at_field_start = False
                index += 1
                continue

            if character == ",":
                at_field_start = True
            elif character == '"':
                record_name = _record_name_for_physical_line(physical_line_number)
                raise ValueError(
                    f"{record_name} contains a quote inside unquoted text that "
                    "changes state in Athena OpenCSVSerde"
                )
            index += 1

        if in_quotes and has_line_ending:
            record_name = _record_name_for_physical_line(physical_line_number)
            raise ValueError(
                f"{record_name} contains embedded CR/LF and cannot be read by "
                "Athena OpenCSVSerde"
            )

    def _require_compatible_existing(
        self, key: str, expected: bytes, *, document_name: str
    ) -> None:
        existing = self._catalog_store.read_object(key)
        if existing is not None and existing != expected:
            raise RuntimeError(f"Conflicting {document_name} already exists at {key}")

    def _publish_exact(
        self,
        key: str,
        content: bytes,
        *,
        content_type: str,
        document_name: str,
    ) -> bool:
        created = self._catalog_store.write_object_if_absent(
            key, content, content_type=content_type
        )
        if created:
            return True

        existing = self._catalog_store.read_object(key)
        if existing != content:
            raise RuntimeError(
                f"Conflicting {document_name} won conditional publication at {key}"
            )
        return False


def _record_name_for_physical_line(physical_line_number: int) -> str:
    """Map a physical line to the header or source row named in diagnostics."""
    if physical_line_number == 1:
        return "CSV header"
    return f"Source row {physical_line_number - 1}"
