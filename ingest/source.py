"""Download and inspect complete FMCSA Socrata exports."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import time
import urllib.error
import urllib.request
from http.client import HTTPResponse, IncompleteRead, RemoteDisconnected
from pathlib import Path
from typing import cast

from ingest.models import DownloadedSnapshot, FeedDefinition

BUFFER_BYTES = 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 120
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BACKOFF_BASE_SECONDS = 5
DOWNLOAD_BACKOFF_CAP_SECONDS = 60
RETRYABLE_HTTP_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class IncompleteDownloadError(OSError):
    """Signal that a response ended before its declared byte count arrived."""


def _file_sha256(path: Path) -> str:
    """Hash a file without loading a multi-gigabyte snapshot into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(BUFFER_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _compress_file(source: Path, destination: Path) -> tuple[str, int]:
    """Compress a complete source file while hashing its original bytes.

    Gzip's timestamp and filename fields are fixed so identical source exports
    produce identical objects. That makes checksum comparison meaningful during
    recovery from an upload that finished before its manifest was written.
    """
    content_digest = hashlib.sha256()
    uncompressed_bytes = 0

    with (
        source.open("rb") as source_file,
        destination.open("wb") as raw_destination,
        gzip.GzipFile(
            fileobj=raw_destination,
            mode="wb",
            filename="",
            mtime=0,
        ) as compressed_destination,
    ):
        while chunk := source_file.read(BUFFER_BYTES):
            compressed_destination.write(chunk)
            content_digest.update(chunk)
            uncompressed_bytes += len(chunk)

    return content_digest.hexdigest(), uncompressed_bytes


def _validated_append_mode(response: HTTPResponse, offset: int) -> str:
    """Choose append or restart without ever duplicating partial response bytes.

    Continuation is safe only when the server returns HTTP 206 and confirms that
    its content range begins at the existing file size. Some servers ignore a Range
    header and return HTTP 200; in that case the partial file must be replaced.
    """
    if offset == 0 or response.status != 206:
        return "wb"

    content_range = response.headers.get("Content-Range", "")
    expected_prefix = f"bytes {offset}-"
    if not content_range.startswith(expected_prefix):
        raise ValueError(
            "Partial response did not begin at the requested byte offset: "
            f"expected {expected_prefix!r}, received {content_range!r}"
        )
    return "ab"


def _if_range_validator(response: HTTPResponse) -> str | None:
    """Return a validator that makes a subsequent range request safe.

    ``If-Range`` requires a strong ETag; weak ETags cannot prove byte-for-byte
    identity. ``Last-Modified`` is the standards-defined fallback when the
    endpoint does not provide a strong ETag.
    """
    etag = response.headers.get("ETag")
    if etag is not None and not etag.strip().startswith("W/"):
        return etag
    return response.headers.get("Last-Modified")


def _stream_response_to_file(response: HTTPResponse, partial: Path, mode: str) -> None:
    """Persist one HTTP response and reject a clean but prematurely short EOF."""
    response_bytes = 0
    with partial.open(mode) as destination:
        while chunk := response.read(BUFFER_BYTES):
            destination.write(chunk)
            response_bytes += len(chunk)

    content_length = response.headers.get("Content-Length")
    if content_length is not None and response_bytes != int(content_length):
        raise IncompleteDownloadError(
            f"response declared {content_length} bytes but delivered {response_bytes}"
        )


def _download_with_retries(feed: FeedDefinition, partial: Path) -> None:
    """Download a feed with bounded retries and range-based continuation.

    Bytes written before a retryable connection failure remain in ``partial``.
    The next attempt requests only the missing suffix. If the endpoint does not
    honor ranges, the response replaces the partial file and still produces a
    correct snapshot, at the cost of restarting that attempt from byte zero.
    """
    if_range: str | None = None

    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        partial_size = partial.stat().st_size if partial.exists() else 0
        offset = partial_size if if_range is not None else 0
        headers = {
            "Accept": "text/csv",
            "Accept-Encoding": "identity",
        }
        if offset and if_range is not None:
            headers["Range"] = f"bytes={offset}-"
            headers["If-Range"] = if_range
        request = urllib.request.Request(feed.source_url, headers=headers)

        try:
            with urllib.request.urlopen(
                request, timeout=DOWNLOAD_TIMEOUT_SECONDS
            ) as raw_response:
                response = cast(HTTPResponse, raw_response)
                mode = _validated_append_mode(response, offset)
                if mode == "wb":
                    if_range = _if_range_validator(response)
                _stream_response_to_file(response, partial, mode)
            return
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_HTTP_STATUS_CODES:
                raise
            retryable_error: Exception = error
        except (
            IncompleteDownloadError,
            IncompleteRead,
            RemoteDisconnected,
            TimeoutError,
            ConnectionError,
            urllib.error.URLError,
        ) as error:
            retryable_error = error

        if attempt == DOWNLOAD_MAX_ATTEMPTS:
            raise retryable_error

        backoff_seconds = min(
            DOWNLOAD_BACKOFF_CAP_SECONDS,
            DOWNLOAD_BACKOFF_BASE_SECONDS * 2 ** (attempt - 1),
        )
        time.sleep(backoff_seconds)


def _inspect_csv(archive: Path, feed_name: str) -> tuple[tuple[str, ...], int]:
    """Return the header and logical record count from a compressed CSV.

    ``csv.reader`` is intentional: counting newline bytes would overcount records
    whenever a quoted text field contains an embedded newline.
    """
    with gzip.open(archive, mode="rt", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        try:
            columns = tuple(next(reader))
        except StopIteration as error:
            raise ValueError(f"{feed_name} download contained no CSV header") from error
        row_count = sum(1 for _ in reader)

    if len(columns) != len(set(columns)):
        raise ValueError(f"{feed_name} download contains duplicate column names")
    return columns, row_count


def download_feed(feed: FeedDefinition, destination: Path) -> DownloadedSnapshot:
    """Acquire, deterministically compress, and inspect one complete FMCSA feed.

    The function performs no cloud writes. Keeping source acquisition independent
    from storage lets the same operation run from the command line today and an
    Airflow task later without duplicating download or validation logic.
    """
    partial = destination.with_name(f"{destination.name}.part")
    _download_with_retries(feed, partial)
    content_sha256, uncompressed_bytes = _compress_file(partial, destination)
    partial.unlink()

    columns, row_count = _inspect_csv(destination, feed.name)
    schema_document = json.dumps(columns, separators=(",", ":")).encode()
    return DownloadedSnapshot(
        row_count=row_count,
        uncompressed_bytes=uncompressed_bytes,
        compressed_bytes=destination.stat().st_size,
        content_sha256=content_sha256,
        object_sha256=_file_sha256(destination),
        schema_fingerprint=hashlib.sha256(schema_document).hexdigest(),
        columns=columns,
    )
