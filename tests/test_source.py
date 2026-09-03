from __future__ import annotations

import hashlib
import io
import urllib.error
from email.message import Message
from http.client import HTTPResponse
from pathlib import Path
from typing import cast

import pytest

from ingest.models import FeedDefinition
from ingest.source import download_feed


class ByteResponse(io.BytesIO):
    """Context-managed byte stream matching the part of HTTPResponse we use."""

    def __init__(
        self,
        content: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(content)
        self.status = status
        self.headers = Message()
        for name, value in (headers or {}).items():
            self.headers[name] = value

    def __enter__(self) -> ByteResponse:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class InterruptedResponse(ByteResponse):
    """Return one prefix, then simulate a connection loss on the next read."""

    def __init__(self, prefix: bytes, *, headers: dict[str, str] | None = None) -> None:
        super().__init__(b"", headers=headers)
        self._prefix = prefix
        self._reads = 0

    def read(self, size: int | None = -1) -> bytes:
        del size
        self._reads += 1
        if self._reads == 1:
            return self._prefix
        raise urllib.error.URLError("connection reset")


def test_download_counts_logical_csv_records_and_fingerprints_schema(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_csv = b'id,note\n1,"line one\nline two"\n2,plain\n'
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout: ByteResponse(source_csv),
    )

    downloaded = download_feed(
        FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz"
    )

    assert downloaded.row_count == 2
    assert downloaded.columns == ("id", "note")
    assert downloaded.content_sha256 == hashlib.sha256(source_csv).hexdigest()


def test_download_retries_and_resumes_from_the_last_written_byte(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_prefix = b"id,note\n1,first\n"
    source_suffix = b"2,second\n"
    requested_ranges: list[str | None] = []
    requested_validators: list[str | None] = []
    sleeps: list[int] = []

    def open_response(request: object, timeout: int) -> HTTPResponse:
        del timeout
        typed_request = cast("urllib.request.Request", request)
        requested_ranges.append(typed_request.get_header("Range"))
        requested_validators.append(typed_request.get_header("If-range"))
        if len(requested_ranges) == 1:
            return cast(
                HTTPResponse,
                InterruptedResponse(source_prefix, headers={"ETag": '"export-v1"'}),
            )
        return cast(
            HTTPResponse,
            ByteResponse(
                source_suffix,
                status=206,
                headers={
                    "Content-Range": (
                        f"bytes {len(source_prefix)}-"
                        f"{len(source_prefix) + len(source_suffix) - 1}/"
                        f"{len(source_prefix) + len(source_suffix)}"
                    ),
                    "Content-Length": str(len(source_suffix)),
                },
            ),
        )

    monkeypatch.setattr("urllib.request.urlopen", open_response)
    monkeypatch.setattr("ingest.source.time.sleep", sleeps.append)

    downloaded = download_feed(
        FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz"
    )

    assert requested_ranges == [None, f"bytes={len(source_prefix)}-"]
    assert requested_validators == [None, '"export-v1"']
    assert sleeps == [5]
    assert downloaded.row_count == 2
    assert (
        downloaded.content_sha256
        == hashlib.sha256(source_prefix + source_suffix).hexdigest()
    )


def test_download_restarts_safely_when_server_ignores_range(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    complete_source = b"id\n1\n2\n"
    attempts = 0
    requested_ranges: list[str | None] = []
    requested_validators: list[str | None] = []

    def open_response(request: object, timeout: int) -> HTTPResponse:
        nonlocal attempts
        del timeout
        attempts += 1
        typed_request = cast("urllib.request.Request", request)
        requested_ranges.append(typed_request.get_header("Range"))
        requested_validators.append(typed_request.get_header("If-range"))
        if attempts == 1:
            return cast(
                HTTPResponse,
                InterruptedResponse(b"id\n1", headers={"ETag": '"export-v1"'}),
            )
        return cast(
            HTTPResponse,
            ByteResponse(
                complete_source,
                status=200,
                headers={"ETag": '"export-v2"'},
            ),
        )

    monkeypatch.setattr("urllib.request.urlopen", open_response)
    monkeypatch.setattr("ingest.source.time.sleep", lambda seconds: None)

    downloaded = download_feed(
        FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz"
    )

    assert downloaded.row_count == 2
    assert downloaded.content_sha256 == hashlib.sha256(complete_source).hexdigest()
    assert requested_ranges == [None, "bytes=4-"]
    assert requested_validators == [None, '"export-v1"']


def test_download_restarts_without_a_resource_validator(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    complete_source = b"id\n1\n2\n"
    requested_ranges: list[str | None] = []

    def open_response(request: object, timeout: int) -> HTTPResponse:
        del timeout
        typed_request = cast("urllib.request.Request", request)
        requested_ranges.append(typed_request.get_header("Range"))
        if len(requested_ranges) == 1:
            return cast(HTTPResponse, InterruptedResponse(b"id\n1"))
        return cast(HTTPResponse, ByteResponse(complete_source))

    monkeypatch.setattr("urllib.request.urlopen", open_response)
    monkeypatch.setattr("ingest.source.time.sleep", lambda seconds: None)

    downloaded = download_feed(
        FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz"
    )

    assert requested_ranges == [None, None]
    assert downloaded.content_sha256 == hashlib.sha256(complete_source).hexdigest()


def test_download_stops_after_the_configured_attempt_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attempts = 0
    sleeps: list[int] = []

    def unavailable(request: object, timeout: int) -> HTTPResponse:
        nonlocal attempts
        del request, timeout
        attempts += 1
        raise urllib.error.URLError("source unavailable")

    monkeypatch.setattr("urllib.request.urlopen", unavailable)
    monkeypatch.setattr("ingest.source.DOWNLOAD_MAX_ATTEMPTS", 3)
    monkeypatch.setattr("ingest.source.time.sleep", sleeps.append)

    with pytest.raises(urllib.error.URLError, match="source unavailable"):
        download_feed(FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz")

    assert attempts == 3
    assert sleeps == [5, 10]


@pytest.mark.parametrize("source_csv", [b"", b"id,id\n1,2\n"])
def test_download_rejects_an_invalid_csv_header(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source_csv: bytes
) -> None:
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request, timeout: ByteResponse(source_csv),
    )

    with pytest.raises(ValueError):
        download_feed(FeedDefinition("test", "dataset"), tmp_path / "snapshot.csv.gz")
