"""Pure parsing of S3 event notifications delivered through SQS."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast
from urllib.parse import unquote_plus


@dataclass(frozen=True)
class ManifestReference:
    """Immutable location of a committed snapshot manifest in S3."""

    bucket: str
    key: str
    version_id: str | None = None


def parse_s3_manifest_message(body: str) -> tuple[ManifestReference, ...]:
    """Return unique manifest references from one S3 notification.

    S3 test events intentionally produce no work. Malformed notifications and
    non-manifest object events fail explicitly so they can be retried or moved to
    the queue's dead-letter queue instead of being silently acknowledged.
    """
    document = _decode_document(body)
    message = document.get("Message")
    if isinstance(message, str):
        document = _decode_document(message)
        if "Message" in document:
            raise ValueError("SQS notification contains nested SNS envelopes")
    if document.get("Event") == "s3:TestEvent":
        return ()

    records = document.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("S3 notification must contain a non-empty Records list")

    references: list[ManifestReference] = []
    seen: set[ManifestReference] = set()
    for position, record in enumerate(records):
        reference = _parse_record(record, position)
        if reference not in seen:
            references.append(reference)
            seen.add(reference)
    return tuple(references)


def _decode_document(body: str) -> dict[str, object]:
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError("SQS message body is not valid JSON") from error
    if not isinstance(document, dict):
        raise ValueError("SQS message body must be a JSON object")

    return cast(dict[str, object], document)


def _parse_record(record: object, position: int) -> ManifestReference:
    if not isinstance(record, dict):
        raise ValueError(f"S3 notification record {position} must be an object")
    event_name = record.get("eventName")
    if not isinstance(event_name, str) or "ObjectCreated:" not in event_name:
        raise ValueError(f"S3 notification record {position} is not an object creation")

    try:
        s3 = record["s3"]
        bucket = s3["bucket"]["name"]
        object_details = s3["object"]
        encoded_key = object_details["key"]
        version_id = object_details.get("versionId")
    except (KeyError, TypeError) as error:
        raise ValueError(
            f"S3 notification record {position} has no bucket or object key"
        ) from error

    if not isinstance(bucket, str) or not isinstance(encoded_key, str):
        raise ValueError(
            f"S3 notification record {position} has invalid bucket or key values"
        )
    if version_id is not None and not isinstance(version_id, str):
        raise ValueError(f"S3 notification record {position} has invalid versionId")

    key = unquote_plus(encoded_key)
    if not key.endswith("/manifest.json"):
        raise ValueError(f"S3 notification record {position} is not a manifest")
    return ManifestReference(bucket=bucket, key=key, version_id=version_id)
