from __future__ import annotations

import json

import pytest

from orchestration.messages import ManifestReference, parse_s3_manifest_message


def test_parses_manifest_records_and_decodes_object_keys() -> None:
    body = json.dumps(
        {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": "raw-bucket"},
                        "object": {
                            "key": (
                                "raw%2Ffeed%3Dcrashes%2F"
                                "acquisition_date%3D2026-09-04%2Fmanifest.json"
                            ),
                            "versionId": "version-1",
                        },
                    },
                }
            ]
        }
    )

    assert parse_s3_manifest_message(body) == (
        ManifestReference(
            bucket="raw-bucket",
            key="raw/feed=crashes/acquisition_date=2026-09-04/manifest.json",
            version_id="version-1",
        ),
    )


def test_test_event_contains_no_manifest_references() -> None:
    body = json.dumps({"Event": "s3:TestEvent"})

    assert parse_s3_manifest_message(body) == ()


def test_accepts_one_sns_envelope_and_rejects_nested_envelopes() -> None:
    envelope = json.dumps({"Message": json.dumps({"Event": "s3:TestEvent"})})

    assert parse_s3_manifest_message(envelope) == ()
    with pytest.raises(ValueError, match="nested"):
        parse_s3_manifest_message(json.dumps({"Message": envelope}))


def test_empty_record_list_is_not_acknowledged_as_success() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        parse_s3_manifest_message('{"Records":[]}')


def test_duplicate_records_are_collapsed_in_source_order() -> None:
    record = {
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": "raw-bucket"},
            "object": {"key": "raw%2Ffeed%3Dcrashes%2Fmanifest.json"},
        },
    }
    body = json.dumps({"Records": [record, record]})

    assert parse_s3_manifest_message(body) == (
        ManifestReference(
            bucket="raw-bucket",
            key="raw/feed=crashes/manifest.json",
        ),
    )


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        json.dumps({"Records": "not-a-list"}),
        json.dumps({"Records": [{}]}),
        json.dumps(
            {
                "Records": [
                    {
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "raw-bucket"},
                            "object": {"key": "raw/snapshot.csv.gz"},
                        },
                    }
                ]
            }
        ),
    ],
)
def test_rejects_malformed_or_non_manifest_messages(body: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_manifest_message(body)
