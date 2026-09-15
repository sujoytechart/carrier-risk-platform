"""Fingerprint failures and source exclusions are observable extraction behavior."""

import hashlib
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ml.demo_source import INSPECTION_COLUMNS, extract_demo_sources
from ml.source_records import CRASH_COLUMNS


def write_source(root: Path, feed: str, rows: list[dict[str, str]]) -> None:
    directory = root / feed
    directory.mkdir()
    path = directory / "snapshot.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    (directory / "lineage.json").write_text(
        json.dumps(
            {
                "parquet_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "observed_at": "2026-09-03T00:15:00+00:00",
                "reconciliation": {
                    "source_value_sha256": "same",
                    "parquet_value_sha256": "same",
                },
            }
        )
    )


def test_streaming_extraction_excludes_invalid_and_not_yet_visible_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inspection = dict.fromkeys(INSPECTION_COLUMNS, "") | {
        "INSPECTION_ID": "12",
        "DOT_NUMBER": "123",
        "INSP_DATE": "20240101",
        "MCMIS_ADD_DATE": "20240102 1200",
        "VIOL_TOTAL": "2",
        "OOS_TOTAL": "1",
    }
    crash = dict.fromkeys(CRASH_COLUMNS, "") | {
        "DOT_NUMBER": "123",
        "CRASH_ID": "11",
        "REPORT_DATE": "20240401",
        "ADD_DATE": "20240403 1200",
        "REPORT_STATE": "MI",
        "REPORT_NUMBER": "a",
        "REPORT_TIME": "1200",
        "FEDERAL_RECORDABLE": "Y",
    }
    write_source(
        tmp_path, "inspections", [inspection, inspection | {"VIOL_TOTAL": "bad"}]
    )
    write_source(tmp_path, "crashes", [crash, crash | {"ADD_DATE": "20260903 1200"}])
    connection = MagicMock()
    monkeypatch.setattr("ml.demo_source.psycopg.connect", lambda _: connection)
    cursor = connection.__enter__.return_value.cursor.return_value
    copy = cursor.copy.return_value.__enter__.return_value
    result = extract_demo_sources(tmp_path, "unused", data_as_of=date(2026, 9, 3))
    assert result["selected_rows"] == {
        "inspections": 1,
        "crashes": 1,
        "crashes_not_visible_at_acquisition": 1,
    }
    assert result["parse_exclusions"] == {"inspections:invalid_inspection_integer": 1}
    assert copy.write_row.call_count == 2


def test_corrupt_source_is_rejected_before_database_mutation(tmp_path: Path) -> None:
    write_source(tmp_path, "inspections", [{"x": "a"}])
    path = tmp_path / "inspections" / "snapshot.parquet"
    path.write_bytes(path.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        extract_demo_sources(tmp_path, "unused", data_as_of=date(2026, 9, 3))


def test_failed_value_reconciliation_is_rejected(tmp_path: Path) -> None:
    write_source(tmp_path, "inspections", [{"x": "a"}])
    path = tmp_path / "inspections" / "lineage.json"
    lineage = json.loads(path.read_text())
    lineage["reconciliation"]["parquet_value_sha256"] = "different"
    path.write_text(json.dumps(lineage))
    with pytest.raises(ValueError, match="reconciliation mismatch"):
        extract_demo_sources(tmp_path, "unused", data_as_of=date(2026, 9, 3))


def test_wrong_acquisition_date_rejected_before_database_mutation(
    tmp_path: Path,
) -> None:
    write_source(tmp_path, "inspections", [{"x": "a"}])
    path = tmp_path / "inspections" / "lineage.json"
    lineage = json.loads(path.read_text())
    lineage["observed_at"] = "2026-10-03T00:15:00+00:00"
    path.write_text(json.dumps(lineage))
    with pytest.raises(ValueError, match="acquisition date"):
        extract_demo_sources(tmp_path, "unused", data_as_of=date(2026, 9, 3))
