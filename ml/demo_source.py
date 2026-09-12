"""Bounded source extraction for the snapshot-ascertained learning demo.

This isolated table is not canonical event history. It contains current retained
source versions and source-add availability proxies, with that limitation carried
through training metadata. Original raw and modeled relations are untouched.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pyarrow.parquet as parquet

from ml.maturity_cli import COLUMNS, _report, _source_add, _source_date

Event = tuple[str, str, str, date, date, int, int, bool]
INSPECTION_COLUMNS = [
    "INSPECTION_ID",
    "DOT_NUMBER",
    "INSP_DATE",
    "MCMIS_ADD_DATE",
    "CHANGE_DATE",
    "VIOL_TOTAL",
    "OOS_TOTAL",
]


def _integer(value: str) -> int:
    if not re.fullmatch(r"[0-9]+(?:\.0+)?", value.strip()):
        raise ValueError("invalid_inspection_integer")
    return int(value.strip().split(".")[0])


def inspection_event(row: dict[str, str]) -> Event:
    """Validate one retained inspection; source-add plus one day is a proxy."""
    key, carrier = _integer(row["INSPECTION_ID"]), _integer(row["DOT_NUMBER"])
    if key <= 0 or carrier <= 0:
        raise ValueError("invalid_inspection_identity")
    event = _source_date(row["INSP_DATE"].strip())
    reported = (_source_add(row["MCMIS_ADD_DATE"].strip()) + timedelta(days=1)).date()
    if reported < event:
        raise ValueError("impossible_event_chronology")
    if row["CHANGE_DATE"].strip():
        _source_add(row["CHANGE_DATE"].strip())
    return (
        "inspection",
        str(key),
        str(carrier),
        event,
        reported,
        _integer(row["VIOL_TOTAL"]),
        _integer(row["OOS_TOTAL"]),
        False,
    )


def crash_event(row: dict[str, str]) -> Event:
    """Use the same stable carrier/incident identity as the maturity screen."""
    report = _report(row)
    if report.usdot_number is None:
        raise ValueError("invalid_usdot_number")
    key = json.dumps(
        [
            report.report_state,
            report.report_number,
            report.event_date.isoformat(),
            report.report_time,
        ],
        separators=(",", ":"),
    )
    return (
        "crash",
        key,
        report.usdot_number,
        report.event_date,
        report.reported_date,
        0,
        0,
        report.federal_recordable,
    )


def extract_demo_sources(
    derived_root: Path, database_url: str, *, data_as_of: date
) -> dict[str, Any]:
    """Atomically replace isolated demo events after verifying both file hashes.

    Streaming COPY bounds memory. A transaction-scoped lock prevents concurrent
    extraction. The enclosing runner serializes demo steps; building features
    separately acquires the existing dbt publication lock.
    Non-selected records are omitted, rather than counted as parse exclusions.
    """
    fingerprints: dict[str, str] = {}
    for feed in ("inspections", "crashes"):
        path = derived_root / feed / "snapshot.parquet"
        expected = json.loads((path.parent / "lineage.json").read_text())
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected["parquet_sha256"]:
            raise ValueError(f"{feed} Parquet fingerprint mismatch")
        observed = datetime.fromisoformat(expected["observed_at"])
        if observed.tzinfo is None or observed.astimezone(UTC).date() != data_as_of:
            raise ValueError(f"{feed} acquisition date does not match the experiment")
        reconciliation = expected["reconciliation"]
        if (
            reconciliation["source_value_sha256"]
            != reconciliation["parquet_value_sha256"]
        ):
            raise ValueError(f"{feed} source reconciliation mismatch")
        fingerprints[feed] = digest.hexdigest()
    counts: Counter[str] = Counter()
    exclusions: Counter[str] = Counter()
    with psycopg.connect(database_url) as connection:
        connection.execute("select pg_advisory_xact_lock(764301235)")
        connection.execute("create schema if not exists learning_demo")
        connection.execute("""
            create table if not exists learning_demo.source_events (
                event_type text not null, source_key text not null,
                usdot_number text not null, event_date date not null,
                reported_date date not null, violation_count bigint not null,
                oos_violation_count bigint not null, federal_recordable boolean not null
            )
        """)
        connection.execute("truncate learning_demo.source_events")
        with connection.cursor().copy(
            "copy learning_demo.source_events from stdin"
        ) as copy:
            for feed, columns, parser in (
                ("inspections", INSPECTION_COLUMNS, inspection_event),
                ("crashes", COLUMNS, crash_event),
            ):
                source = parquet.ParquetFile(derived_root / feed / "snapshot.parquet")
                for batch in source.iter_batches(batch_size=32768, columns=columns):
                    for row in batch.to_pylist():
                        # The demo needs only historical experiment features and
                        # acquisition-month features, plus the six-month labels.
                        raw_date = row[
                            "INSP_DATE" if feed == "inspections" else "REPORT_DATE"
                        ].strip()
                        if feed == "inspections":
                            selected = (
                                "20231001" <= raw_date < "20240201"
                                or "20240501" <= raw_date < "20240901"
                                or "20260501" <= raw_date < "20260901"
                            )
                        else:
                            selected = "20220201" <= raw_date < "20260901"
                        if not selected:
                            continue
                        try:
                            event = parser(row)
                        except ValueError as error:
                            exclusions[f"{feed}:{error}"] += 1
                            continue
                        if event[4] >= data_as_of:
                            counts[f"{feed}_not_visible_at_acquisition"] += 1
                            continue
                        copy.write_row(event)
                        counts[feed] += 1
                    print(f"{feed}: {counts[feed]} selected valid rows", flush=True)
        connection.execute("""
            create index if not exists demo_events_carrier_date
            on learning_demo.source_events (usdot_number, event_date)
        """)
        connection.execute("analyze learning_demo.source_events")
    return {
        "data_as_of": data_as_of.isoformat(),
        "parquet_sha256": fingerprints,
        "selected_rows": dict(counts),
        "parse_exclusions": dict(exclusions),
        "availability_quality": "source_proxy",
        "label_ascertainment": "retained_snapshot_not_eventual_completeness",
    }
