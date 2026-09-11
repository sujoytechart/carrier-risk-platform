"""Measure maturity from a verified, local full crash Parquet snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import tempfile
import time
from collections import Counter
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.compute as compute
import pyarrow.parquet as parquet

from ml.maturity import (
    CrashReportVersion,
    add_months,
    calculate_watermark,
    mature_cohort_months,
    training_eligibility,
)

COLUMNS = [
    "CRASH_ID",
    "DOT_NUMBER",
    "REPORT_STATE",
    "REPORT_NUMBER",
    "REPORT_DATE",
    "REPORT_TIME",
    "REPORT_SEQ_NO",
    "FEDERAL_RECORDABLE",
    "ADD_DATE",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_date(value: str) -> date:
    if not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("invalid_event_date")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    except ValueError as error:
        raise ValueError("invalid_event_date") from error


def _source_add(value: str) -> datetime:
    if not re.fullmatch(r"[0-9]{8} [0-9]{4}", value):
        raise ValueError("invalid_source_add_at")
    try:
        return datetime(
            int(value[:4]),
            int(value[4:6]),
            int(value[6:8]),
            int(value[9:11]),
            int(value[11:13]),
            tzinfo=UTC,
        )
    except ValueError as error:
        raise ValueError("invalid_source_add_at") from error


def _report(row: dict[str, str]) -> CrashReportVersion:
    number = row["DOT_NUMBER"].strip()
    if not re.fullmatch(r"[0-9]+(?:\.0+)?", number) or int(number.split(".")[0]) <= 0:
        raise ValueError("invalid_usdot_number")
    carrier = str(int(number.split(".")[0]))
    event_date = _source_date(row["REPORT_DATE"].strip())
    knowledge_time = _source_add(row["ADD_DATE"].strip()) + timedelta(days=1)
    if knowledge_time.date() < event_date:
        raise ValueError("impossible_event_chronology")
    state = row["REPORT_STATE"].strip()
    report_number = row["REPORT_NUMBER"].strip()
    report_time = row["REPORT_TIME"].strip()
    if (
        not state
        or not report_number
        or not report_time.isdecimal()
        or int(report_time) > 2359
        or int(report_time) % 100 > 59
    ):
        raise ValueError("invalid_incident_key")
    recordable = row["FEDERAL_RECORDABLE"].strip().lower()
    if recordable not in {"y", "yes", "1", "true", "n", "no", "0", "false", ""}:
        raise ValueError("invalid_federal_recordable")
    source_key = row["CRASH_ID"].strip()
    if not source_key:
        sequence = row["REPORT_SEQ_NO"].strip()
        if not sequence.isdecimal():
            raise ValueError("missing_source_record_key")
        source_key = json.dumps(
            [
                state,
                report_number,
                event_date.isoformat(),
                int(report_time),
                int(sequence),
            ]
        )
    return CrashReportVersion(
        usdot_number=carrier,
        report_state=state,
        report_number=report_number,
        event_date=event_date,
        report_time=str(int(report_time)),
        source_record_key=source_key,
        reported_date=knowledge_time.date(),
        knowledge_valid_from=knowledge_time,
        federal_recordable=recordable in {"y", "yes", "1", "true"},
    )


def _scan_snapshot(
    path: Path, months: tuple[date, ...]
) -> tuple[list[CrashReportVersion], dict[str, Any]]:
    reports: list[CrashReportVersion] = []
    excluded: Counter[str] = Counter()
    row_count = missing_rows = selected_rows = selected_missing = 0
    first_date = months[0].strftime("%Y%m%d")
    after_last_date = add_months(months[-1], 1).strftime("%Y%m%d")
    # Missing-carrier identity reconciliation spans the full snapshot. SQLite
    # keeps this potentially million-incident distinct set off the Python heap.
    with (
        tempfile.TemporaryDirectory(prefix="carrier-maturity-") as directory,
        closing(sqlite3.connect(Path(directory) / "excluded.sqlite")) as connection,
    ):
        connection.execute(
            "create table missing (state text, number text, event text, time text, "
            "primary key (state, number, event, time)) without rowid"
        )
        source = parquet.ParquetFile(path)
        for batch in source.iter_batches(batch_size=65536, columns=COLUMNS):
            row_count += batch.num_rows
            carrier_values = compute.utf8_trim_whitespace(batch.column("DOT_NUMBER"))
            missing_mask = compute.equal(carrier_values, "")
            missing_batch = batch.filter(missing_mask)
            missing_rows += missing_batch.num_rows
            missing_keys = [
                (
                    row["REPORT_STATE"].strip(),
                    row["REPORT_NUMBER"].strip(),
                    row["REPORT_DATE"].strip(),
                    str(int(row["REPORT_TIME"]))
                    if row["REPORT_TIME"].strip().isdecimal()
                    else row["REPORT_TIME"].strip(),
                )
                for row in missing_batch.select(
                    ["REPORT_STATE", "REPORT_NUMBER", "REPORT_DATE", "REPORT_TIME"]
                ).to_pylist()
            ]
            connection.executemany(
                "insert or ignore into missing values (?, ?, ?, ?)", missing_keys
            )
            event_dates = compute.utf8_trim_whitespace(batch.column("REPORT_DATE"))
            selected_mask = compute.and_(
                compute.greater_equal(event_dates, first_date),
                compute.less(event_dates, after_last_date),
            )
            selected = batch.filter(selected_mask)
            selected_rows += selected.num_rows
            for row in selected.to_pylist():
                if not row["DOT_NUMBER"].strip():
                    selected_missing += 1
                    continue
                try:
                    reports.append(_report(row))
                except ValueError as error:
                    excluded[str(error)] += 1
        distinct_missing = int(
            connection.execute("select count(*) from missing").fetchone()[0]
        )
    return reports, {
        "source_rows_scanned": row_count,
        "excluded_missing_usdot_rows": missing_rows,
        "excluded_missing_usdot_incidents": distinct_missing,
        "missing_incident_identity": [
            "report_state",
            "report_number",
            "report_date",
            "report_time",
        ],
        "missing_incident_scope": (
            "Distinct source incident keys without carrier identity; "
            "these cannot be carrier-level incidents."
        ),
        "selected_cohort_rows": selected_rows,
        "selected_cohort_missing_usdot_rows": selected_missing,
        "selected_cohort_parse_exclusions": dict(sorted(excluded.items())),
        "selected_cohort_valid_rows": len(reports),
    }


def measure_parquet_snapshot(
    parquet_path: Path,
    lineage_path: Path,
    *,
    computed_at: datetime,
    bootstrap_seed: int = 20260911,
    bootstrap_replicates: int = 2000,
) -> dict[str, Any]:
    """Verify full-snapshot lineage and calculate aggregate source-proxy evidence.

    Invalid selected-cohort source values are counted as quarantined exclusions.
    Missing carriers are excluded before carrier-level deduplication, with global
    row and distinct source-incident counts retained in the acquisition audit.
    """
    started_at = datetime.now(UTC)
    started = time.monotonic()
    lineage = json.loads(lineage_path.read_text())
    actual_checksum = _sha256(parquet_path)
    if actual_checksum != lineage["parquet_sha256"]:
        raise ValueError("Parquet checksum does not match acquisition lineage")
    reconciliation = lineage["reconciliation"]
    if reconciliation["source_value_sha256"] != reconciliation["parquet_value_sha256"]:
        raise ValueError("Source and Parquet values did not reconcile")
    manifest = lineage["source_manifest"]
    observed_at = datetime.fromisoformat(manifest["observed_at"])
    data_as_of = observed_at.date()
    reports, audit = _scan_snapshot(parquet_path, mature_cohort_months(data_as_of))
    counts = [
        manifest["row_count"],
        reconciliation["source_row_count"],
        reconciliation["parquet_row_count"],
        audit["source_rows_scanned"],
    ]
    if len(set(counts)) != 1:
        raise ValueError("Full-snapshot row counts do not reconcile")
    watermark = calculate_watermark(
        reports,
        data_as_of=data_as_of,
        computed_at=computed_at,
        source_fingerprint=actual_checksum,
        bootstrap_seed=bootstrap_seed,
        bootstrap_replicates=bootstrap_replicates,
        source_excluded_missing_usdot_rows=audit["excluded_missing_usdot_rows"],
        source_excluded_missing_usdot_incidents=audit[
            "excluded_missing_usdot_incidents"
        ],
    )
    # Evaluate actual calendar intervals across representative retrospective years.
    label_ends = [add_months(date(2024, 1, 1), offset) for offset in range(24)]
    decisions = [
        training_eligibility(watermark, label_window_end=end) for end in label_ends
    ]
    return {
        "watermark": json.loads(watermark.to_json()),
        "execution": {
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
        "source_audit": audit,
        "lineage": {
            "source_batch_id": manifest["batch_id"],
            "source_object_sha256": manifest["object_sha256"],
            "parquet_sha256": actual_checksum,
            "source_observed_at": manifest["observed_at"],
            "source_rows": manifest["row_count"],
            "source_parquet_values_reconciled": True,
        },
        "bootstrap_method": (
            "Exact empirical histogram multinomial bootstrap with replacement; "
            "linear p99.5 in each replicate; linear 95th percentile of replicate "
            "estimates; ceiling of largest of 12 monthly upper bounds."
        ),
        "software": {"numpy": np.__version__},
        "retrospective_gate": {
            "eligible_for_any_tested_label_end": any(
                decision.eligible for decision in decisions
            ),
            "tested_label_end_start": label_ends[0].isoformat(),
            "tested_label_end_end": label_ends[-1].isoformat(),
            "reasons": sorted({decision.reason for decision in decisions}),
            "maximum_calendar_grace_months": 9,
        },
        "limitations": [
            "This is one current-version snapshot, not an observed archive of "
            "historical first versions. Deleted or superseded historical rows "
            "cannot be reconstructed.",
            "First appearance is the earliest retained incident ADD_DATE plus "
            "one publication day, labeled source_proxy. Current reportability "
            "and identity may contain later corrections.",
            "The source clock has no documented timezone; UTC is a normalization "
            "convention. This does not assert observed public availability at "
            "that instant.",
            "Snapshot absence and tails arriving after data_as_of remain "
            "unobservable. Mature means month end plus nine calendar months; "
            "it is a policy eligibility rule, not proof the cohort can receive "
            "no later reports.",
            "Missing-USDOT source incident counts cannot identify carrier-level "
            "incidents. They are reported separately from retained carrier "
            "incident sample counts.",
            "Watermark missing-carrier exclusions cover the entire verified "
            "snapshot; source_audit separately records selected-cohort and "
            "parse exclusions.",
        ],
    }


def main() -> None:
    """Write aggregate evidence without copying federal source rows."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet", type=Path, required=True, help="Local full crash snapshot"
    )
    parser.add_argument(
        "--lineage", type=Path, required=True, help="Verified converter lineage JSON"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="Aggregate-only evidence JSON"
    )
    parser.add_argument(
        "--computed-at",
        type=datetime.fromisoformat,
        required=True,
        help="Timezone-aware ISO calculation timestamp",
    )
    parser.add_argument("--seed", type=int, default=20260911)
    parser.add_argument("--bootstrap-replicates", type=int, default=2000)
    arguments = parser.parse_args()
    evidence = measure_parquet_snapshot(
        arguments.parquet,
        arguments.lineage,
        computed_at=arguments.computed_at,
        bootstrap_seed=arguments.seed,
        bootstrap_replicates=arguments.bootstrap_replicates,
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "output": str(arguments.output),
                "measured_grace_days": evidence["watermark"]["measured_grace_days"],
                "gate": evidence["retrospective_gate"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
