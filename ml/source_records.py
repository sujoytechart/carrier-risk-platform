"""Federal source parsing shared by maturity analysis and demo extraction."""

import json
import re
from datetime import UTC, date, datetime, timedelta

from ml.maturity import CrashReportVersion

CRASH_COLUMNS = [
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


def parse_source_date(value: str) -> date:
    """Parse a federal YYYYMMDD date or raise a stable exclusion reason."""
    if not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("invalid_event_date")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    except ValueError as error:
        raise ValueError("invalid_event_date") from error


def parse_source_timestamp(value: str) -> datetime:
    """Parse a federal YYYYMMDD HHMM timestamp in UTC."""
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


def parse_crash_report(row: dict[str, str]) -> CrashReportVersion:
    """Validate incident identity and apply the source-add plus one-day proxy."""
    number = row["DOT_NUMBER"].strip()
    if not re.fullmatch(r"[0-9]+(?:\.0+)?", number) or int(number.split(".")[0]) <= 0:
        raise ValueError("invalid_usdot_number")
    carrier = str(int(number.split(".")[0]))
    event_date = parse_source_date(row["REPORT_DATE"].strip())
    knowledge_time = parse_source_timestamp(row["ADD_DATE"].strip()) + timedelta(days=1)
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
