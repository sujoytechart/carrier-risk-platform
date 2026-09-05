from __future__ import annotations

from datetime import date
from pathlib import Path

import psycopg

from tests.dbt_support import POSTGRES_DSN, run_dbt, write_test_profile

FIRST_INSPECTION_BATCH_ID = "a" * 64
FIRST_CRASH_BATCH_ID = "b" * 64


def test_clean_models_type_valid_rows_and_preserve_rejection_reasons(
    tmp_path: Path,
) -> None:
    write_test_profile(tmp_path)

    run_dbt(tmp_path, "seed", "--full-refresh")
    run_dbt(tmp_path, "build", "--select", "path:dbt/models/clean")

    with psycopg.connect(POSTGRES_DSN) as connection:
        inspections = connection.execute(
            """
            select source_record_key, usdot_number, event_date,
                   violation_count, oos_violation_count, parse_reasons
             from clean.clean_inspections
             where batch_id = %s
             order by source_row_number
            """,
            (FIRST_INSPECTION_BATCH_ID,),
        ).fetchall()
        crashes = connection.execute(
            """
            select source_record_key, usdot_number, event_date,
                   model_eligibility, exclusion_reason, parse_reasons
             from clean.clean_crashes
             where batch_id = %s
             order by source_row_number
            """,
            (FIRST_CRASH_BATCH_ID,),
        ).fetchall()

    assert inspections == [
        ("1001", "123", date(2025, 12, 1), 2, 1, []),
        ("1002", None, date(2025, 12, 2), 0, 0, ["invalid_usdot_number"]),
        ("1004", "123", date(2026, 1, 15), 1, 0, []),
        ("1005", "123", date(2025, 11, 1), 1, 0, []),
        (
            "1006",
            "123",
            date(2026, 1, 10),
            1,
            0,
            ["impossible_event_chronology"],
        ),
    ]
    assert crashes == [
        ("2001", "123", date(2026, 1, 15), "eligible", None, []),
        (
            "2002",
            None,
            date(2026, 1, 16),
            "excluded",
            "missing_usdot_number",
            [],
        ),
        ("2003", "456", None, "excluded", "invalid_event_date", ["invalid_event_date"]),
        ("2005", "123", date(2026, 2, 1), "eligible", None, []),
        (
            "2006",
            "123",
            date(2026, 2, 10),
            "excluded",
            "impossible_event_chronology",
            ["impossible_event_chronology"],
        ),
    ]
