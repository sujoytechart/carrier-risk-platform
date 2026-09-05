from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import psycopg

POSTGRES_DSN = os.getenv(
    "CARRIER_RISK_TEST_DATABASE_URL",
    "postgresql://carrier_risk:carrier_risk@localhost:5432/carrier_risk",
)


def _write_test_profile(directory: Path) -> None:
    profile = """carrier_risk_platform:
  target: test
  outputs:
    test:
      type: postgres
      host: localhost
      port: 5432
      user: carrier_risk
      password: carrier_risk
      dbname: carrier_risk
      schema: public
      threads: 2
"""
    (directory / "profiles.yml").write_text(profile)


def _run_dbt(profiles_dir: Path, *arguments: str) -> None:
    result = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            *arguments,
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"dbt command failed:\n{result.stdout}\n{result.stderr}")


def test_clean_models_type_valid_rows_and_preserve_rejection_reasons(
    tmp_path: Path,
) -> None:
    _write_test_profile(tmp_path)

    _run_dbt(tmp_path, "seed", "--full-refresh")
    _run_dbt(tmp_path, "build", "--select", "path:dbt/models/clean")

    with psycopg.connect(POSTGRES_DSN) as connection:
        inspections = connection.execute(
            """
            select source_record_key, usdot_number, event_date,
                   violation_count, oos_violation_count, parse_reasons
              from clean.clean_inspections
             order by source_row_number
            """
        ).fetchall()
        crashes = connection.execute(
            """
            select source_record_key, usdot_number, event_date,
                   model_eligibility, exclusion_reason, parse_reasons
              from clean.clean_crashes
             order by source_row_number
            """
        ).fetchall()

    assert inspections == [
        ("1001", "123", date(2025, 12, 1), 2, 1, []),
        ("1002", None, date(2025, 12, 2), 0, 0, ["invalid_usdot_number"]),
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
    ]
