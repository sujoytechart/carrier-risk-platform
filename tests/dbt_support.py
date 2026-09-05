"""Shared helpers for dbt integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

POSTGRES_DSN = os.getenv(
    "CARRIER_RISK_TEST_DATABASE_URL",
    "postgresql://carrier_risk:carrier_risk@localhost:5432/carrier_risk",
)


def write_test_profile(directory: Path, *, threads: int = 2) -> None:
    """Write an isolated dbt profile for the disposable PostgreSQL database."""
    profile = f"""carrier_risk_platform:
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
      threads: {threads}
"""
    (directory / "profiles.yml").write_text(profile)


def execute_dbt(
    profiles_dir: Path, *arguments: str
) -> subprocess.CompletedProcess[str]:
    """Run dbt and return captured output without interpreting its status."""
    return subprocess.run(
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


def run_dbt(profiles_dir: Path, *arguments: str) -> None:
    """Run dbt and fail the calling test with complete diagnostic output."""
    result = execute_dbt(profiles_dir, *arguments)
    if result.returncode != 0:
        raise AssertionError(f"dbt command failed:\n{result.stdout}\n{result.stderr}")
