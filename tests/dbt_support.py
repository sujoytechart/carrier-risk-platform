"""Shared helpers for dbt integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict

POSTGRES_DSN = os.getenv(
    "CARRIER_RISK_TEST_DATABASE_URL",
    "host=localhost port=5432 dbname=carrier_risk user=carrier_risk",
)


def write_test_profile(directory: Path, *, threads: int = 2) -> None:
    """Write a dbt profile for the same database used by the test lock."""
    connection = conninfo_to_dict(POSTGRES_DSN)
    profile = f"""carrier_risk_platform:
  target: test
  outputs:
    test:
      type: postgres
      host: {connection["host"]}
      port: {connection["port"]}
      user: {connection["user"]}
      password: "{{{{ env_var('PGPASSWORD') }}}}"
      dbname: {connection["dbname"]}
      schema: public
      threads: {threads}
"""
    (directory / "profiles.yml").write_text(profile)


def execute_dbt(
    profiles_dir: Path, *arguments: str
) -> subprocess.CompletedProcess[str]:
    """Run dbt and return captured output without interpreting its status."""
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as lock_connection:
        lock_connection.execute("select pg_advisory_lock(764301234::bigint)")
        environment = dict(os.environ)
        environment["CARRIER_RISK_DBT_LOCK_PID"] = str(lock_connection.info.backend_pid)
        return subprocess.run(
            [
                str(Path(sys.executable).with_name("dbt")),
                *arguments,
                "--profiles-dir",
                str(profiles_dir),
                "--target-path",
                str(profiles_dir / "target"),
                "--log-path",
                str(profiles_dir / "logs"),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )


def run_dbt(profiles_dir: Path, *arguments: str) -> None:
    """Run dbt and fail the calling test with complete diagnostic output."""
    result = execute_dbt(profiles_dir, *arguments)
    if result.returncode != 0:
        raise AssertionError(f"dbt command failed:\n{result.stdout}\n{result.stderr}")
