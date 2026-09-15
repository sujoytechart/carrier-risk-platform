"""Run dbt model commands with a warehouse-wide publication lock."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import psycopg

from orchestration.dbt_lock import PostgresBuildLock
from orchestration.dbt_runner import DbtRunner


def main() -> None:
    """Parse dbt arguments and hold a PostgreSQL session for the full command."""
    parser = argparse.ArgumentParser(
        description="Run dbt safely using CARRIER_RISK_DATABASE_URL for the build lock."
    )
    parser.add_argument("command", choices=("build", "run", "seed"))
    parser.add_argument("dbt_arguments", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    database_url = os.getenv("CARRIER_RISK_DATABASE_URL")
    if not database_url:
        parser.error(
            "Set CARRIER_RISK_DATABASE_URL; use PGPASSWORD or .pgpass for secrets"
        )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    runner = DbtRunner(
        project_dir=Path(os.getenv("CARRIER_RISK_PROJECT_DIR", ".")),
        profiles_dir=Path(os.getenv("CARRIER_RISK_DBT_PROFILES_DIR", "config")),
        build_lock=PostgresBuildLock(
            lambda: psycopg.connect(database_url, autocommit=True)
        ),
    )
    runner.run(arguments.command, arguments.dbt_arguments)


if __name__ == "__main__":
    main()
