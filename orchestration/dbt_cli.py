"""Run dbt model commands with a warehouse-wide publication lock."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg

from orchestration.dbt_lock import PostgresBuildLock
from orchestration.dbt_runner import DbtRunner
from orchestration.snowflake_lock import SnowflakeBuildLock

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


def main() -> None:
    """Select the warehouse guard and hold it for the full dbt command."""
    parser = argparse.ArgumentParser(
        description="Run dbt with a build lock. PostgreSQL uses "
        "CARRIER_RISK_DATABASE_URL; Snowflake uses SNOWFLAKE_* configuration."
    )
    parser.add_argument(
        "--warehouse-target", choices=("postgres", "snowflake"), default="postgres"
    )
    parser.add_argument("command", choices=("build", "run", "seed"))
    parser.add_argument("dbt_arguments", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    build_lock: Callable[[], AbstractContextManager[int]]
    dbt_arguments = list(arguments.dbt_arguments)
    if arguments.warehouse_target == "snowflake":
        if any(
            value == "--target"
            or value.startswith("--target=")
            or value.startswith("-t")
            for value in dbt_arguments
        ):
            parser.error("The Snowflake launcher controls the dbt target")
        required = (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_WAREHOUSE",
            "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PATH",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            parser.error("Set " + ", ".join(missing))
        build_lock = SnowflakeBuildLock(
            _connect_snowflake, database=os.environ["SNOWFLAKE_DATABASE"]
        )
        dbt_arguments.extend(("--target", "snowflake"))
    else:
        database_url = os.getenv("CARRIER_RISK_DATABASE_URL")
        if not database_url:
            parser.error(
                "Set CARRIER_RISK_DATABASE_URL; use PGPASSWORD or .pgpass for secrets"
            )
        build_lock = PostgresBuildLock(
            lambda: psycopg.connect(database_url, autocommit=True)
        )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    runner = DbtRunner(
        project_dir=Path(os.getenv("CARRIER_RISK_PROJECT_DIR", ".")),
        profiles_dir=Path(os.getenv("CARRIER_RISK_DBT_PROFILES_DIR", "config")),
        build_lock=build_lock,
    )
    runner.run(arguments.command, dbt_arguments)


def _connect_snowflake() -> SnowflakeConnection:
    """Import the optional connector only for an explicitly selected cloud run."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.environ["DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PATH"],
        private_key_file_pwd=os.getenv(
            "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", ""
        ),
        login_timeout=30,
        network_timeout=15,
        socket_timeout=15,
        client_session_keep_alive=False,
        session_parameters={
            "TIMEZONE": "UTC",
            "STATEMENT_TIMEOUT_IN_SECONDS": 60,
            "ABORT_DETACHED_QUERY": True,
            "QUERY_TAG": "carrier-risk-dbt-guard",
        },
    )


if __name__ == "__main__":
    main()
