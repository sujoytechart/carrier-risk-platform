"""Verify the build lock against PostgreSQL's real session-lock semantics."""

import os

import psycopg
import pytest


def test_build_lock_is_exclusive_and_released_after_failure() -> None:
    from orchestration.dbt_lock import DBT_BUILD_LOCK_KEY, PostgresBuildLock

    dsn = os.environ["CARRIER_RISK_TEST_DATABASE_URL"]
    lock = PostgresBuildLock(lambda: psycopg.connect(dsn, autocommit=True))
    with psycopg.connect(dsn, autocommit=True) as observer:
        with pytest.raises(ValueError, match="build failed"), lock() as backend_pid:
            assert backend_pid != observer.info.backend_pid
            assert observer.execute(
                "select pg_try_advisory_lock(%s)", (DBT_BUILD_LOCK_KEY,)
            ).fetchone() == (False,)
            raise ValueError("build failed")

        assert observer.execute(
            "select pg_try_advisory_lock(%s)", (DBT_BUILD_LOCK_KEY,)
        ).fetchone() == (True,)
        observer.execute("select pg_advisory_unlock(%s)", (DBT_BUILD_LOCK_KEY,))
