"""A database-scoped session lock spanning an entire dbt subprocess."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

from ingest.database import DatabaseConnection

# dbt's startup guard verifies this key and the owning backend PID. Keep the SQL
# guard synchronized; changing either side alone must reject the build.
DBT_BUILD_LOCK_KEY = 764301234


class PostgresBuildLock:
    """Serialize model publication across Airflow, backfill, and operator runs.

    The connection stays open while dbt uses its own worker connections. Closing
    it releases the session lock even if the subprocess or its caller fails.
    """

    def __init__(self, connection_factory: Callable[[], DatabaseConnection]) -> None:
        self._connection_factory = connection_factory

    @contextmanager
    def __call__(self) -> Iterator[int]:
        """Yield the backend PID only after acquiring the exclusive build lock."""
        with self._connection_factory() as connection:
            connection.execute("select pg_advisory_lock(%s)", (DBT_BUILD_LOCK_KEY,))
            try:
                yield connection.info.backend_pid
            finally:
                # Wait for server acknowledgement instead of relying on eventual
                # backend cleanup after the client closes its TCP connection.
                connection.execute(
                    "select pg_advisory_unlock(%s)", (DBT_BUILD_LOCK_KEY,)
                )
