"""A durable run claim plus a dedicated Snowflake transaction for dbt writers."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor


class SnowflakeRecoveryRequired(RuntimeError):
    """The writer must remain blocked until its prior work is reconciled."""


class SnowflakeBuildLock:
    """Keep uncertain worker outcomes from admitting a competing build.

    The preinitialized singleton has a committed owner before any worker starts.
    A separate transaction then holds its write lock for the whole command. Losing
    that transaction cannot erase the committed owner. Only a normal context exit
    with the exact lock still held clears ownership; every failure leaves a claim
    for explicit recovery after the old processes and warehouse work have stopped.

    Callers must wait for their synchronous workers before normal context exit.
    This class never resumes compute, initializes tables, or automatically clears
    a previous claim. Use a dedicated writer user shared with dbt's connections.
    """

    def __init__(
        self,
        connection_factory: Callable[[], SnowflakeConnection],
        *,
        database: str,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", database):
            raise ValueError("Use a simple unquoted Snowflake database identifier")
        self._connection_factory = connection_factory
        self._table = f"{database.upper()}.CARRIER_RISK_CONTROL.BUILD_GUARD"

    @contextmanager
    def __call__(self) -> Iterator[int]:
        """Yield the acquired transaction; retain ownership on uncertain cleanup."""
        connection = self._connection_factory()
        owner = str(uuid4())
        try:
            cursor = connection.cursor()
            cursor.execute("ALTER SESSION SET LOCK_TIMEOUT = 3")
            cursor.execute("BEGIN")
            # Change every row so malformed singleton state cannot silently avoid
            # contention. The transaction is rolled back unless exactly one exists.
            cursor.execute(f"UPDATE {self._table} SET revision = revision + 1")
            self._require_singleton(cursor)
            cursor.execute(f"SELECT singleton, owner_run FROM {self._table}")
            row = cursor.fetchone()
            if not isinstance(row, tuple) or len(row) != 2 or row[0] != 1:
                raise SnowflakeRecoveryRequired("Invalid build guard singleton")
            if row[1] is not None:
                raise SnowflakeRecoveryRequired(
                    f"Unfinished Snowflake run {row[1]}; stop and reconcile its "
                    "workers before explicit recovery"
                )
            cursor.execute(
                f"UPDATE {self._table} SET owner_run = %s WHERE singleton = 1",
                (owner,),
            )
            self._require_singleton(cursor)
            cursor.execute("COMMIT")
            cursor.execute("BEGIN")
            cursor.execute(
                f"UPDATE {self._table} SET revision = revision + 1 "
                "WHERE singleton = 1 AND owner_run = %s",
                (owner,),
            )
            self._require_singleton(cursor)
            cursor.execute("SELECT CURRENT_TRANSACTION()")
            row = cursor.fetchone()
            if (
                not isinstance(row, tuple)
                or len(row) != 1
                or not re.fullmatch(r"[1-9][0-9]{0,37}", str(row[0]))
            ):
                raise SnowflakeRecoveryRequired("No acquired guard transaction")
            transaction = int(str(row[0]))
            yield transaction
            try:
                self._verify_lock(cursor, transaction)
            except Exception as error:
                raise SnowflakeRecoveryRequired(
                    "Cannot verify the build guard lock; retain the run for recovery"
                ) from error
            cursor.execute("ROLLBACK")
            # Ownership remains committed while switching transactions. Another
            # launcher can acquire the row lock here but must reject that owner.
            cursor.execute("BEGIN")
            cursor.execute(
                f"UPDATE {self._table} SET owner_run = NULL "
                "WHERE singleton = 1 AND owner_run = %s",
                (owner,),
            )
            self._require_singleton(cursor)
            cursor.execute("COMMIT")
        except BaseException:
            # Rollback is explicit: connector.close() did not release the lock in
            # the live disconnect probe. A lost connection still requires recovery.
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _require_singleton(cursor: SnowflakeCursor) -> None:
        if cursor.rowcount != 1:
            raise SnowflakeRecoveryRequired(
                "Expected exactly one build guard singleton"
            )

    def _verify_lock(self, cursor: SnowflakeCursor, transaction: int) -> None:
        cursor.execute("SHOW LOCKS")
        columns = [column.name for column in cursor.description or ()]
        for values in cursor.fetchall():
            if not isinstance(values, tuple):
                continue
            row = dict(zip(columns, values, strict=True))
            if (
                row.get("resource") == self._table
                and str(row.get("transaction")) == str(transaction)
                and row.get("status") == "HOLDING"
            ):
                return
        raise SnowflakeRecoveryRequired(
            "The exact build guard lock was lost; retain the run for recovery"
        )
