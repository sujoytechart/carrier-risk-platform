"""Failure behavior at the Snowflake connector boundary; cloud proofs are separate."""

from unittest.mock import MagicMock

import pytest

from orchestration.snowflake_lock import SnowflakeBuildLock, SnowflakeRecoveryRequired


def connection_fixture(*, owner: str | None = None) -> MagicMock:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.rowcount = 1
    cursor.fetchone.side_effect = [(1, owner), ("1234567890123456789",)]
    cursor.fetchall.return_value = [
        ("FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD", 1234567890123456789, "HOLDING")
    ]
    cursor.description = [MagicMock(name="column") for _ in range(3)]
    for column, name in zip(
        cursor.description, ["resource", "transaction", "status"], strict=True
    ):
        column.name = name
    return connection


def statements(connection: MagicMock) -> list[str]:
    return [
        call.args[0] for call in connection.cursor.return_value.execute.call_args_list
    ]


def test_success_releases_marker_only_after_worker_returns() -> None:
    connection = connection_fixture()
    guard = SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")
    with guard() as transaction:
        assert transaction == 1234567890123456789
        assert not any("SET owner_run = NULL" in sql for sql in statements(connection))
    assert any("SET owner_run = NULL" in sql for sql in statements(connection))
    assert statements(connection)[-1] == "COMMIT"
    connection.close.assert_called_once()


def test_worker_failure_retains_marker_for_orphan_recovery() -> None:
    connection = connection_fixture()
    guard = SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")
    with pytest.raises(RuntimeError, match="worker failed"), guard():
        raise RuntimeError("worker failed")
    assert not any("SET owner_run = NULL" in sql for sql in statements(connection))
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_prior_committed_run_blocks_even_when_no_transaction_lock_remains() -> None:
    connection = connection_fixture(owner="unfinished-run")
    guard = SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")
    with pytest.raises(SnowflakeRecoveryRequired, match="unfinished-run"), guard():
        pytest.fail("An unfinished run must never admit another worker")
    assert "COMMIT" not in statements(connection)
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_lost_guard_does_not_clear_committed_marker() -> None:
    connection = connection_fixture()
    connection.cursor.return_value.fetchall.return_value = []
    guard = SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")
    with pytest.raises(SnowflakeRecoveryRequired, match="lock"), guard():
        pass
    assert not any("SET owner_run = NULL" in sql for sql in statements(connection))
    connection.close.assert_called_once()


@pytest.mark.parametrize("rowcount", [0, 2])
def test_missing_or_duplicate_singleton_refuses_to_start(rowcount: int) -> None:
    connection = connection_fixture()
    connection.cursor.return_value.rowcount = rowcount
    with (
        pytest.raises(SnowflakeRecoveryRequired, match="singleton"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pytest.fail("Invalid guard relation must not admit workers")
    assert "COMMIT" not in statements(connection)


def test_database_identifier_is_validated_before_opening_connection() -> None:
    factory = MagicMock()
    with pytest.raises(ValueError, match="database"):
        SnowflakeBuildLock(factory, database="fixture; DROP DATABASE unrelated")
    factory.assert_not_called()


@pytest.mark.parametrize("row", [(2, None), (1,), None])
def test_invalid_singleton_shape_blocks_before_claim(row: object) -> None:
    connection = connection_fixture()
    connection.cursor.return_value.fetchone.side_effect = [row]
    with (
        pytest.raises(SnowflakeRecoveryRequired, match="singleton"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pytest.fail("Malformed singleton must not admit workers")
    assert "COMMIT" not in statements(connection)


@pytest.mark.parametrize(
    "lock",
    [
        ("OTHER_DB.CARRIER_RISK_CONTROL.BUILD_GUARD", 1234567890123456789, "HOLDING"),
        ("FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD", 1, "HOLDING"),
        ("FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD", 1234567890123456789, "WAITING"),
    ],
)
def test_unrelated_or_waiting_lock_cannot_authorize_claim_release(
    lock: tuple[object, ...],
) -> None:
    connection = connection_fixture()
    connection.cursor.return_value.fetchall.return_value = [lock]
    with (
        pytest.raises(SnowflakeRecoveryRequired, match="lock"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pass
    assert not any("SET owner_run = NULL" in sql for sql in statements(connection))


def test_cleanup_failure_still_closes_connection() -> None:
    connection = connection_fixture()
    connection.rollback.side_effect = OSError("connection lost")
    with (
        pytest.raises(OSError, match="connection lost"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        raise RuntimeError("worker interrupted")
    connection.close.assert_called_once()
    assert not any("SET owner_run = NULL" in sql for sql in statements(connection))


@pytest.mark.parametrize("transaction", [(), (None,), (0,), (-1,)])
def test_invalid_transaction_identifier_never_reaches_worker(
    transaction: tuple[object, ...],
) -> None:
    connection = connection_fixture()
    connection.cursor.return_value.fetchone.side_effect = [(1, None), transaction]
    with (
        pytest.raises(SnowflakeRecoveryRequired, match="transaction"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pytest.fail("Invalid transaction must not admit workers")


def test_cursor_creation_failure_closes_connection() -> None:
    connection = connection_fixture()
    connection.cursor.side_effect = OSError("cursor unavailable")
    with (
        pytest.raises(OSError, match="cursor unavailable"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pytest.fail("Cursor failure must not admit workers")
    connection.close.assert_called_once()


def test_unreadable_guard_after_worker_exit_requires_recovery() -> None:
    connection = connection_fixture()

    def execute(sql: str, *parameters: object) -> None:
        if sql == "SHOW LOCKS":
            raise RuntimeError("transaction aborted; statements rejected")

    connection.cursor.return_value.execute.side_effect = execute
    with (
        pytest.raises(SnowflakeRecoveryRequired, match="verify"),
        SnowflakeBuildLock(lambda: connection, database="FIXTURE_DB")(),
    ):
        pass
    assert not any("SET owner_run = NULL" in sql for sql in statements(connection))
