"""PostgreSQL integration proof for request-date eligibility and latest features."""

import os
from collections.abc import Iterator
from datetime import UTC, date, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from serving.repository import PostgresFeatureRepository
from serving.service import FeatureStoreUnavailable


@pytest.fixture
def warehouse() -> Iterator[tuple[psycopg.Connection[tuple[object, ...]], str]]:
    dsn = os.environ["CARRIER_RISK_TEST_DATABASE_URL"]
    schema = "serving_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL("""
            create table {}.training_features (
                usdot_number text, scoring_date date, inspections_6m bigint,
                violations_6m bigint, oos_violations_6m bigint, crashes_24m bigint,
                violations_per_inspection numeric, oos_violation_rate numeric,
                days_since_last_inspection integer
            )
        """).format(sql.Identifier(schema))
        )
        connection.execute(
            sql.SQL("""
            create table {}.inspections (
                usdot_number text, event_date date, reported_date date,
                knowledge_valid_from timestamptz, knowledge_valid_to timestamptz,
                is_deleted boolean
            )
        """).format(sql.Identifier(schema))
        )
        connection.execute(
            sql.SQL("""
            insert into {}.training_features values
            ('1','2026-08-01',1,1,0,0,1,0,1),
            ('1','2026-09-01',2,4,1,1,2,0.25,2),
            ('1','2026-10-01',3,9,1,2,3,0.11,3)
        """).format(sql.Identifier(schema))
        )
        try:
            yield connection, schema
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )


def add_inspection(
    connection: psycopg.Connection[tuple[object, ...]],
    schema: str,
    *,
    event_date: date = date(2026, 8, 15),
    reported_date: date = date(2026, 8, 16),
    knowledge_from: datetime = datetime(2026, 8, 16, tzinfo=UTC),
    knowledge_to: datetime | None = None,
    deleted: bool = False,
) -> None:
    connection.execute(
        sql.SQL("insert into {}.inspections values (%s,%s,%s,%s,%s,%s)").format(
            sql.Identifier(schema)
        ),
        (
            "1",
            event_date,
            reported_date,
            knowledge_from,
            knowledge_to,
            deleted,
        ),
    )


def test_lookup_uses_latest_nonfuture_row_and_maps_numeric_rates(
    warehouse: tuple[psycopg.Connection[tuple[object, ...]], str],
) -> None:
    connection, schema = warehouse
    add_inspection(connection, schema)
    repository = PostgresFeatureRepository(
        os.environ["CARRIER_RISK_TEST_DATABASE_URL"], schema=schema
    )
    repository.open()
    try:
        result = repository.lookup("1", date(2026, 9, 11))
        assert result.eligible is True
        assert result.features is not None
        assert result.features.scoring_date == date(2026, 9, 1)
        assert result.features.numeric_values() == (2, 4, 1, 1, 2, 0.25, 2)
        assert repository.lookup("999", date(2026, 9, 11)).eligible is False
    finally:
        repository.close()


@pytest.mark.parametrize(
    ("field", "value", "eligible"),
    [
        ("event_date", date(2026, 3, 10), False),
        ("event_date", date(2026, 3, 11), True),
        ("event_date", date(2026, 9, 11), False),
        ("reported_date", date(2026, 9, 11), False),
        ("knowledge_from", datetime(2026, 9, 11, tzinfo=UTC), False),
        ("knowledge_to", datetime(2026, 9, 11, tzinfo=UTC), False),
        ("knowledge_to", datetime(2026, 9, 11, 0, 0, 1, tzinfo=UTC), True),
        ("deleted", True, False),
    ],
)
def test_request_eligibility_enforces_six_calendar_months_and_both_clocks(
    warehouse: tuple[psycopg.Connection[tuple[object, ...]], str],
    field: str,
    value: object,
    eligible: bool,
) -> None:
    connection, schema = warehouse
    add_inspection(connection, schema, **{field: value})
    repository = PostgresFeatureRepository(
        os.environ["CARRIER_RISK_TEST_DATABASE_URL"], schema=schema
    )
    repository.open()
    try:
        result = repository.lookup("1", date(2026, 9, 11))
        assert result.eligible is eligible
        assert result.features is not None
    finally:
        repository.close()


def test_missing_relation_is_a_typed_warehouse_failure(
    warehouse: tuple[psycopg.Connection[tuple[object, ...]], str],
) -> None:
    _, schema = warehouse
    repository = PostgresFeatureRepository(
        os.environ["CARRIER_RISK_TEST_DATABASE_URL"], schema=schema + "_absent"
    )
    repository.open()
    try:
        with pytest.raises(FeatureStoreUnavailable, match="lookup failed"):
            repository.lookup("1", date(2026, 9, 11))
    finally:
        repository.close()


def test_invalid_warehouse_feature_values_fail_closed(
    warehouse: tuple[psycopg.Connection[tuple[object, ...]], str],
) -> None:
    connection, schema = warehouse
    connection.execute(
        sql.SQL("update {}.training_features set inspections_6m=-1").format(
            sql.Identifier(schema)
        )
    )
    repository = PostgresFeatureRepository(
        os.environ["CARRIER_RISK_TEST_DATABASE_URL"], schema=schema
    )
    repository.open()
    try:
        with pytest.raises(FeatureStoreUnavailable):
            repository.lookup("1", date(2026, 9, 11))
    finally:
        repository.close()
