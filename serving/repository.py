"""A pooled PostgreSQL boundary for monthly features and live eligibility."""

from __future__ import annotations

from datetime import UTC, date, datetime, time

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout

from ml.features import FeatureRow
from serving.service import FeatureLookup, FeatureStoreUnavailable


class PostgresFeatureRepository:
    """Use one bounded read statement so features and eligibility share a snapshot."""

    def __init__(self, database_url: str, *, schema: str = "modeled") -> None:
        self._pool: ConnectionPool[psycopg.Connection[dict[str, object]]] = (
            ConnectionPool(
                database_url,
                min_size=1,
                max_size=12,
                timeout=2,
                open=False,
                kwargs={
                    "autocommit": True,
                    "row_factory": dict_row,
                    "connect_timeout": 3,
                    "options": "-c statement_timeout=2000 -c timezone=UTC",
                },
            )
        )
        self._query = sql.SQL("""
            select current_eligibility.eligible, features.*
            from (
                select exists (
                    select 1 from {}.inspections inspection
                    where inspection.usdot_number = %(usdot)s
                      and inspection.event_date >=
                          (%(scoring_date)s::date - interval '6 months')::date
                      and inspection.event_date < %(scoring_date)s::date
                      and inspection.reported_date < %(scoring_date)s::date
                      and inspection.knowledge_valid_from < %(scoring_time)s
                      and (inspection.knowledge_valid_to is null
                           or inspection.knowledge_valid_to > %(scoring_time)s)
                      and not inspection.is_deleted
                ) as eligible
            ) current_eligibility
            left join lateral (
                select * from {}.training_features
                where usdot_number = %(usdot)s
                  and scoring_date <= %(scoring_date)s::date
                order by scoring_date desc
                limit 1
            ) features on true
        """).format(sql.Identifier(schema), sql.Identifier(schema))

    def open(self) -> None:
        """Connect at startup and fail promptly if PostgreSQL is unavailable."""
        try:
            self._pool.open(wait=True, timeout=4)
        except (psycopg.Error, PoolTimeout) as error:
            self._pool.close()
            raise FeatureStoreUnavailable("Warehouse connection unavailable") from error

    def close(self) -> None:
        """Release all connections when the application exits."""
        self._pool.close()

    def lookup(self, usdot_number: str, scoring_date: date) -> FeatureLookup:
        """Enforce strict UTC event, reporting, and knowledge-time boundaries."""
        parameters = {
            "usdot": usdot_number,
            "scoring_date": scoring_date,
            "scoring_time": datetime.combine(scoring_date, time.min, tzinfo=UTC),
        }
        try:
            with self._pool.connection() as connection:
                row = connection.execute(self._query, parameters).fetchone()
            if row is None:
                raise FeatureStoreUnavailable("Warehouse lookup returned no status")
            features = (
                FeatureRow.from_mapping(row)
                if row["scoring_date"] is not None
                else None
            )
            return FeatureLookup(eligible=bool(row["eligible"]), features=features)
        except (psycopg.Error, PoolTimeout, ValueError, KeyError, TypeError) as error:
            raise FeatureStoreUnavailable("Warehouse feature lookup failed") from error
