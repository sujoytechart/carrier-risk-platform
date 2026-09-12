"""Execute four-month feature and six-month label boundaries in PostgreSQL."""

from datetime import date
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from tests import dbt_support


def test_demo_sql_respects_scoring_and_outcome_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = dbt_support.POSTGRES_DSN
    with psycopg.connect(dsn, autocommit=True) as admin:
        if (
            admin.execute(
                "select 1 from pg_database where datname='carrier_risk_demo_test'"
            ).fetchone()
            is None
        ):
            admin.execute("create database carrier_risk_demo_test")
    isolated = make_conninfo(dsn, dbname="carrier_risk_demo_test")
    monkeypatch.setattr(dbt_support, "POSTGRES_DSN", isolated)
    dbt_support.write_test_profile(tmp_path, threads=1)
    with psycopg.connect(isolated, autocommit=True) as connection:
        connection.execute("drop schema if exists learning_demo cascade")
        connection.execute("create schema learning_demo")
        connection.execute("""
            create table learning_demo.source_events (
                event_type text, source_key text, usdot_number text,
                event_date date, reported_date date,
                violation_count bigint, oos_violation_count bigint,
                federal_recordable boolean
            )
        """)
        events = [
            ("inspection", "one", "2023-10-01", "2023-10-02", 3, 1, False),
            ("inspection", "one", "2023-10-01", "2023-10-02", 3, 1, False),
            ("inspection", "too_old", "2023-09-30", "2023-10-02", 7, 1, False),
            ("inspection", "not_known", "2024-01-31", "2024-02-01", 7, 1, False),
            ("crash", "known_prior", "2023-12-01", "2023-12-02", 0, 0, True),
            ("crash", "late_prior", "2024-01-30", "2024-02-01", 0, 0, True),
            ("crash", "nonfederal_future", "2024-04-01", "2024-04-02", 0, 0, False),
            ("crash", "after_target", "2024-08-01", "2024-08-02", 0, 0, True),
            ("crash", "too_late_label", "2024-04-01", "2026-09-03", 0, 0, True),
        ]
        with connection.cursor() as cursor:
            cursor.executemany(
                "insert into learning_demo.source_events values "
                "(%s,%s,'123',%s,%s,%s,%s,%s)",
                events,
            )
        connection.execute("""
            insert into learning_demo.source_events values
            ('inspection','control_inspection','456',date '2023-10-01',
                date '2023-10-02',1,0,false),
            ('crash','valid_outcome','456',date '2024-02-01',
                date '2024-02-02',0,0,true),
            ('inspection','current_inspection','789',date '2026-05-01',
                date '2026-05-02',0,0,false)
        """)
        try:
            dbt_support.run_dbt(
                tmp_path,
                "build",
                "--select",
                "demo_training_features",
                "demo_inspection_identity",
                "--vars",
                '{"learning_demo":true}',
            )
            row = connection.execute("""
                select scoring_date, inspections_4m, violations_4m, crashes_24m, label
                from learning_demo.demo_training_features
                where usdot_number='123'
            """).fetchone()
            assert row == (date(2024, 2, 1), 1, 3, 1, 0)
            assert connection.execute(
                "select label from learning_demo.demo_training_features "
                "where usdot_number='456'"
            ).fetchone() == (1,)
            assert connection.execute(
                "select label from learning_demo.demo_training_features "
                "where usdot_number='789'"
            ).fetchone() == (None,)
        finally:
            connection.execute("drop schema learning_demo cascade")
