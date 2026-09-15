from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import pytest

from tests.dbt_support import POSTGRES_DSN, execute_dbt, run_dbt, write_test_profile

SCORING_DATES = "{scoring_dates: ['2026-03-01', '2026-06-01']}"


def _insert_late_report_fixture() -> None:
    """Add an event that a one-clock feature query would leak into March."""
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            """
            insert into modeled.event_versions (
                feed_name, event_type, source_record_key, usdot_number, event_date,
                reported_date, knowledge_valid_from, availability_quality,
                first_observed_at,
                is_current, is_deleted, record_hash,
                first_seen_batch_id, last_seen_batch_id,
                state, violation_count, oos_violation_count
            ) values (
                'inspections', 'inspection', 'late-report-fixture', '123', %s, %s, %s,
                'source_proxy', %s, true, false, md5('late-report-fixture'),
                %s, %s, 'MI', 5, 2
            )
            """,
            (
                date(2026, 1, 20),
                date(2026, 4, 1),
                datetime(2026, 2, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                "a" * 64,
                "a" * 64,
            ),
        )


def _insert_after_utc_midnight_fixture() -> None:
    """Add an event first known after a UTC scoring boundary."""
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            """
            insert into modeled.event_versions (
                feed_name, event_type, source_record_key, usdot_number, event_date,
                reported_date, knowledge_valid_from, availability_quality,
                first_observed_at,
                is_current, is_deleted, record_hash,
                first_seen_batch_id, last_seen_batch_id,
                state, violation_count, oos_violation_count
            ) values (
                'inspections', 'inspection', 'after-utc-midnight', '123', %s, %s,
                %s, 'observed', %s, true, false, md5('after-utc-midnight'),
                %s, %s, 'MI', 100, 10
            )
            """,
            (
                date(2026, 1, 25),
                date(2026, 1, 26),
                datetime(2026, 3, 1, 2, tzinfo=UTC),
                datetime(2026, 3, 1, 2, tzinfo=UTC),
                "a" * 64,
                "a" * 64,
            ),
        )


def test_modeled_features_enforce_both_clocks_and_deduplicate_incidents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_test_profile(tmp_path, threads=1)

    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        connection.execute("drop schema if exists modeled cascade")

    run_dbt(tmp_path, "seed", "--full-refresh", "--vars", "{load_test_fixtures: true}")
    run_dbt(tmp_path, "build", "--select", "path:dbt/models/clean")
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    _insert_late_report_fixture()
    _insert_after_utc_midnight_fixture()
    monkeypatch.setenv("PGOPTIONS", "-c timezone=America/Detroit")
    run_dbt(
        tmp_path,
        "build",
        "--select",
        "+training_features+",
        "--vars",
        SCORING_DATES,
    )

    with psycopg.connect(POSTGRES_DSN) as connection:
        march_features = connection.execute(
            """
            select inspections_6m, violations_6m, oos_violations_6m
              from modeled.training_features
             where usdot_number = '123'
               and scoring_date = date '2026-03-01'
            """
        ).fetchone()
        one_clock_count = connection.execute(
            """
            select count(*)
              from modeled.events_union
             where usdot_number = '123'
               and event_type = 'inspection'
               and knowledge_valid_from < (
                   date '2026-03-01'::timestamp at time zone 'UTC'
               )
               and (knowledge_valid_to is null
                    or knowledge_valid_to > (
                       date '2026-03-01'::timestamp at time zone 'UTC'
                    ))
               and not is_deleted
               and event_date >= date '2026-03-01' - interval '6 months'
               and event_date < date '2026-03-01'
            """
        ).fetchone()
        incident_rows = connection.execute(
            """
            select count(*), max(fatalities), bool_or(tow_away)
              from modeled.crash_incidents
             where carrier_crash_key = md5('789' || chr(31) || 'IL' || chr(31)
                 || 'C-2004' || chr(31) || '2026-04-20' || chr(31) || '830')
               and not is_deleted
            """
        ).fetchone()

    assert march_features == (3, 4, 1)
    assert one_clock_count == (4,)
    assert incident_rows == (1, 2, True)

    temporal_tests = execute_dbt(
        tmp_path, "test", "--select", "tag:temporal", "--vars", SCORING_DATES
    )
    assert temporal_tests.returncode == 0, temporal_tests.stdout + temporal_tests.stderr

    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            """
            update modeled.training_features
               set inspections_6m = inspections_6m + 1
             where usdot_number = '123'
               and scoring_date = date '2026-03-01'
            """
        )

    broken_feature_test = execute_dbt(
        tmp_path,
        "test",
        "--select",
        "training_features_match_point_in_time_events",
        "--vars",
        SCORING_DATES,
    )
    assert broken_feature_test.returncode != 0
    assert "training_features_match_point_in_time_events" in broken_feature_test.stdout

    run_dbt(tmp_path, "run", "--select", "training_features", "--vars", SCORING_DATES)
