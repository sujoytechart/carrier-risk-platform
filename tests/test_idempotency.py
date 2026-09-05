from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import psycopg

from tests.dbt_support import POSTGRES_DSN, execute_dbt, run_dbt, write_test_profile


def _event_versions() -> list[tuple[object, ...]]:
    with psycopg.connect(POSTGRES_DSN) as connection:
        return connection.execute(
            """
            select feed_name, source_record_key, record_hash, event_date,
                   reported_date, knowledge_valid_from, knowledge_valid_to,
                   is_current, is_deleted, deletion_reason,
                   first_seen_batch_id, last_seen_batch_id,
                   predecessor_version_key, superseded_by_version_key
              from modeled.event_versions
             order by event_version_key
            """
        ).fetchall()


def test_event_history_is_correction_aware_and_replay_safe(tmp_path: Path) -> None:
    write_test_profile(tmp_path, threads=1)

    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        connection.execute("drop schema if exists modeled cascade")

    run_dbt(tmp_path, "seed", "--full-refresh")
    run_dbt(tmp_path, "build", "--select", "path:dbt/models/clean")
    run_dbt(tmp_path, "build", "--select", "+event_versions+")

    first_build = _event_versions()

    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    replayed_build = _event_versions()

    assert replayed_build == first_build

    with psycopg.connect(POSTGRES_DSN) as connection:
        inspection_1001 = connection.execute(
            """
            select event_version_key, violation_count,
                   knowledge_valid_from, knowledge_valid_to,
                   is_current, predecessor_version_key,
                   superseded_by_version_key
              from modeled.event_versions
             where feed_name = 'inspections'
               and source_record_key = '1001'
             order by knowledge_valid_from
            """
        ).fetchall()
        deleted_inspection = connection.execute(
            """
            select is_deleted, deletion_reason
              from modeled.event_versions
             where feed_name = 'inspections'
               and source_record_key = '1004'
               and is_current
            """
        ).fetchone()
        expired_inspection = connection.execute(
            """
            select count(*), bool_or(is_deleted)
              from modeled.event_versions
             where feed_name = 'inspections'
               and source_record_key = '1005'
            """
        ).fetchone()
        deleted_crash = connection.execute(
            """
            select is_deleted, deletion_reason
              from modeled.event_versions
             where feed_name = 'crashes'
               and source_record_key = '2005'
               and is_current
            """
        ).fetchone()

    assert inspection_1001[0][1:5] == (
        2,
        datetime(2025, 12, 6, 12, 0, tzinfo=UTC),
        datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        False,
    )
    assert inspection_1001[1][1:5] == (
        3,
        datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        None,
        True,
    )
    assert inspection_1001[0][6] == inspection_1001[1][0]
    assert inspection_1001[1][5] == inspection_1001[0][0]
    assert deleted_inspection == (True, "source_deleted")
    assert expired_inspection == (1, False)
    assert deleted_crash == (True, "source_deleted")

    assert any(
        row[3] == date(2025, 12, 1) and row[4] == date(2025, 12, 6)
        for row in first_build
    )

    older_batch_id = "e" * 64
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            """
            insert into raw.snapshot_batches (
                batch_id, feed_name, dataset_id, observed_at, status, row_count
            ) values (%s, 'inspections', 'fx4q-ay7w', %s, 'loaded', 0)
            """,
            (older_batch_id, datetime(2026, 4, 15, tzinfo=UTC)),
        )

    out_of_order_build = execute_dbt(tmp_path, "build", "--select", "+event_versions")

    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "delete from raw.snapshot_batches where batch_id = %s",
            (older_batch_id,),
        )

    assert out_of_order_build.returncode != 0
    assert "run the controlled rebuild" in (
        out_of_order_build.stdout + out_of_order_build.stderr
    )
