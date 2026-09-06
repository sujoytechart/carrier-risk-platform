"""Behavioral regressions for source conformance and published knowledge history."""

from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic, sleep

import psycopg
import pytest
from psycopg import sql

from tests.dbt_support import POSTGRES_DSN, execute_dbt, run_dbt, write_test_profile

FIXTURES = "{load_test_fixtures: true}"


def prepare_database(directory: Path) -> None:
    """Reset only the disposable modeled history and reload synthetic inputs."""
    write_test_profile(directory, threads=1)
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        connection.execute("drop schema if exists modeled cascade")
    run_dbt(directory, "seed", "--full-refresh", "--vars", FIXTURES)


def test_invalid_source_values_are_quarantined_without_aborting(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    cases = [
        ("report_date", "20260230", "invalid_event_date"),
        ("report_date", "abcdefgh", "invalid_event_date"),
        ("change_date", "20260230 1200", "invalid_source_change_at"),
        ("change_date", "abcdefgh ijkl", "invalid_source_change_at"),
        ("fatalities", "2147483648", "invalid_fatalities"),
        ("injuries", "bad", "invalid_injuries"),
        ("tow_away", "maybe", "invalid_tow_away"),
        ("report_time", "2360", "invalid_report_time"),
        ("report_seq_no", "999999999999999999999999", "invalid_report_seq_no"),
        ("dot_number", "999999999999999999999999", "invalid_usdot_number"),
    ]
    with psycopg.connect(POSTGRES_DSN) as connection:
        for number, (column, value, _) in enumerate(cases, 20):
            connection.execute(
                "insert into raw.crash_rows "
                "select batch_id, %s, crash_id, report_state, report_number, "
                "report_date, report_time, report_seq_no, dot_number, add_date, "
                "change_date, fatalities, injuries, tow_away, federal_recordable "
                "from raw.crash_rows where batch_id = %s and source_row_number = 1",
                (number, "b" * 64),
            )
            connection.execute(
                sql.SQL(
                    "update raw.crash_rows set {} = %s "
                    "where batch_id = %s and source_row_number = %s"
                ).format(sql.Identifier(column)),
                (value, "b" * 64, number),
            )
        connection.execute(
            "update raw.snapshot_batches set row_count = row_count + %s "
            "where batch_id = %s",
            (len(cases), "b" * 64),
        )
    run_dbt(tmp_path, "run", "--select", "path:dbt/models/clean")
    with psycopg.connect(POSTGRES_DSN) as connection:
        rows = connection.execute(
            "select parse_reasons from clean.clean_crashes "
            "where source_row_number >= 20 order by source_row_number"
        ).fetchall()
    assert rows == [([reason],) for _, _, reason in cases]


def test_missing_crash_identity_is_excluded_and_hash_preserves_null(
    tmp_path: Path,
) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "update raw.crash_rows set crash_id = null, report_seq_no = null "
            "where batch_id = %s and source_row_number = 1",
            ("b" * 64,),
        )
        connection.execute(
            "update raw.crash_rows set fatalities = null, injuries = null, "
            "tow_away = null where batch_id = %s and source_row_number = 1",
            ("d" * 64,),
        )
    run_dbt(tmp_path, "run", "--select", "path:dbt/models/clean")
    with psycopg.connect(POSTGRES_DSN) as connection:
        excluded = connection.execute(
            "select model_eligibility, exclusion_reason from clean.clean_crashes "
            "where batch_id = %s and source_row_number = 1",
            ("b" * 64,),
        ).fetchone()
        original_hash = connection.execute(
            "select record_hash from clean.clean_crashes "
            "where batch_id = %s and source_row_number = 1",
            ("d" * 64,),
        ).fetchone()
        connection.execute(
            "update raw.crash_rows set fatalities = '0', injuries = '0', "
            "tow_away = 'N' "
            "where batch_id = %s and source_row_number = 1",
            ("d" * 64,),
        )
    assert excluded == ("excluded", "missing_source_record_key")
    run_dbt(tmp_path, "run", "--select", "path:dbt/models/clean")
    with psycopg.connect(POSTGRES_DSN) as connection:
        zero_hash = connection.execute(
            "select record_hash from clean.clean_crashes "
            "where batch_id = %s and source_row_number = 1",
            ("d" * 64,),
        ).fetchone()
    assert original_hash != zero_hash


@pytest.mark.parametrize(
    "correction", ["missing_carrier", "not_recordable", "new_incident"]
)
def test_corrections_close_previously_eligible_incident(
    tmp_path: Path, correction: str
) -> None:
    prepare_database(tmp_path)
    assignments = {
        "missing_carrier": "dot_number = null",
        "not_recordable": "federal_recordable = 'N'",
        "new_incident": "report_number = 'CORRECTED', dot_number = '999'",
    }
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            sql.SQL(
                "update raw.crash_rows set {} where batch_id = %s "
                "and source_row_number = 1"
            ).format(sql.SQL(assignments[correction])),
            ("d" * 64,),
        )
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        old_incident = connection.execute(
            "select knowledge_valid_to, is_current from modeled.crash_incidents "
            "where report_number = 'C-2001' and not is_deleted"
        ).fetchall()
        current = connection.execute(
            "select count(*) from modeled.current_events where event_type = 'crash' "
            "and usdot_number = '123'"
        ).fetchone()
    assert old_incident == [(datetime(2026, 5, 5, 12, tzinfo=UTC), False)]
    assert current == (0,)


def test_equal_reported_event_date_is_valid(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "update raw.inspection_rows set mcmis_add_date = '20251130 1200' "
            "where inspection_id = '1001'"
        )
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        first = connection.execute(
            "select reported_date from modeled.event_versions "
            "where source_record_key = '1001' "
            "order by knowledge_valid_from limit 1"
        ).fetchone()
    assert first == (date(2025, 12, 1),)


def test_unpublished_raw_batch_cannot_delete_history(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        before = connection.execute(
            "select * from modeled.event_versions order by event_version_key"
        ).fetchall()
        connection.execute(
            "insert into raw.snapshot_batches values (%s, 'crashes', 'aayw-vxb3', "
            "'2026-06-01 12:00:00+00', 'loaded', 0)",
            ("e" * 64,),
        )
    run_dbt(tmp_path, "run", "--select", "event_versions")
    with psycopg.connect(POSTGRES_DSN) as connection:
        after = connection.execute(
            "select * from modeled.event_versions order by event_version_key"
        ).fetchall()
    assert after == before
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        remaining = connection.execute(
            "select count(*) from modeled.current_events where event_type = 'crash'"
        ).fetchone()
    assert remaining == (0,)


def test_history_contract_rejects_actual_type_drift(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions")
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "alter table modeled.event_versions alter column state type varchar(2)"
        )
    result = execute_dbt(tmp_path, "run", "--select", "event_versions")
    assert result.returncode != 0
    assert "contract" in (result.stdout + result.stderr).lower()


def test_retention_expiry_has_separate_lineage(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        expiry = connection.execute(
            "select source_record_key, batch_id, retention_cutoff "
            "from modeled.inspection_retention_expiries"
        ).fetchall()
    assert expiry == [("1005", "c" * 64, date(2025, 12, 1))]


def test_default_scoring_grid_comes_from_complete_batch_metadata(
    tmp_path: Path,
) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        grid = connection.execute(
            "select distinct scoring_date from modeled.training_features "
            "order by scoring_date"
        ).fetchall()
    assert grid == [(date(2026, 4, 1),), (date(2026, 5, 1),)]


def test_concurrent_replays_recheck_after_serialization(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        before = connection.execute(
            "select * from modeled.event_versions order by event_version_key"
        ).fetchall()
        connection.execute(
            "insert into raw.snapshot_batches values (%s, 'inspections', 'fx4q-ay7w', "
            "'2026-06-01 12:00:00+00', 'loaded', 0)",
            ("e" * 64,),
        )
    run_dbt(tmp_path, "run", "--select", "+event_change_candidates")
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as blocker:
        blocker.execute("select pg_advisory_lock(hashtextextended('inspections', 0))")
        with ThreadPoolExecutor(max_workers=1) as executor:
            job = executor.submit(
                execute_dbt, tmp_path, "run", "--select", "event_versions"
            )
            try:
                deadline = monotonic() + 90
                while monotonic() < deadline:
                    waiting = blocker.execute(
                        "select count(*) from pg_locks where locktype = 'advisory' "
                        "and not granted and objsubid = 1 "
                        "and classid::bigint = "
                        "((hashtextextended('inspections', 0) >> 32) & 4294967295) "
                        "and objid::bigint = "
                        "(hashtextextended('inspections', 0) & 4294967295) "
                        "and database = (select oid from pg_database "
                        "where datname = current_database())"
                    ).fetchone()
                    if waiting == (1,):
                        break
                    sleep(0.1)
                else:
                    raise AssertionError("History must reach the per-feed lock")
                # Applying an empty inspection batch has only this registry effect:
                # no candidate changes and no retention cutoff/deletion evidence.
                # Commit it after the worker's pending-batch cursor was opened.
                blocker.execute("set statement_timeout = '3s'")
                blocker.execute(
                    "insert into modeled.event_version_batches "
                    "(batch_id, feed_name, observed_at) "
                    "values (%s, 'inspections', '2026-06-01 12:00:00+00')",
                    ("e" * 64,),
                )
            finally:
                blocker.execute(
                    "select pg_advisory_unlock(hashtextextended('inspections', 0))"
                )
            result = job.result(timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(POSTGRES_DSN) as connection:
        after = connection.execute(
            "select * from modeled.event_versions order by event_version_key"
        ).fetchall()
        applied = connection.execute(
            "select count(*) from modeled.event_version_batches where batch_id = %s",
            ("e" * 64,),
        ).fetchone()
    assert after == before
    assert applied == (1,)


def test_concurrent_dbt_builds_are_serialized(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "insert into raw.snapshot_batches values (%s, 'crashes', 'aayw-vxb3', "
            "'2026-06-01 12:00:00+00', 'loaded', 0)",
            ("e" * 64,),
        )
    run_dbt(tmp_path, "run", "--select", "+event_change_candidates")
    profiles = [tmp_path / "first", tmp_path / "second"]
    for profile in profiles:
        profile.mkdir()
        write_test_profile(profile, threads=1)
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as blocker:
        blocker.execute("select pg_advisory_lock(hashtextextended('crashes', 0))")
        with ThreadPoolExecutor(max_workers=2) as executor:
            jobs = [
                executor.submit(
                    execute_dbt, profile, "run", "--select", "event_versions"
                )
                for profile in profiles
            ]
            deadline = monotonic() + 90
            try:
                while monotonic() < deadline:
                    waiting = blocker.execute(
                        "select count(*) from pg_locks where locktype = 'advisory' "
                        "and not granted and database = (select oid from pg_database "
                        "where datname = current_database())"
                    ).fetchone()
                    if waiting and waiting[0] >= 2:
                        break
                    sleep(0.1)
                else:
                    raise AssertionError(
                        "Both dbt invocations must reach the serialization barrier"
                    )
            finally:
                blocker.execute(
                    "select pg_advisory_unlock(hashtextextended('crashes', 0))"
                )
            results = [job.result(timeout=90) for job in jobs]
    assert all(result.returncode == 0 for result in results), "\n".join(
        result.stdout + result.stderr for result in results
    )
    with psycopg.connect(POSTGRES_DSN) as connection:
        applied = connection.execute(
            "select count(*) from modeled.event_version_batches where batch_id = %s",
            ("e" * 64,),
        ).fetchone()
    assert applied == (1,)


def test_production_build_does_not_overwrite_raw_inputs(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute("update raw.inspection_rows set report_number = 'KEEP-ME'")
    run_dbt(tmp_path, "build")
    with psycopg.connect(POSTGRES_DSN) as connection:
        counts = connection.execute(
            "select count(*) from raw.inspection_rows where report_number = 'KEEP-ME'"
        ).fetchone()
    assert counts == (7,)


@pytest.mark.parametrize(
    "scoring_dates",
    ["['2026-06-02']", "['2026-13-01']", "[]", "{'2026-06-01': 1}"],
)
def test_scoring_grid_rejects_invalid_explicit_dates(
    tmp_path: Path, scoring_dates: str
) -> None:
    write_test_profile(tmp_path)
    result = execute_dbt(
        tmp_path,
        "compile",
        "--select",
        "training_features",
        "--vars",
        "{scoring_dates: " + scoring_dates + "}",
    )
    assert result.returncode != 0
    assert "ISO month-start" in result.stdout + result.stderr


def test_direct_mutating_dbt_command_requires_live_build_lock(tmp_path: Path) -> None:
    write_test_profile(tmp_path)
    environment = dict(os.environ)
    environment.pop("CARRIER_RISK_DBT_LOCK_PID", None)
    result = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "run",
            "--select",
            "clean_snapshot_batches",
            "--profiles-dir",
            str(tmp_path),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert "python -m orchestration.dbt_cli run" in result.stdout + result.stderr


def test_candidate_publication_rejects_missing_clean_rows(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    run_dbt(tmp_path, "build", "--select", "+event_versions")
    with psycopg.connect(POSTGRES_DSN) as connection:
        before = connection.execute(
            "select * from intermediate.event_change_candidates "
            "order by batch_id, source_record_key nulls first"
        ).fetchall()
        connection.execute(
            "delete from clean.clean_crashes "
            "where batch_id = %s and source_row_number = 1",
            ("d" * 64,),
        )
    result = execute_dbt(tmp_path, "run", "--select", "event_change_candidates")
    assert result.returncode != 0
    assert "Clean candidates do not match pinned complete batches" in result.stdout
    with psycopg.connect(POSTGRES_DSN) as connection:
        after = connection.execute(
            "select * from intermediate.event_change_candidates "
            "order by batch_id, source_record_key nulls first"
        ).fetchall()
    assert before == after
