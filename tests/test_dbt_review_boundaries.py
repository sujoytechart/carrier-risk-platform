"""Final temporal review regressions at source and knowledge boundaries."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg

from tests.dbt_support import POSTGRES_DSN, run_dbt
from tests.test_dbt_regressions import prepare_database


def test_unaffected_crash_vehicle_remains_visible_at_exact_correction(
    tmp_path: Path,
) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "insert into raw.snapshot_batches values (%s, 'crashes', 'aayw-vxb3', "
            "'2026-06-01 00:00:00+00', 'loaded', 3)",
            ("e" * 64,),
        )
        connection.execute(
            "insert into raw.crash_rows select %s, source_row_number, crash_id, "
            "report_state, report_number, report_date, report_time, report_seq_no, "
            "dot_number, add_date, change_date, fatalities, injuries, tow_away, "
            "federal_recordable from raw.crash_rows where batch_id = %s",
            ("e" * 64, "d" * 64),
        )
        connection.execute(
            "update raw.crash_rows set fatalities = '7' "
            "where batch_id = %s and crash_id = '2004'",
            ("e" * 64,),
        )
    run_dbt(
        tmp_path,
        "build",
        "--select",
        "+event_versions+",
        "--vars",
        "{scoring_dates: ['2026-06-01']}",
    )
    with psycopg.connect(POSTGRES_DSN) as connection:
        features = connection.execute(
            "select crashes_24m from modeled.training_features "
            "where usdot_number = '789'"
        ).fetchone()
        visible = connection.execute(
            "select count(*), max(fatalities) from modeled.events_union "
            "where usdot_number = '789' and event_type = 'crash' "
            "and knowledge_valid_from < '2026-06-01' "
            "and (knowledge_valid_to is null or knowledge_valid_to > '2026-06-01') "
            "and reported_date < '2026-06-01' and event_date < '2026-06-01' "
            "and not is_deleted"
        ).fetchone()
        current = connection.execute(
            "select count(*), max(fatalities) from modeled.current_events "
            "where event_type = 'crash' and usdot_number = '789'"
        ).fetchone()
    assert features == (1,)
    assert visible == (1, 2)
    assert current == (1, 7)


def test_excluded_future_correction_closes_history_without_impossible_event(
    tmp_path: Path,
) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "update raw.crash_rows set report_date = '20300101' "
            "where batch_id = %s and crash_id = '2001'",
            ("d" * 64,),
        )
    run_dbt(tmp_path, "build", "--select", "+event_versions+")
    with psycopg.connect(POSTGRES_DSN) as connection:
        correction = connection.execute(
            "select event_date, is_model_eligible, exclusion_reason "
            "from modeled.event_versions where source_record_key = '2001' "
            "and is_current"
        ).fetchone()
        predecessor = connection.execute(
            "select knowledge_valid_to from modeled.event_versions "
            "where source_record_key = '2001' and not is_current"
        ).fetchone()
        quarantine = connection.execute(
            "select parse_reasons from clean.clean_crashes "
            "where batch_id = %s and source_record_key = '2001'",
            ("d" * 64,),
        ).fetchone()
    assert correction == (None, False, "impossible_event_chronology")
    assert predecessor == (datetime(2026, 5, 5, 12, tzinfo=UTC),)
    assert quarantine == (["impossible_event_chronology"],)


def test_nonnull_blank_values_are_quarantined_in_both_feeds(tmp_path: Path) -> None:
    prepare_database(tmp_path)
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            "update raw.crash_rows set crash_id = '', dot_number = ' ', "
            "report_date = '', add_date = ' ', change_date = '', report_time = ' ', "
            "report_seq_no = '', fatalities = ' ', injuries = '', tow_away = ' ', "
            "federal_recordable = '' where batch_id = %s and source_row_number = 1",
            ("b" * 64,),
        )
        connection.execute(
            "update raw.inspection_rows set inspection_id = '', dot_number = ' ', "
            "insp_date = '', mcmis_add_date = ' ', change_date = '', viol_total = ' ', "
            "oos_total = '' where batch_id = %s and source_row_number = 1",
            ("a" * 64,),
        )
    run_dbt(tmp_path, "run", "--select", "path:dbt/models/clean")
    with psycopg.connect(POSTGRES_DSN) as connection:
        crashes = connection.execute(
            "select parse_reasons from clean.clean_crashes "
            "where batch_id = %s and source_row_number = 1",
            ("b" * 64,),
        ).fetchone()
        inspections = connection.execute(
            "select parse_reasons from clean.clean_inspections "
            "where batch_id = %s and source_row_number = 1",
            ("a" * 64,),
        ).fetchone()
    assert crashes == (
        [
            "invalid_source_record_key",
            "invalid_usdot_number",
            "invalid_event_date",
            "invalid_source_add_at",
            "invalid_source_change_at",
            "invalid_report_time",
            "invalid_report_seq_no",
            "invalid_fatalities",
            "invalid_injuries",
            "invalid_tow_away",
            "invalid_federal_recordable",
        ],
    )
    assert inspections == (
        [
            "invalid_source_record_key",
            "invalid_usdot_number",
            "invalid_event_date",
            "invalid_source_add_at",
            "invalid_source_change_at",
            "invalid_violation_count",
            "invalid_oos_violation_count",
        ],
    )
