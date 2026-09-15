from __future__ import annotations

from dags.backfill import backfill
from dags.build_tables import build_tables
from dags.load_events import load_events


def test_load_events_waits_deferrably_without_early_acknowledgement() -> None:
    sensor = load_events.task_dict["wait_for_manifests"]

    assert load_events.dag_id == "load_events"
    assert load_events.catchup is False
    assert sensor.deferrable is True
    assert sensor.delete_message_on_reception is False
    assert "load_snapshot" in load_events.task_dict


def test_build_tables_is_asset_driven_and_runs_dbt() -> None:
    assert build_tables.dag_id == "build_tables"
    assert build_tables.catchup is False
    assert build_tables.schedule is not None
    assert set(build_tables.task_dict) == {"build_event_tables"}


def test_backfill_is_manual_and_requires_a_bounded_request() -> None:
    assert backfill.dag_id == "backfill"
    assert backfill.schedule is None
    assert backfill.catchup is False
    assert set(backfill.params) >= {"feed_name", "start_date", "end_date"}
