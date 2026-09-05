"""Asset-driven dbt build for clean and modeled event relations."""

from __future__ import annotations

from airflow.sdk import dag, task

from orchestration.assets import RAW_SNAPSHOTS
from orchestration.runtime import RuntimeSettings, create_dbt_runner


@dag(
    dag_id="build_tables",
    schedule=[RAW_SNAPSHOTS],
    catchup=False,
    max_active_runs=1,
    tags=["carrier-risk", "phase-1", "dbt"],
)
def _build_tables() -> None:
    @task(task_id="build_event_tables", retries=2, retry_exponential_backoff=True)
    def build_event_tables() -> None:
        """Run contracts, modeled builds, and blocking temporal tests."""
        settings = RuntimeSettings.from_environment()
        create_dbt_runner(settings).build()

    build_event_tables()


build_tables = _build_tables()
