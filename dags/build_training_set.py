"""Monthly immutable, maturity-gated training dataset generation."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

from ml.monthly import run_from_environment


@dag(
    dag_id="build_training_set",
    schedule="@monthly",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["carrier-risk", "phase-3"],
)
def _build_training_set() -> None:
    @task(task_id="build_monthly_dataset", retries=2, retry_exponential_backoff=True)
    def build_monthly_dataset() -> str:
        """Persist the measured policy and build only a permitted scoring grid."""
        return run_from_environment("build").status

    build_monthly_dataset()


build_training_set = _build_training_set()
