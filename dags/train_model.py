"""Monthly branching between a measured skip and purged model evaluation."""

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from airflow.sdk import dag, task

from ml.monthly import run_from_environment


@dag(
    dag_id="train_model",
    schedule="@monthly",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["carrier-risk", "phase-3"],
)
def _train_model() -> None:
    @task(task_id="refresh_maturity", retries=2, retry_exponential_backoff=True)
    def refresh_maturity() -> dict[str, Any]:
        """Save the measured policy before choosing a fitting or skip task."""
        return asdict(run_from_environment("measure"))

    @task.branch(task_id="choose_training_path")
    def choose_training_path(result: dict[str, Any]) -> str:
        """Branch on the validated persisted policy, never a caller-set grace."""
        return (
            "fit_model" if result["status"] == "eligible" else "record_skipped_training"
        )

    @task(task_id="fit_model", retries=2, retry_exponential_backoff=True)
    def fit_model(result: dict[str, Any]) -> str:
        """Revalidate the exact policy under the serialized training lock."""
        return run_from_environment(
            "train", expected_watermark_id=str(result["watermark_id"])
        ).status

    @task(task_id="record_skipped_training")
    def record_skipped_training(result: dict[str, Any]) -> str:
        """Log a durable MLflow skip without generating rows or fitting a model."""
        return run_from_environment(
            "train", expected_watermark_id=str(result["watermark_id"])
        ).status

    policy = refresh_maturity()
    branch = choose_training_path(policy)
    branch >> [fit_model(policy), record_skipped_training(policy)]


train_model = _train_model()
