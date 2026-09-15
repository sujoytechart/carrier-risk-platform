"""Manually prove the real-data learning path without bypassing the v0 gate."""

from datetime import UTC, datetime

from airflow.sdk import dag, task


@dag(
    dag_id="train_demo_model",
    schedule=None,
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["carrier-risk", "experimental", "real-data"],
)
def _train_demo_model() -> None:
    @task(retries=1)
    def extract_sources() -> str:
        """Verify reconciled file fingerprints and load the isolated demo schema."""
        from ml.demo_pipeline import run_demo

        run_demo("extract")
        return "extracted"

    @task(retries=1)
    def build_features(_: str) -> str:
        """Run contracted dbt models and boundary tests under the build lock."""
        from ml.demo_pipeline import run_demo

        run_demo("build")
        return "built"

    @task(retries=0)
    def train_and_register(_: str) -> str:
        """Publish measured holdout evidence and an isolated experimental version."""
        from ml.demo_pipeline import run_demo

        return str(run_demo("train")["model_version"])

    train_and_register(build_features(extract_sources()))


train_demo_model = _train_demo_model()
