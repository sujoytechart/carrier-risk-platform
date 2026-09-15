"""Manual bounded replay of immutable snapshot manifests."""

from __future__ import annotations

from datetime import date
from typing import Any, cast

import boto3
from airflow.sdk import Param, dag, get_current_context, task

from orchestration.backfill_service import discover_backfill_manifests
from orchestration.load_service import load_manifest
from orchestration.messages import ManifestReference
from orchestration.runtime import (
    RuntimeSettings,
    create_dbt_runner,
    create_load_dependencies,
)


@dag(
    dag_id="backfill",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "feed_name": Param("inspections", enum=["inspections", "crashes"]),
        "start_date": Param(type="string", format="date"),
        "end_date": Param(type="string", format="date"),
    },
    tags=["carrier-risk", "phase-1", "backfill"],
)
def _backfill() -> None:
    @task(task_id="discover_manifests")
    def discover_manifests() -> list[dict[str, str | None]]:
        """Return ordered manifest references for the requested date interval."""
        params = get_current_context()["params"]
        settings = RuntimeSettings.from_environment()
        client = boto3.client(
            "s3",
            region_name=settings.aws_region,
            endpoint_url=settings.s3_endpoint_url,
        )
        references = discover_backfill_manifests(
            client=client,
            bucket=settings.raw_bucket,
            feed_name=str(params["feed_name"]),
            start_date=date.fromisoformat(str(params["start_date"])),
            end_date=date.fromisoformat(str(params["end_date"])),
        )
        return [
            {
                "bucket": reference.bucket,
                "key": reference.key,
                "version_id": reference.version_id,
            }
            for reference in references
        ]

    @task(task_id="load_backfill_manifest", retries=4, retry_exponential_backoff=True)
    def load_backfill_manifest(reference: dict[str, Any]) -> str:
        """Load one discovered manifest through the normal raw-load service."""
        settings = RuntimeSettings.from_environment()
        result = load_manifest(
            ManifestReference(
                bucket=str(reference["bucket"]),
                key=str(reference["key"]),
                version_id=(
                    str(reference["version_id"])
                    if reference.get("version_id") is not None
                    else None
                ),
            ),
            create_load_dependencies(settings),
        )
        return result.batch_id

    @task(task_id="build_backfill_tables", retries=2, retry_exponential_backoff=True)
    def build_backfill_tables(_: list[str]) -> None:
        """Build all pending batches after every raw transaction commits."""
        settings = RuntimeSettings.from_environment()
        create_dbt_runner(settings).build()

    loaded_batches = load_backfill_manifest.expand(reference=discover_manifests())
    build_backfill_tables(cast(list[str], loaded_batches))


backfill = _backfill()
