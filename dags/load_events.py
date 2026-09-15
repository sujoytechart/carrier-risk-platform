"""SQS-triggered raw snapshot loading DAG."""

from __future__ import annotations

import os
from typing import Any, cast

from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.sdk import XComArg, dag, task

from orchestration.assets import RAW_SNAPSHOTS
from orchestration.load_service import QueueMessage, process_queue_message
from orchestration.runtime import RuntimeSettings, create_load_dependencies


@dag(
    dag_id="load_events",
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
    tags=["carrier-risk", "phase-1"],
)
def _load_events() -> None:
    wait_for_manifests = SqsSensor(
        task_id="wait_for_manifests",
        sqs_queue=os.getenv(
            "CARRIER_RISK_QUEUE_URL",
            "http://elasticmq:9324/000000000000/carrier-risk-arrivals",
        ),
        aws_conn_id="aws_default",
        max_messages=10,
        num_batches=1,
        wait_time_seconds=20,
        visibility_timeout=3600,
        delete_message_on_reception=False,
        deferrable=True,
        poke_interval=30,
    )

    @task(task_id="extract_messages")
    def extract_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Move the sensor's named XCom into a map-compatible task return."""
        return messages

    @task(
        task_id="load_snapshot",
        retries=4,
        retry_exponential_backoff=True,
        outlets=[RAW_SNAPSHOTS],
    )
    def load_snapshot(message: dict[str, Any]) -> list[dict[str, object]]:
        """Load one SQS delivery and acknowledge it only after commit."""
        queue_message = QueueMessage(
            body=str(message["Body"]),
            receipt_handle=str(message["ReceiptHandle"]),
        )
        dependencies = create_load_dependencies(RuntimeSettings.from_environment())
        return [
            {
                "batch_id": result.batch_id,
                "inserted_rows": result.inserted_rows,
                "already_loaded": result.already_loaded,
            }
            for result in process_queue_message(queue_message, dependencies)
        ]

    sensor_messages = cast(
        list[dict[str, Any]],
        XComArg(wait_for_manifests, key="messages"),
    )
    messages = extract_messages(sensor_messages)
    mapped_loads = load_snapshot.expand(message=messages)
    wait_for_manifests >> mapped_loads


load_events = _load_events()
