from __future__ import annotations

from pathlib import Path

import yaml


def test_local_stack_contains_queue_warehouse_and_airflow_runtime() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text())
    services = compose["services"]

    assert set(services) >= {"postgres", "minio", "elasticmq", "airflow"}
    assert services["elasticmq"]["image"].startswith("softwaremill/elasticmq")
    assert services["airflow"]["command"] == ["airflow", "standalone"]
    assert services["airflow"]["depends_on"]["postgres"]["condition"] == (
        "service_healthy"
    )
    assert services["airflow"]["depends_on"]["elasticmq"]["condition"] == (
        "service_started"
    )


def test_local_services_bind_only_to_loopback() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    services = yaml.safe_load(compose_path.read_text())["services"]

    for service in services.values():
        for port in service.get("ports", []):
            assert port.startswith("127.0.0.1:")


def test_deferrable_sensor_uses_the_local_queue_endpoint() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    environment = yaml.safe_load(compose_path.read_text())["services"]["airflow"][
        "environment"
    ]

    connection = environment.get("AIRFLOW_CONN_AWS_DEFAULT", "")
    assert "http://elasticmq:9324" in connection
