from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

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


def test_tracking_and_airflow_use_separate_migration_databases() -> None:
    """Both tools own public.alembic_version and cannot share that database."""
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    services = yaml.safe_load(compose_path.read_text())["services"]
    command = services["mlflow"]["command"]
    tracking = urlparse(command[command.index("--backend-store-uri") + 1])
    airflow_uri = services["airflow"]["environment"][
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    ]
    airflow = urlparse(re.sub(r"\$\{[^}]+\}", "test-value", airflow_uri))
    assert (tracking.hostname, tracking.path) != (airflow.hostname, airflow.path)
    assert services["mlflow"]["depends_on"][tracking.hostname]["condition"] == (
        "service_healthy"
    )


def test_api_waits_for_registry_readiness_before_loading_its_model() -> None:
    """An early failed alias lookup otherwise persists until the API restarts."""
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    services = yaml.safe_load(compose_path.read_text())["services"]
    assert services["api"]["depends_on"]["mlflow"]["condition"] == ("service_healthy")
    assert "/health" in " ".join(services["mlflow"]["healthcheck"]["test"])
