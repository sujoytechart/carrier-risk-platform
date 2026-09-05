"""Environment-backed construction of production orchestration dependencies."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import boto3
import psycopg

from ingest.object_store import S3SnapshotObjectStore
from ingest.raw_loader import RawSnapshotLoader
from orchestration.aws_adapters import S3ManifestRepository, SqsMessageAcknowledger
from orchestration.dbt_lock import PostgresBuildLock
from orchestration.dbt_runner import DbtRunner
from orchestration.load_service import LoadDependencies


@dataclass(frozen=True)
class RuntimeSettings:
    """Validated configuration needed by Airflow task processes."""

    raw_bucket: str
    queue_url: str
    database_url: str
    aws_region: str
    project_dir: Path
    profiles_dir: Path
    s3_endpoint_url: str | None = None
    sqs_endpoint_url: str | None = None

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        """Read required settings and fail with the missing variable's name."""
        return cls(
            raw_bucket=_required_environment("CARRIER_RISK_RAW_BUCKET"),
            queue_url=_required_environment("CARRIER_RISK_QUEUE_URL"),
            database_url=_required_environment("CARRIER_RISK_DATABASE_URL"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            project_dir=Path(
                os.getenv("CARRIER_RISK_PROJECT_DIR", "/opt/carrier-risk")
            ),
            profiles_dir=Path(
                os.getenv("CARRIER_RISK_DBT_PROFILES_DIR", "/opt/carrier-risk/config")
            ),
            s3_endpoint_url=os.getenv("CARRIER_RISK_S3_ENDPOINT_URL"),
            sqs_endpoint_url=os.getenv("CARRIER_RISK_SQS_ENDPOINT_URL"),
        )


def create_load_dependencies(settings: RuntimeSettings) -> LoadDependencies:
    """Construct AWS and PostgreSQL adapters for one Airflow task process."""
    session = boto3.Session(region_name=settings.aws_region)
    s3_client = session.client("s3", endpoint_url=settings.s3_endpoint_url)
    sqs_client = session.client("sqs", endpoint_url=settings.sqs_endpoint_url)
    loader = RawSnapshotLoader(
        connection_factory=lambda: psycopg.connect(settings.database_url),
        object_store=S3SnapshotObjectStore(settings.raw_bucket, s3_client),
    )
    return LoadDependencies(
        manifest_repository=S3ManifestRepository(s3_client),
        loader=loader,
        acknowledger=SqsMessageAcknowledger(
            client=sqs_client,
            queue_url=settings.queue_url,
        ),
        expected_bucket=settings.raw_bucket,
    )


def create_dbt_runner(settings: RuntimeSettings) -> DbtRunner:
    """Construct a dbt runner using the task process's existing environment."""
    return DbtRunner(
        project_dir=settings.project_dir,
        profiles_dir=settings.profiles_dir,
        build_lock=PostgresBuildLock(
            lambda: psycopg.connect(settings.database_url, autocommit=True)
        ),
    )


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value
