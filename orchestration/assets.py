"""Shared asset identities connecting the independent Airflow DAGs."""

from airflow.sdk import Asset

RAW_SNAPSHOTS = Asset("carrier-risk://raw-snapshots")
