"""Exercise the synthetic fixture CLI, replay, registry, and serving boundary."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path

import mlflow
import psycopg
import pytest
from fastapi.testclient import TestClient
from mlflow import MlflowClient
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from ml.features import FEATURE_NAMES
from ml.train import MODEL_PARAMETERS
from serving.app import create_app
from serving.benchmark import valid_fixture_score
from serving.benchmark_fixture import FIXTURE_DATABASE, main
from serving.model import FIXTURE_MODEL_NAME
from tests import dbt_support


@pytest.fixture
def fixture_database() -> Iterator[str]:
    """Reserve the fixed CLI database and remove only the database this test owns."""
    database_url = make_conninfo(dbt_support.POSTGRES_DSN, dbname=FIXTURE_DATABASE)
    assert conninfo_to_dict(database_url)["host"] in {"localhost", "127.0.0.1", "::1"}
    tracking_uri = mlflow.get_tracking_uri()
    with psycopg.connect(
        make_conninfo(database_url, dbname="postgres"), autocommit=True
    ) as admin:
        # The CLI intentionally accepts one database name. Serialize this test
        # across workers and refuse any pre-existing operator-owned fixture.
        admin.execute("select pg_advisory_lock(hashtext(%s))", (FIXTURE_DATABASE,))
        assert (
            admin.execute(
                "select 1 from pg_database where datname = %s", (FIXTURE_DATABASE,)
            ).fetchone()
            is None
        ), "The fixture database must be absent before this test"
        try:
            yield database_url
        finally:
            mlflow.set_tracking_uri(tracking_uri)
            admin.execute(
                sql.SQL("drop database if exists {}").format(
                    sql.Identifier(FIXTURE_DATABASE)
                )
            )
            assert (
                admin.execute(
                    "select 1 from pg_database where datname = %s", (FIXTURE_DATABASE,)
                ).fetchone()
                is None
            )


def _fixture_snapshot(
    database_url: str,
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    """Compare every stored fixture field, including timestamps, across replay."""
    with psycopg.connect(database_url) as connection:
        features = connection.execute(
            "select * from modeled.training_features "
            "order by usdot_number, scoring_date"
        ).fetchall()
        inspections = connection.execute(
            "select * from modeled.inspections order by usdot_number"
        ).fetchall()
    return features, inspections


def test_fixture_cli_replay_preserves_rows_and_serves_only_in_synthetic_profile(
    fixture_database: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fit and register the real fixture twice, then score through real adapters."""
    directory = tmp_path / "fixture"
    monkeypatch.setenv("CARRIER_RISK_DATABASE_URL", fixture_database)
    monkeypatch.setattr(
        sys, "argv", ["benchmark_fixture", "--output-dir", str(directory)]
    )
    main()
    first = json.loads(capsys.readouterr().out)
    assert first == json.loads((directory / "fixture.json").read_text())
    assert first["validation_fixture"] is True
    assert first["carriers"] == 512
    assert first["model_name"] == FIXTURE_MODEL_NAME
    assert first["model_parameters"] == MODEL_PARAMETERS
    original_rows = _fixture_snapshot(fixture_database)
    assert tuple(len(rows) for rows in original_rows) == (512, 512)
    assert {row[0] for row in original_rows[0]} == {str(i) for i in range(1, 513)}

    main()
    replay = json.loads(capsys.readouterr().out)
    assert replay == json.loads((directory / "fixture.json").read_text())
    assert _fixture_snapshot(fixture_database) == original_rows
    assert replay["model_version"] != first["model_version"]
    client = MlflowClient(tracking_uri=replay["tracking_uri"])
    champion = client.get_model_version_by_alias(FIXTURE_MODEL_NAME, "champion")
    assert str(champion.version) == str(replay["model_version"])
    assert champion.tags["validation_fixture"] == "true"
    assert champion.tags["feature_names"] == ",".join(FEATURE_NAMES)
    run = client.get_run(champion.run_id)
    assert run.info.status == "FINISHED"
    assert run.data.params["fixture_rows"] == "512"
    assert all(
        run.data.params[key] == str(value) for key, value in MODEL_PARAMETERS.items()
    )

    monkeypatch.setenv("MLFLOW_TRACKING_URI", replay["tracking_uri"])
    monkeypatch.setenv("CARRIER_RISK_MODEL_NAME", FIXTURE_MODEL_NAME)
    monkeypatch.setenv("CARRIER_RISK_MODEL_ALIAS", "champion")
    monkeypatch.setenv("CARRIER_RISK_VALIDATION_PROFILE", "synthetic")
    with TestClient(create_app()) as api:
        assert api.get("/health/ready").status_code == 200
        response = api.get("/score/1")
        assert valid_fixture_score(
            response.json(),
            status_code=response.status_code,
            month=replay["features_as_of"],
            usdot="1",
        )
        assert response.json()["model_version"] == str(replay["model_version"])

    monkeypatch.setenv("CARRIER_RISK_VALIDATION_PROFILE", "production")
    with TestClient(create_app()) as api:
        assert api.get("/health/ready").status_code == 503
        response = api.get("/score/1")
        assert response.status_code == 503
        assert response.json()["status"] == "model_unavailable"
        assert "risk_score" not in response.json()
