"""Demo probabilities are explicitly experimental and use four-month features."""

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pytest
from fastapi.testclient import TestClient

from ml.demo_features import DEMO_FEATURE_NAMES
from serving.demo_app import DemoModel, PostgresDemoRepository, create_demo_app


class Classifier:
    def predict_proba(self, values: np.ndarray) -> np.ndarray:
        return np.tile([0.7, 0.3], (len(values), 1))


class Repository:
    def lookup(self, usdot: str, scoring_date: date) -> tuple[float, ...] | None:
        return (2, 3, 1, 0, 1.5, 1 / 3, 4) if usdot == "123" else None

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_score_contains_experimental_snapshot_provenance() -> None:
    app = create_demo_app(
        repository=Repository(), model_loader=lambda: DemoModel(Classifier(), "1", 0.2)
    )
    with TestClient(app) as client:
        response = client.get("/demo/score/123?scoring_date=2024-09-01")
        assert response.status_code == 200
        score = response.json()
        assert score["experimental"] is True
        assert score["validation_fixture"] is False
        assert score["inspection_lookback_months"] == 4
        assert score["predicted_positive"] is True
        assert score["risk_score"] == 0.3
        assert score["data_as_of"] == "2026-09-03"
        assert client.get("/metrics").status_code == 200


def test_demo_refuses_missing_features_and_unbuilt_dates() -> None:
    with TestClient(
        create_demo_app(
            repository=Repository(),
            model_loader=lambda: DemoModel(Classifier(), "1", 0.2),
        )
    ) as client:
        assert client.get("/demo/score/999").status_code == 404
        assert client.get("/demo/score/123?scoring_date=2025-01-01").status_code == 422


def test_example_withholds_real_carrier_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CARRIER_RISK_DEMO_EXAMPLE_USDOT", "123")
    with TestClient(
        create_demo_app(
            repository=Repository(),
            model_loader=lambda: DemoModel(Classifier(), "1", 0.2),
        )
    ) as client:
        result = client.get("/demo/example").json()
        assert result["identifier_redacted"] is True
        assert "usdot_number" not in result


@pytest.mark.parametrize("identifier", ["bad", "0", "999999999999999999999"])
def test_invalid_identifier_is_rejected(identifier: str) -> None:
    with TestClient(
        create_demo_app(
            repository=Repository(),
            model_loader=lambda: DemoModel(Classifier(), "1", 0.2),
        )
    ) as client:
        assert client.get(f"/demo/score/{identifier}").status_code == 422


def test_failed_model_startup_returns_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def unavailable() -> DemoModel:
        raise RuntimeError("private registry configuration")

    with TestClient(
        create_demo_app(repository=Repository(), model_loader=unavailable)
    ) as client:
        assert client.get("/readyz").status_code == 503
        assert client.get("/demo/score/123").status_code == 503
        assert "private" not in client.get("/demo/score/123").text
    assert "model_load" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "private registry configuration" not in caplog.text


@pytest.mark.parametrize("missing_feature", ["violations_4m", "crashes_24m"])
def test_missing_required_warehouse_value_emits_no_probability(
    missing_feature: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = dict(zip(DEMO_FEATURE_NAMES, (2, 0, 0, 0, 0.0, None, 4), strict=True))
    row[missing_feature] = None
    pool = MagicMock()
    connection = pool.connection.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = row
    monkeypatch.setattr("serving.demo_app.ConnectionPool", lambda *a, **k: pool)
    with TestClient(
        create_demo_app(
            repository=PostgresDemoRepository("unused"),
            model_loader=lambda: DemoModel(Classifier(), "1", 0.2),
        )
    ) as client:
        response = client.get("/demo/score/123")
        assert response.status_code == 503
        assert "risk_score" not in response.json()
    assert "feature_lookup" in caplog.text


@pytest.mark.parametrize(
    "vector", [(1, 2), (2, float("nan"), 0, 0, 0, 0, 1), (2, -1, 0, 0, 0, 0, 1)]
)
def test_invalid_feature_vector_emits_no_probability(vector: tuple[float, ...]) -> None:
    class InvalidRepository(Repository):
        def lookup(self, usdot: str, scoring_date: date) -> tuple[float, ...]:
            return vector

    with TestClient(
        create_demo_app(
            repository=InvalidRepository(),
            model_loader=lambda: DemoModel(Classifier(), "1", 0.2),
        )
    ) as client:
        response = client.get("/demo/score/123")
        assert response.status_code == 503
        assert "risk_score" not in response.json()
