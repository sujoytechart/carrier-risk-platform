"""Observable score, readiness, and instrumentation contracts without network I/O."""

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

from ml.features import FeatureRow
from serving.app import create_app
from serving.service import FeatureLookup, FeatureStoreUnavailable, ModelUnavailable

NOW = datetime(2026, 9, 11, 12, tzinfo=UTC)
FEATURE = FeatureRow("1", date(2026, 9, 1), 3, 4, 1, 1, 4 / 3, 0.25, 2)


@dataclass
class MemoryRepository:
    result: FeatureLookup = FeatureLookup(True, FEATURE)
    unavailable: bool = False

    def lookup(self, usdot_number: str, scoring_date: date) -> FeatureLookup:
        if self.unavailable:
            raise FeatureStoreUnavailable("private warehouse details")
        return self.result


@dataclass
class FixedClassifier:
    version: str = "7"
    validation_fixture: bool = False
    probability_value: float = 0.31
    unavailable: bool = False

    def probability(self, features: FeatureRow) -> float:
        if self.unavailable:
            raise ModelUnavailable("private model details")
        return self.probability_value


def test_score_contains_probability_resolved_version_and_utc_feature_provenance() -> (
    None
):
    app = create_app(
        repository=MemoryRepository(), model_loader=FixedClassifier, clock=lambda: NOW
    )
    with TestClient(app) as client:
        response = client.get("/score/0001")
        assert response.status_code == 200
        assert response.json() == {
            "status": "scored",
            "usdot_number": "1",
            "risk_score": 0.31,
            "model_version": "7",
            "features_as_of": "2026-09-01",
            "computed_at": "2026-09-11T12:00:00Z",
            "validation_fixture": False,
        }
        assert client.get("/health/ready").json()["model_version"] == "7"


@pytest.mark.parametrize("identifier", ["0", "-1", "1.2", "abc", "1e3"])
def test_invalid_carrier_is_typed_insufficient_history(identifier: str) -> None:
    app = create_app(repository=MemoryRepository(), model_loader=FixedClassifier)
    with TestClient(app) as client:
        response = client.get(f"/score/{identifier}")
        assert response.status_code == 404
        assert response.json()["status"] == "insufficient_history"
        assert response.json()["usdot_number"] is None
        assert "risk_score" not in response.json()


def test_old_monthly_row_cannot_override_current_inspection_ineligibility() -> None:
    app = create_app(
        repository=MemoryRepository(FeatureLookup(False, FEATURE)),
        model_loader=FixedClassifier,
        clock=lambda: NOW,
    )
    with TestClient(app) as client:
        response = client.get("/score/1")
        assert response.status_code == 404
        assert response.json()["status"] == "insufficient_history"


@pytest.mark.parametrize(
    "features",
    [
        None,
        replace(FEATURE, scoring_date=date(2026, 8, 1)),
        replace(FEATURE, scoring_date=date(2026, 10, 1)),
        replace(FEATURE, usdot_number="2"),
    ],
)
def test_missing_stale_future_or_wrong_carrier_features_fail_closed(
    features: FeatureRow | None,
) -> None:
    app = create_app(
        repository=MemoryRepository(FeatureLookup(True, features)),
        model_loader=FixedClassifier,
        clock=lambda: NOW,
    )
    with TestClient(app) as client:
        response = client.get("/score/1")
        assert response.status_code == 503
        assert response.json()["status"] == "features_unavailable"
        assert "risk_score" not in response.json()


def test_absent_promoted_model_keeps_liveness_but_refuses_scores() -> None:
    def missing_model() -> FixedClassifier:
        raise ModelUnavailable("private storage location")

    app = create_app(repository=MemoryRepository(), model_loader=missing_model)
    with TestClient(app) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 503
        response = client.get("/score/1")
        assert response.status_code == 503
        assert response.json()["status"] == "model_unavailable"
        assert "private" not in response.text
        assert "carrier_risk_model_available 0.0" in client.get("/metrics").text


def test_warehouse_failure_is_typed_and_does_not_expose_connection_details() -> None:
    app = create_app(
        repository=MemoryRepository(unavailable=True), model_loader=FixedClassifier
    )
    with TestClient(app) as client:
        response = client.get("/score/1")
        assert response.status_code == 503
        assert response.json()["status"] == "warehouse_unavailable"
        assert "private" not in response.text


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.01, 1.01])
def test_malformed_model_probability_is_unavailable(value: float) -> None:
    app = create_app(
        repository=MemoryRepository(),
        model_loader=lambda: FixedClassifier(probability_value=value),
        clock=lambda: NOW,
    )
    with TestClient(app) as client:
        response = client.get("/score/1")
        assert response.status_code == 503
        assert response.json()["status"] == "model_unavailable"


def test_prediction_failure_and_synthetic_provenance_are_explicit() -> None:
    app = create_app(
        repository=MemoryRepository(),
        model_loader=lambda: FixedClassifier(unavailable=True),
        clock=lambda: NOW,
    )
    with TestClient(app) as client:
        assert client.get("/score/1").json()["status"] == "model_unavailable"
    fixture_app = create_app(
        repository=MemoryRepository(),
        model_loader=lambda: FixedClassifier(validation_fixture=True),
        clock=lambda: NOW,
    )
    with TestClient(fixture_app) as client:
        assert client.get("/score/1").json()["validation_fixture"] is True


def test_metrics_have_bounded_outcome_labels_and_count_successes_and_refusals() -> None:
    app = create_app(
        repository=MemoryRepository(), model_loader=FixedClassifier, clock=lambda: NOW
    )
    with TestClient(app) as client:
        client.get("/score/1")
        client.get("/score/invalid")
        metrics = client.get("/metrics")
        assert metrics.headers["content-type"].startswith("text/plain")
        assert 'carrier_risk_score_requests_total{outcome="scored"} 1.0' in metrics.text
        assert 'outcome="insufficient_history"} 1.0' in metrics.text
        assert (
            'carrier_risk_score_request_duration_seconds_count{outcome="scored"} 1.0'
            in metrics.text
        )
        assert "usdot_number=" not in metrics.text
        assert "carrier_risk_model_available 1.0" in metrics.text


def test_openapi_documents_typed_success_and_refusal_schemas() -> None:
    schema = create_app().openapi()
    responses = schema["paths"]["/score/{usdot_number}"]["get"]["responses"]
    assert responses["200"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "ScoreResponse"
    )
    assert responses["404"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "FailureResponse"
    )


def test_missing_warehouse_configuration_keeps_health_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CARRIER_RISK_DATABASE_URL", raising=False)
    app = create_app(model_loader=FixedClassifier)
    with TestClient(app) as client:
        assert client.get("/score/1").json()["status"] == "warehouse_unavailable"
        assert client.get("/health/ready").status_code == 503


def test_unknown_validation_profile_cannot_load_a_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CARRIER_RISK_VALIDATION_PROFILE", "unknown")
    app = create_app(repository=MemoryRepository())
    with TestClient(app) as client:
        assert client.get("/score/1").json()["status"] == "model_unavailable"


@pytest.mark.parametrize("startup_unavailable", [False, True])
def test_managed_warehouse_startup_and_shutdown_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
    startup_unavailable: bool,
) -> None:
    class ManagedRepository(MemoryRepository):
        closed = False

        def open(self) -> None:
            if startup_unavailable:
                raise FeatureStoreUnavailable("cannot connect")

        def close(self) -> None:
            self.closed = True

    repository = ManagedRepository()
    monkeypatch.setenv("CARRIER_RISK_DATABASE_URL", "injected-test-connection")
    monkeypatch.setattr("serving.app.PostgresFeatureRepository", lambda url: repository)
    app = create_app(model_loader=FixedClassifier, clock=lambda: NOW)
    with TestClient(app) as client:
        response = client.get("/score/1")
        assert response.status_code == (503 if startup_unavailable else 200)
        assert client.get("/health/ready").status_code == response.status_code
        assert repository.closed is False
    assert repository.closed is True
