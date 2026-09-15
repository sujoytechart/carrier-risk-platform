"""Registry isolation and prediction contract checks at the MLflow boundary."""

import importlib
from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest

from ml.features import FEATURE_NAMES, FeatureRow
from serving.model import LoadedRiskModel, load_promoted_model
from serving.service import ModelUnavailable

FEATURE = FeatureRow("1", date(2026, 9, 1), 2, 3, 1, 0, 1.5, 1 / 3, 2)


@dataclass
class RegistryFixture:
    fixture_tag: str = "false"
    experimental_tag: str = "false"
    feature_names: str = ",".join(FEATURE_NAMES)
    unavailable: bool = False

    def get_model_version_by_alias(self, name: str, alias: str) -> SimpleNamespace:
        if self.unavailable:
            raise RuntimeError("private registry error")
        return SimpleNamespace(
            version="19",
            tags={
                "validation_fixture": self.fixture_tag,
                "experimental": self.experimental_tag,
                "feature_names": self.feature_names,
            },
        )


@pytest.fixture
def registry(monkeypatch: pytest.MonkeyPatch) -> RegistryFixture:
    fixture = RegistryFixture()
    monkeypatch.setattr("serving.model.MlflowClient", lambda **kwargs: fixture)
    monkeypatch.setattr("serving.model.mlflow.set_tracking_uri", lambda uri: None)
    return fixture


@pytest.fixture
def fitted_model() -> Any:
    ensemble = importlib.import_module("sklearn.ensemble")
    classifier = ensemble.GradientBoostingClassifier(n_estimators=2, random_state=42)
    values = np.asarray([[1, 0, 0, 0, 0, 0, 2], [2, 4, 2, 1, 2, 0.5, 4]])
    classifier.fit(values, np.asarray([0, 1]))
    return classifier


def test_promoted_alias_resolves_to_immutable_version_and_predicts(
    registry: RegistryFixture,
    fitted_model: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded_uris = []

    def load(uri: str) -> Any:
        loaded_uris.append(uri)
        return fitted_model

    monkeypatch.setattr("serving.model.mlflow.sklearn.load_model", load)
    model = load_promoted_model(tracking_uri="unused", model_name="carrier-risk-v0")
    assert loaded_uris == ["models:/carrier-risk-v0/19"]
    assert model.version == "19"
    assert model.validation_fixture is False
    assert 0 < model.probability(FEATURE) < 1


def test_production_loader_rejects_experimental_version(
    registry: RegistryFixture,
) -> None:
    registry.experimental_tag = "true"
    with pytest.raises(ModelUnavailable):
        load_promoted_model(tracking_uri="unused", model_name="carrier-risk-v0")


@pytest.mark.parametrize(
    ("name", "profile", "tag"),
    [
        ("carrier-risk-fixture", False, "true"),
        ("carrier-risk-v0", True, "false"),
        ("carrier-risk-v0", False, "true"),
        ("carrier-risk-fixture", True, "false"),
    ],
)
def test_synthetic_scoring_requires_matching_profile_name_and_tag(
    registry: RegistryFixture,
    name: str,
    profile: bool,
    tag: str,
) -> None:
    registry.fixture_tag = tag
    with pytest.raises(ModelUnavailable):
        load_promoted_model(
            tracking_uri="unused",
            model_name=name,
            validation_fixture=profile,
        )


def test_matching_synthetic_registry_model_is_explicitly_tagged(
    registry: RegistryFixture,
    fitted_model: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry.fixture_tag = "true"
    monkeypatch.setattr(
        "serving.model.mlflow.sklearn.load_model", lambda uri: fitted_model
    )
    model = load_promoted_model(
        tracking_uri="unused",
        model_name="carrier-risk-fixture",
        validation_fixture=True,
    )
    assert model.validation_fixture is True


def test_missing_alias_is_model_unavailable(registry: RegistryFixture) -> None:
    registry.unavailable = True
    with pytest.raises(ModelUnavailable, match="No usable promoted classifier"):
        load_promoted_model(tracking_uri="unused", model_name="carrier-risk-v0")


@pytest.mark.parametrize("attribute", ["classes_", "n_features_in_"])
def test_wrong_classifier_contract_is_rejected_before_serving(
    registry: RegistryFixture,
    fitted_model: Any,
    monkeypatch: pytest.MonkeyPatch,
    attribute: str,
) -> None:
    setattr(
        fitted_model, attribute, np.asarray([1, 2]) if attribute == "classes_" else 8
    )
    monkeypatch.setattr(
        "serving.model.mlflow.sklearn.load_model", lambda uri: fitted_model
    )
    with pytest.raises(ModelUnavailable):
        load_promoted_model(tracking_uri="unused", model_name="carrier-risk-v0")


def test_malformed_prediction_shape_is_a_typed_model_failure(fitted_model: Any) -> None:
    fitted_model.predict_proba = lambda values: np.asarray([[0.2]])
    model = LoadedRiskModel(fitted_model, "1", False)
    with pytest.raises(ModelUnavailable):
        model.probability(FEATURE)


@pytest.mark.parametrize(
    "values", [[0.2, 0.2], [float("nan"), 0.5], [-0.1, 0.5], [1.2, 0.5]]
)
def test_invalid_probability_distribution_is_rejected(
    fitted_model: Any, values: list[float]
) -> None:
    fitted_model.predict_proba = lambda features: np.asarray([values])
    model = LoadedRiskModel(fitted_model, "1", False)
    with pytest.raises(ModelUnavailable):
        model.probability(FEATURE)


def test_same_length_reordered_model_features_are_rejected(
    registry: RegistryFixture,
) -> None:
    registry.feature_names = ",".join(reversed(FEATURE_NAMES))
    with pytest.raises(ModelUnavailable):
        load_promoted_model(tracking_uri="unused", model_name="carrier-risk-v0")
