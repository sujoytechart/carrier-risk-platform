"""Load a resolved promoted MLflow model once, with synthetic-model isolation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, cast

import mlflow
import mlflow.sklearn
import numpy as np
from mlflow.tracking import MlflowClient
from numpy.typing import NDArray

from ml.features import FEATURE_NAMES, FeatureRow
from serving.service import ModelUnavailable

FIXTURE_MODEL_NAME = "carrier-risk-fixture"


class ProbabilisticClassifier(Protocol):
    """The fitted sklearn classifier interface required by scoring."""

    classes_: NDArray[np.int64]
    n_features_in_: int

    def predict_proba(self, values: NDArray[np.float64]) -> NDArray[np.float64]:
        """Return one probability per input row and learned class."""
        ...


@dataclass(frozen=True)
class LoadedRiskModel:
    """An immutable registry version and its already-loaded fitted estimator."""

    classifier: ProbabilisticClassifier
    version: str
    validation_fixture: bool

    def probability(self, features: FeatureRow) -> float:
        """Predict the positive class; malformed model output fails closed."""
        try:
            values = np.asarray([features.numeric_values()], dtype=np.float64)
            probabilities = self.classifier.predict_proba(values)
            if probabilities.shape != (1, 2):
                raise ValueError("Classifier must return two class probabilities")
            if (
                not np.isfinite(probabilities).all()
                or (probabilities < 0).any()
                or (probabilities > 1).any()
                or not np.isclose(probabilities[0].sum(), 1.0)
            ):
                raise ValueError("Classifier returned invalid class probabilities")
            return float(probabilities[0, 1])
        except Exception as error:
            raise ModelUnavailable("Promoted classifier could not score") from error


def load_promoted_model(
    *,
    tracking_uri: str,
    model_name: str,
    alias: str = "champion",
    validation_fixture: bool = False,
) -> LoadedRiskModel:
    """Resolve one alias and reject fixture models outside explicit validation.

    Both a separate registry name and an explicit model-version tag are required
    for fixture scoring. A changed alias cannot silently replace a live worker's
    estimator; restart the worker to adopt the next promoted version.
    """
    if validation_fixture != (model_name == FIXTURE_MODEL_NAME):
        raise ModelUnavailable("Synthetic validation requires its isolated model name")
    try:
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
        version = client.get_model_version_by_alias(model_name, alias)
        is_fixture = version.tags.get("validation_fixture") == "true"
        if version.tags.get("experimental") == "true":
            raise ValueError("Experimental models require the separate demo service")
        if is_fixture != validation_fixture:
            raise ValueError("Model validation provenance does not match serving mode")
        if version.tags.get("feature_names") != ",".join(FEATURE_NAMES):
            raise ValueError("Model feature names do not match the frozen input order")
        classifier = cast(
            ProbabilisticClassifier,
            mlflow.sklearn.load_model(f"models:/{model_name}/{version.version}"),
        )
        if classifier.n_features_in_ != len(FEATURE_NAMES) or not np.array_equal(
            classifier.classes_, np.asarray([0, 1])
        ):
            raise ValueError(
                "Promoted model has an incompatible feature/class contract"
            )
        return LoadedRiskModel(classifier, str(version.version), validation_fixture)
    except Exception as error:
        raise ModelUnavailable("No usable promoted classifier is available") from error
