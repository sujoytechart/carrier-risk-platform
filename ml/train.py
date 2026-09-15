"""Frozen v0 estimator and evaluation on a purged chronological holdout."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import numpy as np
from numpy.typing import NDArray

from ml.features import FeatureRow
from ml.promote import PromotionDecision, decide_promotion

MODEL_PARAMETERS = {
    "n_estimators": 100,
    "max_depth": 3,
    "learning_rate": 0.1,
    "random_state": 20260911,
}
MODEL_SPECIFICATION = "gradient-boosting-v0-20260911"


class LabeledFeatures(Protocol):
    """Read-only row boundary shared with the reproducible dataset builder."""

    @property
    def features(self) -> FeatureRow: ...

    @property
    def label(self) -> int: ...


class ProbabilityModel(Protocol):
    """A binary estimator with negative then positive probability columns."""

    def predict_proba(self, values: NDArray[np.float64]) -> NDArray[np.float64]: ...


@dataclass(frozen=True)
class Evaluation:
    """One candidate and all measurements made on the identical held-out rows."""

    model: ProbabilityModel
    metrics: dict[str, float]
    decision: PromotionDecision


def positive_probabilities(
    model: ProbabilityModel, values: NDArray[np.float64]
) -> NDArray[np.float64]:
    """Reject malformed estimator output before evaluating or publishing it."""
    probabilities = np.asarray(model.predict_proba(values), dtype=np.float64)
    if (
        probabilities.shape != (len(values), 2)
        or not np.isfinite(probabilities).all()
        or (probabilities < 0).any()
        or (probabilities > 1).any()
        or not np.allclose(probabilities.sum(axis=1), 1.0)
    ):
        raise ValueError("Estimator returned invalid binary probabilities")
    return probabilities[:, 1]


def evaluate_candidate(
    train: Sequence[LabeledFeatures],
    test: Sequence[LabeledFeatures],
    *,
    incumbent: ProbabilityModel | None = None,
) -> Evaluation:
    """Fit the frozen classifier, requiring both classes and six purged dates.

    The monthly dataset builder validates the complete grid. This boundary also
    rejects overlapping windows so direct callers cannot bypass the embargo.
    All comparisons use average precision, which accounts for rare positives.
    """
    for period in (train, test):
        if {row.label for row in period} != {0, 1}:
            raise ValueError("Each evaluation period requires both outcome classes")
    last_train = max(row.features.scoring_date for row in train)
    first_test = min(row.features.scoring_date for row in test)
    months = (
        (first_test.year - last_train.year) * 12 + first_test.month - last_train.month
    )
    if months < 7:
        raise ValueError(
            "A six-month scoring-date purge is required before the holdout"
        )

    train_values = np.asarray([row.features.numeric_values() for row in train])
    test_values = np.asarray([row.features.numeric_values() for row in test])
    labels = np.asarray([row.label for row in test])
    # Keep untyped third-party APIs at this external boundary; the rest of the
    # workflow consumes explicit row and probability contracts.
    ensemble: Any = importlib.import_module("sklearn.ensemble")
    metrics_api: Any = importlib.import_module("sklearn.metrics")
    model = ensemble.GradientBoostingClassifier(**MODEL_PARAMETERS)
    model.fit(train_values, [row.label for row in train])
    candidate_precision = float(
        metrics_api.average_precision_score(
            labels, positive_probabilities(model, test_values)
        )
    )
    baseline_precision = float(
        metrics_api.average_precision_score(
            labels, [row.features.crashes_24m for row in test]
        )
    )
    incumbent_precision = (
        None
        if incumbent is None
        else float(
            metrics_api.average_precision_score(
                labels, positive_probabilities(incumbent, test_values)
            )
        )
    )
    metrics = {
        "candidate_average_precision": candidate_precision,
        "baseline_average_precision": baseline_precision,
        "heldout_prevalence": float(labels.mean()),
        "training_rows": float(len(train)),
        "heldout_rows": float(len(test)),
    }
    if incumbent_precision is not None:
        metrics["incumbent_average_precision"] = incumbent_precision
    return Evaluation(
        cast(ProbabilityModel, model),
        metrics,
        decide_promotion(candidate_precision, baseline_precision, incumbent_precision),
    )
