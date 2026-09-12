"""Evaluation for an isolated, retrospective real-data learning experiment."""

from __future__ import annotations

import importlib
from typing import Any

import numpy as np
from numpy.typing import NDArray

DEMO_MODEL_NAME = "carrier-risk-learning-demo"
DEMO_FEATURE_NAMES = (
    "inspections_4m",
    "violations_4m",
    "oos_violations_4m",
    "crashes_24m",
    "violations_per_inspection",
    "oos_violation_rate",
    "days_since_last_inspection",
)
TRAIN_DATE = "2024-02-01"
TEST_DATE = "2024-09-01"


def select_threshold(
    labels: NDArray[np.int64], probabilities: NDArray[np.float64]
) -> float:
    """Maximize training F1; never inspect held-out outcomes for this choice.

    This is an illustrative classification operating point, not a calibrated
    probability guarantee. Threshold selection is optimistic on training data;
    the later cohort measures its generalization without threshold tuning.
    """
    metrics: Any = importlib.import_module("sklearn.metrics")
    precision, recall, thresholds = metrics.precision_recall_curve(
        labels, probabilities
    )
    denominator = precision[:-1] + recall[:-1]
    scores = np.divide(
        2 * precision[:-1] * recall[:-1],
        denominator,
        out=np.zeros_like(denominator),
        where=denominator > 0,
    )
    return float(thresholds[int(np.argmax(scores))])


def evaluate_demo(
    labels: NDArray[np.int64],
    probabilities: NDArray[np.float64],
    prior_crashes: NDArray[np.float64],
    threshold: float,
) -> dict[str, float]:
    """Publish positive detection alongside accuracy and the unchanged baseline."""
    if set(labels.tolist()) != {0, 1}:
        raise ValueError("Evaluation requires both classes")
    if (
        probabilities.shape != labels.shape
        or prior_crashes.shape != labels.shape
        or not np.isfinite(probabilities).all()
        or (probabilities < 0).any()
        or (probabilities > 1).any()
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid evaluation probabilities or threshold")
    metrics: Any = importlib.import_module("sklearn.metrics")
    predictions = probabilities >= threshold
    tn, fp, fn, tp = metrics.confusion_matrix(
        labels, predictions, labels=[0, 1]
    ).ravel()
    return {
        "heldout_rows": float(len(labels)),
        "heldout_prevalence": float(labels.mean()),
        "threshold": threshold,
        "accuracy": float(metrics.accuracy_score(labels, predictions)),
        "always_negative_accuracy": float(1 - labels.mean()),
        "positive_recall": float(metrics.recall_score(labels, predictions)),
        "precision": float(
            metrics.precision_score(labels, predictions, zero_division=0)
        ),
        "f1": float(metrics.f1_score(labels, predictions, zero_division=0)),
        "candidate_average_precision": float(
            metrics.average_precision_score(labels, probabilities)
        ),
        "baseline_average_precision": float(
            metrics.average_precision_score(labels, prior_crashes)
        ),
        "true_negative": float(tn),
        "false_positive": float(fp),
        "false_negative": float(fn),
        "true_positive": float(tp),
    }
