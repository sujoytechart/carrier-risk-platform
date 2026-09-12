"""The learning experiment keeps its holdout and model identity explicit."""

import numpy as np
import pytest

from ml.demo_training import evaluate_demo, select_threshold


def test_threshold_is_chosen_on_training_predictions() -> None:
    labels = np.array([0, 0, 1, 1])
    assert select_threshold(labels, np.array([0.1, 0.2, 0.7, 0.8])) == 0.7


def test_metrics_keep_positive_detection_and_baseline_visible() -> None:
    result = evaluate_demo(
        np.array([0, 0, 1, 1]),
        np.array([0.1, 0.6, 0.4, 0.9]),
        np.array([0, 1, 0, 1]),
        threshold=0.5,
    )
    assert result["accuracy"] == 0.5
    assert result["positive_recall"] == 0.5
    assert result["precision"] == 0.5
    assert result["false_negative"] == 1
    assert result["always_negative_accuracy"] == 0.5
    assert "baseline_average_precision" in result


def test_one_class_evaluation_is_rejected() -> None:
    with pytest.raises(ValueError, match="both classes"):
        evaluate_demo(np.array([0, 0]), np.array([0.1, 0.2]), np.zeros(2), 0.5)
