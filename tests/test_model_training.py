"""A frozen candidate is evaluated on the same time holdout as its comparators."""

from dataclasses import dataclass
from datetime import date

import numpy as np
import pytest

from ml.features import FeatureRow
from ml.train import evaluate_candidate


@dataclass(frozen=True)
class Row:
    features: FeatureRow
    label: int


def rows(scoring_date: date) -> tuple[Row, ...]:
    return tuple(
        Row(
            FeatureRow(
                str(index + 1),
                scoring_date,
                10,
                index,
                0,
                0,
                index / 10,
                None if index == 0 else 0.0,
                1,
            ),
            int(index >= 10),
        )
        for index in range(20)
    )


def test_frozen_candidate_beats_uninformative_crash_baseline() -> None:
    evaluation = evaluate_candidate(rows(date(2022, 1, 1)), rows(date(2023, 1, 1)))
    assert evaluation.metrics["candidate_average_precision"] == 1.0
    assert evaluation.metrics["baseline_average_precision"] == 0.5
    assert evaluation.decision.promote
    assert (
        evaluation.model.predict_proba(np.array([[10, 19, 0, 0, 1.9, 0, 1]]))[0, 1]
        > 0.9
    )


def test_identical_incumbent_preserved_on_tie() -> None:
    training, testing = rows(date(2022, 1, 1)), rows(date(2023, 1, 1))
    incumbent = evaluate_candidate(training, testing).model
    result = evaluate_candidate(training, testing, incumbent=incumbent)
    assert not result.decision.promote
    assert result.decision.reason == "did_not_beat_incumbent"


def test_single_class_period_is_rejected_without_metric_claim() -> None:
    with pytest.raises(ValueError, match="both outcome classes"):
        evaluate_candidate(rows(date(2022, 1, 1)), rows(date(2023, 1, 1))[:10])


def test_overlapping_label_windows_rejected_before_fit() -> None:
    with pytest.raises(ValueError, match="purge"):
        evaluate_candidate(rows(date(2022, 1, 1)), rows(date(2022, 4, 1)))


@pytest.mark.parametrize(
    "values",
    [
        np.array([[0.3, 0.9]]),
        np.array([[np.nan, 0.5]]),
        np.array([[-0.1, 1.1]]),
        np.array([[0.2]]),
        np.array([[0.8, np.inf]]),
    ],
)
def test_incoherent_probabilities_cannot_be_evaluated(values) -> None:
    from ml.train import positive_probabilities

    class InvalidClassifier:
        def predict_proba(self, inputs):
            return values

    with pytest.raises(ValueError, match="probabilities"):
        positive_probabilities(InvalidClassifier(), np.zeros((1, 7)))
