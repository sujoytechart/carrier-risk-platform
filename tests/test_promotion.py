"""Promotion decisions use comparable time holdouts and strict improvements."""

import math

import pytest

from ml.promote import decide_promotion


def test_candidate_must_beat_baseline_and_incumbent() -> None:
    assert decide_promotion(0.4, 0.3, 0.35).promote
    assert not decide_promotion(0.3, 0.3, None).promote
    assert not decide_promotion(0.4, 0.3, 0.5).promote
    assert not decide_promotion(0.4, 0.3, 0.4).promote


@pytest.mark.parametrize(
    "candidate,baseline,incumbent",
    [
        (math.nan, 0.3, None),
        (0.5, math.inf, None),
        (0.5, 0.3, math.nan),
        (-0.1, 0.0, None),
        (1.1, 0.5, None),
    ],
)
def test_invalid_metrics_cannot_promote(
    candidate: float, baseline: float, incumbent: float | None
) -> None:
    with pytest.raises(ValueError, match="precision"):
        decide_promotion(candidate, baseline, incumbent)
