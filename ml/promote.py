"""Strict promotion against the baseline and incumbent on one time holdout."""

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class PromotionDecision:
    """A measured decision; ties preserve the incumbent."""

    promote: bool
    reason: str


def decide_promotion(
    candidate_precision: float,
    baseline_precision: float,
    incumbent_precision: float | None,
) -> PromotionDecision:
    """Require strict average-precision improvement over every comparator."""
    values = [candidate_precision, baseline_precision]
    if incumbent_precision is not None:
        values.append(incumbent_precision)
    if any(not isfinite(value) or not 0 <= value <= 1 for value in values):
        raise ValueError("Average precision must be finite and between zero and one")
    if candidate_precision <= baseline_precision:
        return PromotionDecision(False, "did_not_beat_baseline")
    if incumbent_precision is not None and candidate_precision <= incumbent_precision:
        return PromotionDecision(False, "did_not_beat_incumbent")
    return PromotionDecision(True, "beats_baseline_and_incumbent")
