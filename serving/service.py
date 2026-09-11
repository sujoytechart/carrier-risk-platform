"""Scoring policy independent of HTTP, PostgreSQL, and model storage."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, Protocol

from ml.features import FeatureRow


class FeatureStoreUnavailable(RuntimeError):
    """The warehouse could not complete a bounded feature lookup."""


class ModelUnavailable(RuntimeError):
    """A promoted classifier could not be loaded or produce a valid probability."""


@dataclass(frozen=True)
class FeatureLookup:
    """Current eligibility is independent of a stored monthly feature row."""

    eligible: bool
    features: FeatureRow | None


class FeatureRepository(Protocol):
    """Read the latest visible monthly features and current inspection eligibility."""

    def lookup(self, usdot_number: str, scoring_date: date) -> FeatureLookup:
        """Return a snapshot or raise FeatureStoreUnavailable without secrets."""
        ...


class RiskModel(Protocol):
    """One immutable promoted classifier loaded before requests are accepted."""

    @property
    def version(self) -> str:
        """Return the resolved MLflow registered version, never a mutable alias."""
        ...

    @property
    def validation_fixture(self) -> bool:
        """Identify a synthetic validation model explicitly in every score."""
        ...

    def probability(self, features: FeatureRow) -> float:
        """Return the probability of the positive crash label."""
        ...


FailureStatus = Literal[
    "insufficient_history",
    "features_unavailable",
    "model_unavailable",
    "warehouse_unavailable",
]


@dataclass(frozen=True)
class ScoringFailure:
    """A typed refusal to emit a score outside the serving guarantees."""

    status: FailureStatus
    usdot_number: str | None
    computed_at: datetime


@dataclass(frozen=True)
class ScoredCarrier:
    """An eligible carrier's positive-class probability and temporal provenance."""

    usdot_number: str
    risk_score: float
    model_version: str
    features_as_of: date
    computed_at: datetime
    validation_fixture: bool
    status: Literal["scored"] = "scored"


def normalize_usdot(value: str) -> str | None:
    """Normalize ASCII integral identifiers bounded by PostgreSQL bigint."""
    candidate = value.strip()
    if not re.fullmatch(r"[0-9]{1,19}", candidate):
        return None
    number = int(candidate)
    return str(number) if 0 < number <= 9_223_372_036_854_775_807 else None


def score_carrier(
    identifier: str,
    *,
    repository: FeatureRepository,
    model: RiskModel | None,
    computed_at: datetime,
) -> ScoredCarrier | ScoringFailure:
    """Score current eligible carriers using a current-month warehouse snapshot.

    Eligibility is evaluated for the UTC request date; reusing a prior monthly
    eligibility flag would incorrectly score inspections that have aged out.
    Missing refreshes and model failures return explicit unavailable states.
    """
    now = computed_at.astimezone(UTC)
    usdot = normalize_usdot(identifier)
    if usdot is None:
        return ScoringFailure("insufficient_history", None, now)
    try:
        lookup = repository.lookup(usdot, now.date())
    except FeatureStoreUnavailable:
        return ScoringFailure("warehouse_unavailable", usdot, now)
    if not lookup.eligible:
        return ScoringFailure("insufficient_history", usdot, now)
    if model is None:
        return ScoringFailure("model_unavailable", usdot, now)
    features = lookup.features
    if (
        features is None
        or features.usdot_number != usdot
        or not now.date().replace(day=1) <= features.scoring_date <= now.date()
    ):
        return ScoringFailure("features_unavailable", usdot, now)
    try:
        probability = model.probability(features)
    except ModelUnavailable:
        return ScoringFailure("model_unavailable", usdot, now)
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        return ScoringFailure("model_unavailable", usdot, now)
    return ScoredCarrier(
        usdot,
        probability,
        model.version,
        features.scoring_date,
        now,
        model.validation_fixture,
    )
