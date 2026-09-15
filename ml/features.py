"""The fixed, events-only feature contract shared by training and serving."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from math import isfinite

FEATURE_NAMES = (
    "inspections_6m",
    "violations_6m",
    "oos_violations_6m",
    "crashes_24m",
    "violations_per_inspection",
    "oos_violation_rate",
    "days_since_last_inspection",
)


@dataclass(frozen=True)
class FeatureRow:
    """An eligible carrier's immutable seven-feature vector at a scoring date.

    Undefined OOS rates remain null in the source row. The frozen model encoding
    maps them to zero because zero violations also means zero OOS violations.
    Present-day carrier attributes never enter this contract.
    """

    usdot_number: str
    scoring_date: date
    inspections_6m: int
    violations_6m: int
    oos_violations_6m: int
    crashes_24m: int
    violations_per_inspection: float
    oos_violation_rate: float | None
    days_since_last_inspection: int

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[1-9][0-9]*", self.usdot_number):
            raise ValueError("usdot_number must be a canonical positive integer")
        if type(self.scoring_date) is not date:
            raise ValueError("scoring_date must be a date")
        for name in (*FEATURE_NAMES[:4], "days_since_last_inspection"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.inspections_6m < 1 or self.days_since_last_inspection < 1:
            raise ValueError("features require a visible preceding inspection")
        for value in (self.violations_per_inspection, self.oos_violation_rate):
            if value is not None and (not isfinite(value) or value < 0):
                raise ValueError("feature rates must be finite and nonnegative")
        if (self.oos_violation_rate is None) != (self.violations_6m == 0):
            raise ValueError("OOS rate is undefined exactly when violations are zero")

    def numeric_values(self) -> tuple[float, ...]:
        """Return the frozen estimator column order and null-rate encoding."""
        return (
            float(self.inspections_6m),
            float(self.violations_6m),
            float(self.oos_violations_6m),
            float(self.crashes_24m),
            float(self.violations_per_inspection),
            0.0 if self.oos_violation_rate is None else float(self.oos_violation_rate),
            float(self.days_since_last_inspection),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> "FeatureRow":
        """Validate a warehouse row and exclude unrelated columns explicitly."""
        usdot = values.get("usdot_number")
        scoring_date = values.get("scoring_date")
        if not isinstance(usdot, str):
            raise ValueError("usdot_number must be text")
        if type(scoring_date) is not date:
            raise ValueError("scoring_date must be a date")
        return cls(
            usdot,
            scoring_date,
            _integer(values, "inspections_6m"),
            _integer(values, "violations_6m"),
            _integer(values, "oos_violations_6m"),
            _integer(values, "crashes_24m"),
            _rate(values.get("violations_per_inspection")),
            None
            if values.get("oos_violation_rate") is None
            else _rate(values["oos_violation_rate"]),
            _integer(values, "days_since_last_inspection"),
        )


def _integer(values: Mapping[str, object], name: str) -> int:
    value = values.get(name)
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def _rate(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int, Decimal)):
        raise ValueError("feature rates must be numeric")
    return float(value)
