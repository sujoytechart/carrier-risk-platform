"""Feature order and numeric encoding shared by demo training and serving."""

from collections.abc import Mapping
from decimal import Decimal
from math import isfinite

DEMO_FEATURE_NAMES = (
    "inspections_4m",
    "violations_4m",
    "oos_violations_4m",
    "crashes_24m",
    "violations_per_inspection",
    "oos_violation_rate",
    "days_since_last_inspection",
)


def demo_feature_values(row: Mapping[str, object]) -> tuple[float, ...]:
    """Encode a warehouse row, rejecting missing or invalid required features.

    OOS rate is undefined when there are no violations; only that documented
    null becomes zero. Missing counts must never become evidence of no events.
    """
    values = []
    for name in DEMO_FEATURE_NAMES:
        value = row[name]
        if name == "oos_violation_rate" and value is None and row["violations_4m"] == 0:
            value = 0.0
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ValueError(f"Invalid demo feature: {name}")
        numeric = float(value)
        if not isfinite(numeric) or numeric < 0:
            raise ValueError(f"Invalid demo feature: {name}")
        if name in {"inspections_4m", "days_since_last_inspection"} and numeric < 1:
            raise ValueError(f"Demo feature requires a preceding inspection: {name}")
        values.append(numeric)
    return tuple(values)
