"""Training and serving use the same explicit warehouse feature encoding."""

from decimal import Decimal

import pytest

from ml.demo_features import demo_feature_values


def feature_row() -> dict[str, object]:
    return {
        "inspections_4m": 2,
        "violations_4m": 3,
        "oos_violations_4m": 1,
        "crashes_24m": 0,
        "violations_per_inspection": Decimal("1.5"),
        "oos_violation_rate": 1 / 3,
        "days_since_last_inspection": 4,
    }


def test_feature_order_accepts_postgres_numeric_values() -> None:
    assert demo_feature_values(feature_row()) == (2, 3, 1, 0, 1.5, 1 / 3, 4)


def test_undefined_oos_rate_has_one_explicit_zero_encoding() -> None:
    row = feature_row() | {
        "violations_4m": 0,
        "oos_violations_4m": 0,
        "violations_per_inspection": 0,
        "oos_violation_rate": None,
    }
    assert demo_feature_values(row) == (2, 0, 0, 0, 0, 0, 4)


@pytest.mark.parametrize(
    "name,value",
    [
        ("violations_4m", None),
        ("crashes_24m", None),
        ("oos_violation_rate", None),
        ("violations_per_inspection", float("nan")),
        ("violations_per_inspection", float("inf")),
        ("violations_4m", -1),
        ("crashes_24m", True),
        ("crashes_24m", "0"),
        ("inspections_4m", 0),
        ("days_since_last_inspection", 0),
    ],
)
def test_invalid_feature_is_rejected_with_its_name(name: str, value: object) -> None:
    with pytest.raises(ValueError, match=name):
        demo_feature_values(feature_row() | {name: value})
