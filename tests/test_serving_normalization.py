"""The HTTP identifier boundary accepts only positive integral USDOT numbers."""

import pytest

from serving.service import normalize_usdot


@pytest.mark.parametrize("value", ["0", "-1", "+1", "1.0", "1e3", "١", " ", "9" * 30])
def test_invalid_usdot_is_ineligible(value: str) -> None:
    assert normalize_usdot(value) is None


@pytest.mark.parametrize("value", ["1", "001", " 001 "])
def test_positive_integral_usdot_is_normalized(value: str) -> None:
    assert normalize_usdot(value) == "1"
