"""Source proxies must stay strictly earlier than feature scoring dates."""

from datetime import date

import pytest

from ml.demo_source import inspection_event


def test_inspection_normalization_preserves_proxy_and_four_month_counts() -> None:
    row = {
        "INSPECTION_ID": "12.0",
        "DOT_NUMBER": "00123",
        "INSP_DATE": "20240129",
        "MCMIS_ADD_DATE": "20240130 1000",
        "VIOL_TOTAL": "3",
        "OOS_TOTAL": "1",
        "CHANGE_DATE": "",
    }
    event = inspection_event(row)
    assert event[:5] == (
        "inspection",
        "12",
        "123",
        date(2024, 1, 29),
        date(2024, 1, 31),
    )
    assert event[5:7] == (3, 1)


def test_invalid_source_counts_are_excluded() -> None:
    with pytest.raises(ValueError):
        inspection_event(
            {
                "INSPECTION_ID": "12",
                "DOT_NUMBER": "123",
                "INSP_DATE": "20240129",
                "MCMIS_ADD_DATE": "20240130 1000",
                "VIOL_TOTAL": "-1",
                "OOS_TOTAL": "1",
                "CHANGE_DATE": "",
            }
        )
