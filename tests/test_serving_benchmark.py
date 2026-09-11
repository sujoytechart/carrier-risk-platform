"""A benchmark must reject error-only, misrouted, and untagged responses."""

from pathlib import Path

import pytest

from serving.benchmark import (
    nearest_rank_percentile,
    run_load,
    score_failure_reason,
    valid_fixture_score,
)
from serving.benchmark_fixture import fixture_rows, prepare_fixture

PAYLOAD: dict[str, object] = {
    "status": "scored",
    "usdot_number": "1",
    "risk_score": 0.3,
    "model_version": "7",
    "features_as_of": "2026-09-01",
    "validation_fixture": True,
}


def test_actual_synthetic_score_is_accepted() -> None:
    assert valid_fixture_score(PAYLOAD, status_code=200, month="2026-09-01", usdot="1")


def test_transport_exception_is_preserved_for_diagnosis() -> None:
    error = ConnectionError("connection reset")
    assert score_failure_reason(0, None, error) is error


def test_typed_api_failure_keeps_http_and_domain_status() -> None:
    assert score_failure_reason(503, {"status": "model_unavailable"}, None) == (
        "Expected synthetic score; HTTP 503; status=model_unavailable"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "model_unavailable"),
        ("usdot_number", "2"),
        ("risk_score", None),
        ("risk_score", float("nan")),
        ("risk_score", 1.01),
        ("risk_score", True),
        ("model_version", "champion"),
        ("model_version", "0"),
        ("features_as_of", "2026-08-01"),
        ("validation_fixture", False),
    ],
)
def test_benchmark_refuses_non_scores_or_wrong_provenance(
    field: str, value: object
) -> None:
    assert not valid_fixture_score(
        {**PAYLOAD, field: value}, status_code=200, month="2026-09-01", usdot="1"
    )


def test_non_json_and_failed_http_responses_cannot_pass() -> None:
    assert not valid_fixture_score(None, status_code=200, month="2026-09-01", usdot="1")
    assert not valid_fixture_score(
        PAYLOAD, status_code=503, month="2026-09-01", usdot="1"
    )


def test_raw_percentile_retains_tail_without_histogram_rounding() -> None:
    assert (
        nearest_rank_percentile([float(number) for number in range(1, 101)], 0.99) == 99
    )


@pytest.mark.parametrize(
    "host", ["https://example.com", "http://localhost@example.com", "http://127.1.1.1"]
)
def test_load_generator_never_contacts_remote_hosts(host: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="loopback"):
        run_load(host, tmp_path)


@pytest.mark.parametrize(
    ("seconds", "warmup", "rates"),
    [
        (9, 1, (1,)),
        (10, 0, (1,)),
        (10, 1, ()),
        (10, 1, (0,)),
        (10, 1, (1001,)),
    ],
)
def test_invalid_load_shape_is_rejected_before_starting(
    seconds: int,
    warmup: int,
    rates: tuple[int, ...],
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError):
        run_load(
            "http://127.0.0.1:8002",
            tmp_path,
            seconds=seconds,
            warmup=warmup,
            rates=rates,
        )


@pytest.mark.parametrize(
    "dsn",
    [
        "host=remote.example dbname=carrier_risk_phase3_serving",
        "host=localhost dbname=production",
    ],
)
def test_fixture_setup_cannot_mutate_other_databases(dsn: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="dedicated loopback"):
        prepare_fixture(tmp_path, dsn)


def test_synthetic_fixture_has_512_deterministic_valid_rows() -> None:
    rows = fixture_rows()
    assert rows == fixture_rows()
    assert len(rows) == 512
    assert len({row.usdot_number for row in rows}) == 512
    assert all(len(row.numeric_values()) == 7 for row in rows)
