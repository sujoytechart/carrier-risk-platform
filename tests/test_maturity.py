from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta

import pytest

from ml.maturity import (
    CrashReportVersion,
    add_months,
    calculate_watermark,
    mature_cohort_months,
    training_eligibility,
)

AS_OF = date(2026, 9, 3)
COMPUTED_AT = datetime(2026, 9, 11, tzinfo=UTC)


def _report(month: date, lag: int = 30, **overrides: object) -> CrashReportVersion:
    reported = month + timedelta(days=lag)
    values: dict[str, object] = {
        "usdot_number": "123",
        "report_state": "MI",
        "report_number": month.isoformat(),
        "event_date": month,
        "report_time": "1200",
        "source_record_key": month.isoformat(),
        "reported_date": reported,
        "knowledge_valid_from": datetime.combine(reported, datetime.min.time(), UTC),
        "federal_recordable": True,
    }
    values.update(overrides)
    return CrashReportVersion(**values)


def _reports(lag: int = 30) -> list[CrashReportVersion]:
    return [_report(month, lag) for month in mature_cohort_months(AS_OF)]


def _calculate(reports: list[CrashReportVersion], **kwargs: object):
    return calculate_watermark(
        reports,
        data_as_of=AS_OF,
        computed_at=COMPUTED_AT,
        source_fingerprint="fixture-sha256",
        bootstrap_replicates=200,
        **kwargs,
    )


def test_latest_twelve_cohorts_use_calendar_month_end_plus_nine_months() -> None:
    assert mature_cohort_months(AS_OF) == tuple(
        add_months(date(2024, 12, 1), offset) for offset in range(12)
    )
    assert mature_cohort_months(date(2026, 8, 29))[-1] == date(2025, 10, 1)
    assert mature_cohort_months(date(2026, 8, 30))[-1] == date(2025, 11, 1)
    assert add_months(date(2024, 5, 31), 9) == date(2025, 2, 28)


def test_counts_first_incident_version_once_and_excludes_missing_carriers() -> None:
    reports = _reports()
    first = reports[0]
    reports.extend(
        [
            replace(first, source_record_key="other-vehicle"),
            replace(
                first,
                reported_date=first.reported_date + timedelta(days=100),
                knowledge_valid_from=first.knowledge_valid_from + timedelta(days=100),
            ),
            replace(first, usdot_number=None, source_record_key="missing-1"),
            replace(first, usdot_number=None, source_record_key="missing-2"),
            replace(
                first,
                usdot_number="777",
                source_record_key="nonreportable",
                federal_recordable=False,
            ),
        ]
    )
    result = _calculate(reports)
    assert result.sample_size == 12
    assert result.grace_days == 30
    assert result.excluded_missing_usdot_rows == 2
    assert result.excluded_missing_usdot_incidents == 1
    assert result.excluded_nonreportable_incidents == 1
    assert result.cohorts[0].incident_count == 1
    with pytest.raises(FrozenInstanceError):
        result.grace_days = 1


def test_later_correction_cannot_make_initial_nonreportable_incident_reportable() -> (
    None
):
    reports = _reports()
    initial = replace(reports[0], federal_recordable=False)
    reports[0] = initial
    reports.append(
        replace(
            initial,
            federal_recordable=True,
            knowledge_valid_from=initial.knowledge_valid_from + timedelta(days=20),
            reported_date=initial.reported_date + timedelta(days=20),
        )
    )
    result = _calculate(reports)
    assert result.status == "incomplete"
    assert result.grace_days is None
    assert not training_eligibility(
        result, label_window_end=date(2025, 11, 30)
    ).eligible


def test_bootstrap_is_reproducible_order_independent_and_ceil_largest_bound() -> None:
    reports = [
        _report(
            month,
            lag,
            report_number=f"{month}-{lag}",
            source_record_key=f"{month}-{lag}",
        )
        for month in mature_cohort_months(AS_OF)
        for lag in range(1, 201)
    ]
    result = _calculate(reports)
    repeated = _calculate(list(reversed(reports)))
    assert result == repeated
    assert result.grace_days == 200
    assert result.calculation_version
    assert result.bootstrap_seed == 20260911
    assert all(cohort.upper_95_days >= cohort.p995_days for cohort in result.cohorts)
    assert (
        result.watermark_version
        != _calculate(reports, bootstrap_seed=42).watermark_version
    )


def test_missing_or_incomplete_measurement_fails_closed() -> None:
    assert not training_eligibility(None, label_window_end=date(2025, 1, 1)).eligible
    result = _calculate(_reports()[:-1])
    assert result.measured_grace_days is None
    assert result.grace_days is None
    assert result.status == "incomplete"
    assert not training_eligibility(result, label_window_end=date(2025, 1, 1)).eligible
    result = _calculate(_reports(), source_complete=False)
    assert result.status == "incomplete"


def test_increase_is_automatic_but_decrease_preserves_previous_grace_until_review() -> (
    None
):
    previous = _calculate(_reports(60))
    lower = _calculate(_reports(30), previous=previous)
    assert lower.measured_grace_days == 30
    assert lower.grace_days == 60
    assert lower.status == "decrease_pending_review"
    reviewed = _calculate(_reports(30), previous=previous, reviewed_decrease=True)
    assert reviewed.grace_days == 30
    assert reviewed.status == "measured"
    higher = _calculate(_reports(90), previous=previous)
    assert higher.grace_days == 90
    assert higher.status == "measured"


def test_nine_calendar_month_gate_and_label_maturity_are_both_required() -> None:
    over_limit = _calculate(_reports(300))
    decision = training_eligibility(over_limit, label_window_end=date(2025, 1, 1))
    assert not decision.eligible
    assert decision.reason == "grace_exceeds_nine_calendar_months"
    valid = _calculate(_reports(30))
    assert training_eligibility(valid, label_window_end=date(2026, 8, 4)).eligible
    assert not training_eligibility(valid, label_window_end=date(2026, 8, 5)).eligible
    boundary = _calculate(_reports(274))
    assert training_eligibility(boundary, label_window_end=date(2025, 3, 1)).eligible
    assert not training_eligibility(
        boundary, label_window_end=date(2025, 5, 31)
    ).eligible


def test_negative_lag_and_ambiguous_first_version_fail_instead_of_biasing_tail() -> (
    None
):
    reports = _reports()
    invalid = replace(
        reports[0], reported_date=reports[0].event_date - timedelta(days=1)
    )
    with pytest.raises(ValueError, match="negative"):
        _calculate([invalid, *reports[1:]])
    with pytest.raises(ValueError, match="Conflicting"):
        _calculate([*reports, replace(reports[0], federal_recordable=False)])


def test_as_of_cannot_see_future_versions_or_accept_stale_watermark() -> None:
    reports = _reports()
    reports[0] = replace(reports[0], knowledge_valid_from=COMPUTED_AT)
    assert _calculate(reports).status == "incomplete"
    with pytest.raises(ValueError, match="older"):
        calculate_watermark(
            _reports(),
            data_as_of=date(2026, 8, 1),
            computed_at=COMPUTED_AT,
            source_fingerprint="fixture-sha256",
            previous=_calculate(_reports()),
        )


def test_parquet_measurement_verifies_lineage_and_emits_only_aggregate_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    import hashlib
    import json
    import sys

    import pyarrow as arrow
    import pyarrow.parquet as parquet

    from ml.maturity_cli import main, measure_parquet_snapshot

    rows = [
        {
            "CRASH_ID": report.source_record_key,
            "DOT_NUMBER": report.usdot_number,
            "REPORT_STATE": report.report_state,
            "REPORT_NUMBER": report.report_number,
            "REPORT_DATE": report.event_date.strftime("%Y%m%d"),
            "REPORT_TIME": report.report_time,
            "REPORT_SEQ_NO": "1",
            "FEDERAL_RECORDABLE": "Y",
            "ADD_DATE": (report.knowledge_valid_from - timedelta(days=1)).strftime(
                "%Y%m%d %H%M"
            ),
        }
        for report in _reports()
    ]
    rows.extend(
        [
            {**rows[0], "DOT_NUMBER": "", "REPORT_SEQ_NO": sequence}
            for sequence in ("1", "2")
        ]
    )
    rows.append({**rows[1], "DOT_NUMBER": "42", "ADD_DATE": "bad timestamp"})
    snapshot = tmp_path / "snapshot.parquet"
    parquet.write_table(arrow.Table.from_pylist(rows), snapshot)
    lineage = {
        "parquet_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
        "source_manifest": {
            "observed_at": "2026-09-03T00:07:10+00:00",
            "row_count": len(rows),
            "object_sha256": "source-object-sha256",
            "batch_id": "fixture-batch-id",
        },
        "reconciliation": {
            "source_row_count": len(rows),
            "parquet_row_count": len(rows),
            "source_value_sha256": "matching-values",
            "parquet_value_sha256": "matching-values",
        },
    }
    lineage_path = tmp_path / "lineage.json"
    lineage_path.write_text(json.dumps(lineage))
    evidence = measure_parquet_snapshot(
        snapshot, lineage_path, computed_at=COMPUTED_AT, bootstrap_replicates=200
    )
    assert evidence["watermark"]["grace_days"] == 30
    assert evidence["source_audit"]["excluded_missing_usdot_rows"] == 2
    assert evidence["source_audit"]["excluded_missing_usdot_incidents"] == 1
    assert evidence["source_audit"]["selected_cohort_parse_exclusions"] == {
        "invalid_source_add_at": 1
    }
    assert '"usdot_number":' not in json.dumps(evidence)
    assert evidence["limitations"]
    output = tmp_path / "aggregate-evidence.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "maturity",
            "--parquet",
            str(snapshot),
            "--lineage",
            str(lineage_path),
            "--output",
            str(output),
            "--computed-at",
            COMPUTED_AT.isoformat(),
            "--bootstrap-replicates",
            "200",
        ],
    )
    main()
    assert json.loads(output.read_text())["watermark"] == evidence["watermark"]
    lineage["parquet_sha256"] = "mismatch"
    lineage_path.write_text(json.dumps(lineage))
    with pytest.raises(ValueError, match="checksum"):
        measure_parquet_snapshot(snapshot, lineage_path, computed_at=COMPUTED_AT)


def test_watermark_roundtrip_rejects_tampering_and_missing_policy_fields(
    tmp_path,
) -> None:
    import json

    from ml.maturity import read_watermark, watermark_from_dict

    measured = _calculate(_reports())
    payload = json.loads(measured.to_json())
    assert watermark_from_dict(payload) == measured
    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps({"watermark": payload}))
    assert read_watermark(evidence) == measured
    for field, invalid in [
        ("grace_days", 1),
        ("sample_size", 0),
        ("quantile", 0.95),
        ("source_complete", "true"),
    ]:
        with pytest.raises(ValueError):
            watermark_from_dict({**payload, field: invalid})
    del payload["label_definition"]
    with pytest.raises(ValueError):
        watermark_from_dict(payload)
    fabricated = replace(measured, grace_days=-1)
    assert not training_eligibility(
        fabricated, label_window_end=date(2025, 1, 1)
    ).eligible


def test_source_identity_correction_does_not_create_a_second_first_incident() -> None:
    reports = _reports()
    correction = replace(
        reports[0],
        report_number="corrected-incident-key",
        knowledge_valid_from=reports[0].knowledge_valid_from + timedelta(days=100),
        reported_date=reports[0].reported_date + timedelta(days=100),
    )
    result = _calculate([*reports, correction])
    assert result.sample_size == 12
    assert result.grace_days == 30


def test_upstream_missing_carrier_exclusions_are_part_of_watermark_fingerprint() -> (
    None
):
    basic = _calculate(_reports())
    excluded = _calculate(
        _reports(),
        source_excluded_missing_usdot_rows=10,
        source_excluded_missing_usdot_incidents=8,
    )
    assert excluded.excluded_missing_usdot_rows == 10
    assert excluded.excluded_missing_usdot_incidents == 8
    assert excluded.watermark_version != basic.watermark_version


@pytest.mark.parametrize("missing_cohort", [False, True])
def test_incomplete_recovery_preserves_policy_floor_until_review(
    missing_cohort: bool,
) -> None:
    import json

    from ml.maturity import watermark_from_dict

    measured = _calculate(_reports(90))
    incomplete = _calculate(
        _reports(10)[:-1] if missing_cohort else _reports(10),
        source_complete=missing_cohort,
        previous=measured,
    )
    assert incomplete.status == "incomplete"
    assert incomplete.measured_grace_days is None
    assert incomplete.grace_days == 90
    restored = watermark_from_dict(json.loads(incomplete.to_json()))
    assert restored == incomplete
    assert (
        training_eligibility(restored, label_window_end=date(2025, 1, 1)).reason
        == "maturity_incomplete"
    )

    recovered = _calculate(_reports(10), previous=restored)
    assert recovered.measured_grace_days == 10
    assert recovered.grace_days == 90
    assert recovered.status == "decrease_pending_review"
    assert watermark_from_dict(json.loads(recovered.to_json())) == recovered
    approved = _calculate(_reports(10), previous=recovered, reviewed_decrease=True)
    assert approved.grace_days == 10
    assert approved.status == "measured"


def test_repeated_incomplete_updates_retain_the_last_effective_policy_floor() -> None:
    measured = _calculate(_reports(90))
    first_incomplete = _calculate([], source_complete=False, previous=measured)
    second_incomplete = _calculate([], source_complete=False, previous=first_incomplete)
    assert second_incomplete.grace_days == 90
    assert not training_eligibility(
        second_incomplete, label_window_end=date(2025, 1, 1)
    ).eligible
    recovered = _calculate(_reports(10), previous=second_incomplete)
    assert recovered.grace_days == 90
    assert recovered.status == "decrease_pending_review"


def test_initial_incomplete_policy_has_no_invented_floor() -> None:
    import json

    from ml.maturity import watermark_from_dict

    initial = _calculate([], source_complete=False)
    assert initial.grace_days is None
    assert watermark_from_dict(json.loads(initial.to_json())) == initial
    recovered = _calculate(_reports(10), previous=initial)
    assert recovered.grace_days == 10
    assert recovered.status == "measured"
