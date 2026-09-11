"""Observable contracts for monthly datasets and their frozen feature boundary."""

from collections.abc import Iterator
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from ml.dataset import (
    SourceCoverage,
    SourceEvent,
    build_training_dataset,
    generate_scoring_dates,
    load_training_dataset,
    split_purged,
)
from ml.features import FEATURE_NAMES, FeatureRow
from ml.maturity import (
    CrashReportVersion,
    MaturityWatermark,
    add_months,
    calculate_watermark,
    mature_cohort_months,
)
from tests.dbt_support import POSTGRES_DSN


def feature(scoring_date: date = date(2023, 1, 1)) -> FeatureRow:
    return FeatureRow("123", scoring_date, 2, 3, 1, 0, 1.5, 0.333333, 5)


def test_model_columns_are_fixed_and_missing_oos_rate_has_fixed_encoding() -> None:
    row = replace(
        feature(),
        violations_6m=0,
        oos_violations_6m=0,
        violations_per_inspection=0.0,
        oos_violation_rate=None,
    )
    assert FEATURE_NAMES == (
        "inspections_6m",
        "violations_6m",
        "oos_violations_6m",
        "crashes_24m",
        "violations_per_inspection",
        "oos_violation_rate",
        "days_since_last_inspection",
    )
    assert row.numeric_values() == (2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0)
    assert row.oos_violation_rate is None
    with pytest.raises(FrozenInstanceError):
        row.crashes_24m = 100


@pytest.mark.parametrize(
    "values",
    [
        {"usdot_number": "0"},
        {"usdot_number": "-1"},
        {"inspections_6m": 0},
        {"violations_6m": -1},
        {"violations_per_inspection": float("nan")},
        {"oos_violation_rate": float("inf")},
        {"days_since_last_inspection": 0},
    ],
)
def test_feature_boundary_rejects_ineligible_or_nonfinite_values(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        FeatureRow.from_mapping({**feature().__dict__, **values})


def test_database_numeric_values_and_date_are_converted_without_extra_columns() -> None:
    row = FeatureRow.from_mapping(
        {
            **feature().__dict__,
            "violations_per_inspection": Decimal("1.5"),
            "oos_violation_rate": Decimal("0.333333"),
            "current_fleet_size": 400,
        }
    )
    assert row == feature()
    with pytest.raises(ValueError, match="scoring_date"):
        FeatureRow.from_mapping(
            {**feature().__dict__, "scoring_date": datetime.now(UTC)}
        )


def watermark(grace: int = 30, identity: str = "fixture") -> MaturityWatermark:
    cutoff = date(2025, 2, 1)
    reports = []
    for index, month in enumerate(mature_cohort_months(cutoff)):
        reported = month + timedelta(days=grace if index == 0 else 30)
        reports.append(
            CrashReportVersion(
                usdot_number="123",
                report_state="MI",
                report_number=month.isoformat(),
                event_date=month,
                report_time="1200",
                source_record_key=month.isoformat(),
                reported_date=reported,
                federal_recordable=True,
                knowledge_valid_from=datetime.combine(
                    reported, datetime.min.time(), UTC
                ),
            )
        )
    return calculate_watermark(
        reports,
        data_as_of=cutoff,
        computed_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_fingerprint=identity,
        bootstrap_replicates=200,
    )


def coverage() -> SourceCoverage:
    return SourceCoverage(
        date(2022, 7, 1), date(2020, 1, 1), date(2025, 2, 1), ("snapshot-1",)
    )


def source_event(**changes: object) -> SourceEvent:
    values: dict[str, object] = {
        "event_version_key": 1,
        "feed_name": "inspections",
        "source_record_key": "i1",
        "usdot_number": "123",
        "event_date": date(2022, 12, 27),
        "reported_date": date(2022, 12, 28),
        "knowledge_valid_from": datetime(2022, 12, 28, tzinfo=UTC),
        "knowledge_valid_to": None,
        "availability_quality": "source_proxy",
        "record_hash": "hash-1",
        "is_model_eligible": True,
        "is_deleted": False,
        "first_seen_batch_id": "snapshot-1",
        "report_state": None,
        "report_number": None,
        "report_time": None,
    }
    return SourceEvent.from_mapping({**values, **changes})


def test_scoring_grid_respects_both_lookbacks_grace_and_source_coverage() -> None:
    dates = generate_scoring_dates(coverage(), watermark())
    assert dates[0] == date(2023, 1, 1)
    assert dates[-1] == date(2024, 7, 1)
    assert len(dates) == 19
    shorter = replace(coverage(), observed_through=date(2024, 11, 1))
    assert generate_scoring_dates(shorter, watermark())[-1] == date(2024, 4, 1)
    later_crashes = replace(coverage(), crashes_start=date(2021, 6, 15))
    assert generate_scoring_dates(later_crashes, watermark())[0] == date(2023, 7, 1)


def test_missing_maturity_and_excessive_grace_fail_closed() -> None:
    with pytest.raises(ValueError, match="watermark"):
        generate_scoring_dates(coverage(), None)
    with pytest.raises(ValueError, match="grace"):
        generate_scoring_dates(coverage(), watermark(400))


def test_forward_labels_resolve_versions_at_cutoff_and_deduplicate_incidents() -> None:
    crash = source_event(
        event_version_key=2,
        feed_name="crashes",
        source_record_key="c1",
        event_date=date(2023, 2, 1),
        reported_date=date(2023, 3, 1),
        knowledge_valid_from=datetime(2023, 3, 1, tzinfo=UTC),
        knowledge_valid_to=datetime(2025, 3, 1, tzinfo=UTC),
        report_state="MI",
        report_number="incident",
        report_time=1200,
    )
    duplicate = replace(crash, event_version_key=3, source_record_key="c2")
    future_deletion = replace(
        crash,
        event_version_key=4,
        is_deleted=True,
        knowledge_valid_from=datetime(2025, 3, 1, tzinfo=UTC),
        knowledge_valid_to=None,
    )
    dataset = build_training_dataset(
        (feature(),),
        (source_event(), crash, duplicate, future_deletion),
        coverage=coverage(),
        watermark=watermark(),
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )
    assert dataset.rows[0].label == 1
    assert dataset.rows[0].label_incident_count == 1
    assert dataset.row_count == 1
    assert dataset.provenance.availability_quality_counts == (("source_proxy", 3),)


@pytest.mark.parametrize(
    "changes",
    [
        {"reported_date": date(2025, 2, 1)},
        {"knowledge_valid_from": datetime(2025, 2, 1, tzinfo=UTC)},
        {"knowledge_valid_to": datetime(2025, 2, 1, tzinfo=UTC)},
        {"is_deleted": True},
        {"is_model_eligible": False},
        {"usdot_number": None},
        {"event_date": date(2023, 7, 1)},
    ],
)
def test_label_excludes_invisible_deleted_ineligible_and_end_boundary_crashes(
    changes: dict[str, object],
) -> None:
    crash = source_event(
        event_version_key=2,
        feed_name="crashes",
        source_record_key="c1",
        event_date=date(2023, 2, 1),
        reported_date=date(2023, 3, 1),
        knowledge_valid_from=datetime(2023, 3, 1, tzinfo=UTC),
        report_state="MI",
        report_number="incident",
        report_time=1200,
    )
    crash = SourceEvent.from_mapping({**crash.__dict__, **changes})
    dataset = build_training_dataset(
        (feature(),),
        (source_event(), crash),
        coverage=coverage(),
        watermark=watermark(),
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )
    assert dataset.rows[0].label == 0


def test_missing_feature_rows_are_not_silently_treated_as_no_eligible_carriers() -> (
    None
):
    with pytest.raises(ValueError, match="eligible carrier"):
        build_training_dataset(
            (),
            (source_event(),),
            coverage=coverage(),
            watermark=watermark(),
        )


def test_fingerprint_includes_watermark_sources_and_null_feature_representation() -> (
    None
):
    def build(version: str, events: tuple[SourceEvent, ...]):
        return build_training_dataset(
            (feature(),),
            events,
            coverage=coverage(),
            watermark=watermark(identity=version),
            requested_start=date(2023, 1, 1),
            requested_end=date(2023, 1, 1),
        )

    original = build("v1", (source_event(),))
    assert original.fingerprint == build("v1", (source_event(),)).fingerprint
    assert original.fingerprint != build("v2", (source_event(),)).fingerprint
    changed = replace(source_event(), record_hash="changed")
    assert original.fingerprint != build("v1", (changed,)).fingerprint
    later = replace(source_event(), knowledge_valid_to=datetime(2025, 3, 1, tzinfo=UTC))
    correction = replace(
        later,
        event_version_key=2,
        record_hash="future",
        knowledge_valid_from=datetime(2025, 3, 1, tzinfo=UTC),
        knowledge_valid_to=None,
    )
    assert original.fingerprint == build("v1", (correction, later)).fingerprint


def test_purge_reserves_calendar_months_even_with_no_rows_in_some_months() -> None:
    dates = tuple(add_months(date(2023, 1, 1), offset) for offset in range(15))
    # A carrier qualifies in each month, but a different row count per month must
    # never move either boundary.
    inspections = tuple(
        source_event(
            event_version_key=offset + 1,
            source_record_key=f"i{offset}",
            event_date=add_months(scoring, -1),
            reported_date=add_months(scoring, -1),
            knowledge_valid_from=datetime.combine(
                add_months(scoring, -1), datetime.min.time(), UTC
            ),
        )
        for offset, scoring in enumerate(dates)
    )
    dataset = build_training_dataset(
        tuple(feature(d) for d in dates),
        inspections,
        coverage=coverage(),
        watermark=watermark(),
        requested_end=dates[-1],
    )
    split = split_purged(dataset)
    assert split.train_dates == dates[:6]
    assert split.purged_dates == dates[6:12]
    assert split.test_dates == dates[12:]
    assert len(split.train) == 6
    assert len(split.test) == 3
    sparse = replace(dataset, rows=(dataset.rows[0], dataset.rows[-1]))
    assert split_purged(sparse).purged_dates == dates[6:12]
    with pytest.raises(ValueError, match="fifteen"):
        split_purged(replace(dataset, scoring_dates=dates[:-1], rows=dataset.rows[:-1]))


def test_exclusion_counts_separate_vehicle_rows_incidents_and_unkeyable_rows() -> None:
    missing = source_event(
        event_version_key=2,
        feed_name="crashes",
        source_record_key="missing1",
        usdot_number=None,
        is_model_eligible=False,
        report_state="MI",
        report_number="missing",
        report_time=1200,
    )
    duplicate = replace(missing, event_version_key=3, source_record_key="missing2")
    unkeyable = replace(missing, event_version_key=4, report_number=None)
    dataset = build_training_dataset(
        (feature(),),
        (source_event(), missing, duplicate, unkeyable),
        coverage=coverage(),
        watermark=watermark(),
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )
    assert dataset.provenance.excluded_crash_rows == 3
    assert dataset.provenance.excluded_crash_incidents == 1
    assert dataset.provenance.unkeyable_excluded_crash_rows == 1


@pytest.fixture
def database_schemas() -> Iterator[tuple[str, str]]:
    """Use unique schemas so local integration tests never mutate dbt fixtures."""
    raw_schema = f"dataset_raw_{uuid4().hex}"
    modeled_schema = f"dataset_modeled_{uuid4().hex}"
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        for schema in (raw_schema, modeled_schema):
            connection.execute(
                sql.SQL("create schema {}").format(sql.Identifier(schema))
            )
        try:
            connection.execute(
                sql.SQL("""
                create table {}.snapshot_batches (
                    batch_id text, feed_name text, observed_at timestamptz, status text
                )
            """).format(sql.Identifier(raw_schema))
            )
            connection.execute(
                sql.SQL("""
                create table {}.event_version_batches (
                    batch_id text, feed_name text, observed_at timestamptz
                )
            """).format(sql.Identifier(modeled_schema))
            )
            for batch, feed in (
                ("snapshot-1", "inspections"),
                ("snapshot-2", "crashes"),
            ):
                connection.execute(
                    sql.SQL(
                        "insert into {}.snapshot_batches values (%s,%s,%s,'loaded')"
                    ).format(sql.Identifier(raw_schema)),
                    (batch, feed, datetime(2025, 2, 1, tzinfo=UTC)),
                )
                connection.execute(
                    sql.SQL(
                        "insert into {}.event_version_batches values (%s,%s,%s)"
                    ).format(sql.Identifier(modeled_schema)),
                    (batch, feed, datetime(2025, 2, 1, tzinfo=UTC)),
                )
            connection.execute(
                sql.SQL("""
                create table {}.event_versions (
                    event_version_key bigint, feed_name text, source_record_key text,
                    usdot_number text, event_date date, reported_date date,
                    knowledge_valid_from timestamptz, knowledge_valid_to timestamptz,
                    availability_quality text, record_hash text,
                    is_model_eligible boolean,
                    is_deleted boolean, first_seen_batch_id text,
                    report_state text, report_number text, report_time integer
                )
            """).format(sql.Identifier(modeled_schema))
            )
            values = tuple(source_event().__dict__.values())
            connection.execute(
                sql.SQL("insert into {}.event_versions values ({})").format(
                    sql.Identifier(modeled_schema),
                    sql.SQL(",").join(sql.Placeholder() for _ in values),
                ),
                values,
            )
            connection.execute(
                sql.SQL("""
                create table {}.training_features (
                    usdot_number text, scoring_date date, inspections_6m bigint,
                    violations_6m bigint, oos_violations_6m bigint, crashes_24m bigint,
                    violations_per_inspection numeric, oos_violation_rate numeric,
                    days_since_last_inspection integer
                )
            """).format(sql.Identifier(modeled_schema))
            )
            values = tuple(feature().__dict__.values())
            connection.execute(
                sql.SQL("insert into {}.training_features values ({})").format(
                    sql.Identifier(modeled_schema),
                    sql.SQL(",").join(sql.Placeholder() for _ in values),
                ),
                values,
            )
            yield raw_schema, modeled_schema
        finally:
            for schema in (modeled_schema, raw_schema):
                connection.execute(
                    sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
                )


def test_postgres_extract_verifies_applied_sources_and_reads_native_features(
    database_schemas: tuple[str, str],
) -> None:
    raw_schema, modeled_schema = database_schemas
    result = load_training_dataset(
        POSTGRES_DSN,
        coverage=replace(coverage(), batch_ids=("snapshot-1", "snapshot-2")),
        watermark=watermark(),
        modeled_schema=modeled_schema,
        raw_schema=raw_schema,
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )
    assert result.rows[0].features == feature()
    assert result.rows[0].label == 0
    with pytest.raises(ValueError, match="incomplete"):
        load_training_dataset(
            POSTGRES_DSN,
            coverage=replace(coverage(), batch_ids=("not-applied",)),
            watermark=watermark(),
            modeled_schema=modeled_schema,
            raw_schema=raw_schema,
        )


def test_postgres_missing_feed_and_extraction_limit_fail_closed(
    database_schemas: tuple[str, str],
) -> None:
    raw_schema, modeled_schema = database_schemas
    with pytest.raises(ValueError, match="cutoff"):
        load_training_dataset(
            POSTGRES_DSN,
            coverage=coverage(),
            watermark=watermark(),
            modeled_schema=modeled_schema,
            raw_schema=raw_schema,
        )
    with psycopg.connect(POSTGRES_DSN) as connection:
        connection.execute(
            sql.SQL(
                "insert into {}.event_versions select * from {}.event_versions"
            ).format(sql.Identifier(modeled_schema), sql.Identifier(modeled_schema))
        )
    with pytest.raises(ValueError, match="row limit"):
        load_training_dataset(
            POSTGRES_DSN,
            coverage=replace(coverage(), batch_ids=("snapshot-1", "snapshot-2")),
            watermark=watermark(),
            modeled_schema=modeled_schema,
            raw_schema=raw_schema,
            max_source_rows=1,
        )
