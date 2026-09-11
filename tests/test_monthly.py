"""Monthly orchestration must skip before data extraction or model fitting."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from ml.maturity import (
    CrashReportVersion,
    add_months,
    calculate_watermark,
    mature_cohort_months,
)
from ml.monthly import MonthlyDependencies, run_monthly, training_branch
from ml.tracking import TrainingRunResult
from ml.watermark_store import PostgresWatermarkStore
from tests.dbt_support import POSTGRES_DSN
from tests.test_training_dataset import database_schemas as _database_schemas
from tests.test_training_dataset import watermark

database_schemas = _database_schemas


def test_build_with_excessive_maturity_never_extracts_features() -> None:
    def unexpected_build(_):
        pytest.fail("blocked maturity must not build features")

    result = run_monthly(
        "build",
        MonthlyDependencies(
            lambda: watermark(400),
            unexpected_build,
            lambda policy, factory: pytest.fail("build cannot fit"),
        ),
    )
    assert result.status == "skipped"
    assert result.reason == "grace_exceeds_nine_calendar_months"


def test_training_passes_lazy_dataset_factory_to_the_tracking_gate() -> None:
    policy = watermark(400)

    def track(actual_policy, factory):
        assert actual_policy == policy
        return TrainingRunResult(
            "skip-run", "skipped", "grace_exceeds_nine_calendar_months"
        )

    result = run_monthly(
        "train",
        MonthlyDependencies(
            lambda: policy,
            lambda _: pytest.fail("skip cannot build rows"),
            track,
        ),
    )
    assert result.run_id == "skip-run"
    assert result.status == "skipped"


def test_airflow_branch_uses_validated_policy() -> None:
    assert training_branch(watermark(400)) == "record_skipped_training"
    assert training_branch(watermark()) == "fit_model"


def test_monthly_dags_are_scheduled_and_training_has_explicit_branch() -> None:
    from dags.build_training_set import build_training_set
    from dags.train_model import train_model

    assert build_training_set.catchup is False
    assert build_training_set.max_active_runs == 1
    assert set(build_training_set.task_dict) == {"build_monthly_dataset"}
    assert train_model.catchup is False
    assert set(train_model.task_dict) == {
        "refresh_maturity",
        "choose_training_path",
        "fit_model",
        "record_skipped_training",
    }
    assert train_model.task_dict["choose_training_path"].downstream_task_ids == {
        "fit_model",
        "record_skipped_training",
    }
    from airflow.serialization.serialized_objects import DagSerialization
    from airflow.timetables.base import TimeRestriction

    for pipeline in (build_training_set, train_model):
        assert pipeline.start_date == datetime(2026, 9, 1, tzinfo=UTC)
        serialized = DagSerialization.from_dict(DagSerialization.to_dict(pipeline))
        restriction = TimeRestriction(
            earliest=serialized.start_date, latest=None, catchup=False
        )
        scheduled = serialized.timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=restriction,
        )
        assert scheduled is not None
        assert scheduled.data_interval.start.day == 1
        assert scheduled.data_interval.end.day == 1
        following = serialized.timetable.next_dagrun_info(
            last_automated_data_interval=scheduled.data_interval,
            restriction=restriction,
        )
        assert following is not None
        assert following.run_after.date() == add_months(scheduled.run_after.date(), 1)


def test_dataset_freeze_rejects_conflicting_bytes(tmp_path: Path) -> None:
    from datetime import date

    from ml.dataset import build_training_dataset
    from ml.monthly import freeze_dataset
    from tests.test_training_dataset import coverage, feature, source_event

    dataset = build_training_dataset(
        (feature(),),
        (source_event(),),
        coverage=coverage(),
        watermark=watermark(),
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )
    path = freeze_dataset(dataset, tmp_path)
    assert freeze_dataset(dataset, tmp_path) == path
    assert '"usdot_number": "123"' in path.read_text()
    path.write_text("conflicting bytes")
    with pytest.raises(ValueError, match="immutable"):
        freeze_dataset(dataset, tmp_path)


def test_watermark_registry_replays_immutably_and_rejects_unreviewed_replacement() -> (
    None
):
    schema = f"maturity_store_{uuid4().hex}"
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema)
        try:
            first = watermark()
            assert store.latest() is None
            assert store.persist(first) == first
            assert store.persist(first) == first
            assert store.latest() == first
            with pytest.raises(ValueError, match="previous"):
                store.persist(watermark(identity="different"))
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(
                    sql.SQL(
                        "update {}.label_maturity_watermarks set quantile = 0.99"
                    ).format(sql.Identifier(schema))
                )
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )


def test_watermark_registry_preserves_prior_grace_until_decrease_review() -> None:
    schema = f"maturity_store_{uuid4().hex}"
    cutoff = date(2025, 2, 1)
    reports = [
        CrashReportVersion(
            usdot_number="123",
            report_state="MI",
            report_number=str(month),
            event_date=month,
            reported_date=month + timedelta(days=10),
            report_time="1200",
            source_record_key=str(month),
            federal_recordable=True,
            knowledge_valid_from=datetime.combine(
                month + timedelta(days=10), datetime.min.time(), UTC
            ),
        )
        for month in mature_cohort_months(cutoff)
    ]
    first = watermark()
    pending = calculate_watermark(
        reports,
        data_as_of=cutoff,
        computed_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_fingerprint="new",
        previous=first,
        bootstrap_replicates=200,
    )
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema)
        try:
            store.persist(first)
            assert store.persist(pending).grace_days == 30
            assert store.latest().status == "decrease_pending_review"
            rows = connection.execute(
                sql.SQL(
                    "select count(*), count(*) filter(where is_current) "
                    "from {}.label_maturity_watermarks"
                ).format(sql.Identifier(schema))
            ).fetchone()
            assert rows == (2, 1)
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )


def test_warehouse_measurement_uses_first_versions_and_flags_unkeyable_history(
    database_schemas: tuple[str, str],
) -> None:
    from dataclasses import replace

    from tests.test_training_dataset import source_event

    raw_schema, modeled_schema = database_schemas
    cutoff = date(2025, 2, 1)
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        connection.execute(
            sql.SQL(
                "alter table {}.event_versions add column exclusion_reason text"
            ).format(sql.Identifier(modeled_schema))
        )
        columns = sql.SQL(",").join(
            sql.Identifier(name) for name in source_event().__dict__
        )
        for index, month in enumerate(mature_cohort_months(cutoff)):
            reported = month + timedelta(days=30)
            event = source_event(
                event_version_key=index + 2,
                feed_name="crashes",
                source_record_key=f"crash{index}",
                event_date=month,
                reported_date=reported,
                knowledge_valid_from=datetime.combine(
                    reported, datetime.min.time(), UTC
                ),
                first_seen_batch_id="snapshot-2",
                report_state="MI",
                report_number=str(month),
                report_time=1200,
            )
            values = tuple(event.__dict__.values())
            query = sql.SQL("insert into {}.event_versions ({}) values ({})").format(
                sql.Identifier(modeled_schema),
                columns,
                sql.SQL(",").join(sql.Placeholder() for _ in values),
            )
            connection.execute(query, values)
        store = PostgresWatermarkStore(
            connection, schema=modeled_schema, raw_schema=raw_schema
        )
        measured = store.measure(
            data_as_of=cutoff, computed_at=datetime(2025, 2, 1, tzinfo=UTC)
        )
        assert measured.grace_days == 30
        assert measured.sample_size == 12
        assert (
            store.measure(
                data_as_of=cutoff, computed_at=datetime(2025, 2, 2, tzinfo=UTC)
            )
            == measured
        )
        connection.execute(
            sql.SQL(
                "update {}.event_versions set is_deleted=true where event_version_key=2"
            ).format(sql.Identifier(modeled_schema))
        )
        deleted = store.measure(
            data_as_of=cutoff, computed_at=datetime(2025, 2, 2, tzinfo=UTC)
        )
        assert deleted.sample_size == 11
        assert deleted.status == "incomplete"
        connection.execute(
            sql.SQL(
                "update {}.event_versions set is_deleted=false "
                "where event_version_key=2"
            ).format(sql.Identifier(modeled_schema))
        )
        invalid = replace(
            event,
            event_version_key=1000,
            source_record_key="unkeyable",
            report_number=None,
        )
        connection.execute(query, tuple(invalid.__dict__.values()))
        incomplete = store.measure(
            data_as_of=cutoff, computed_at=datetime(2025, 2, 1, tzinfo=UTC)
        )
        assert incomplete.grace_days == measured.grace_days
        assert incomplete.status == "incomplete"
        audit = connection.execute(
            sql.SQL(
                "select source_audit from {}.label_maturity_watermarks where is_current"
            ).format(sql.Identifier(modeled_schema))
        ).fetchone()
        assert audit[0]["unkeyable_excluded_rows"] == 1
        with pytest.raises(ValueError, match="row limit"):
            store.measure(
                data_as_of=cutoff,
                computed_at=datetime(2025, 2, 1, tzinfo=UTC),
                max_source_rows=1,
            )


def test_eligible_monthly_build_preserves_actual_row_count() -> None:
    from ml.dataset import build_training_dataset
    from ml.maturity import add_months
    from tests.test_training_dataset import coverage, feature, source_event

    dates = tuple(add_months(date(2023, 1, 1), offset) for offset in range(15))
    inspections = tuple(
        source_event(
            event_version_key=index + 1,
            source_record_key=f"inspection{index}",
            event_date=scoring - timedelta(days=1),
            reported_date=scoring - timedelta(days=1),
            knowledge_valid_from=datetime.combine(
                scoring - timedelta(days=1), datetime.min.time(), UTC
            ),
        )
        for index, scoring in enumerate(dates)
    )
    dataset = build_training_dataset(
        tuple(feature(scoring) for scoring in dates),
        inspections,
        coverage=coverage(),
        watermark=watermark(),
        requested_end=dates[-1],
    )
    dependencies = MonthlyDependencies(
        lambda: watermark(),
        lambda _: dataset,
        lambda policy, factory: pytest.fail("building cannot train"),
    )
    assert run_monthly("measure", dependencies).status == "eligible"
    result = run_monthly("build", dependencies)
    assert result.row_count == 15
    assert result.dataset_fingerprint == dataset.fingerprint


def test_interrupted_dataset_publication_leaves_no_partial_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    from ml.dataset import build_training_dataset
    from ml.monthly import freeze_dataset
    from tests.test_training_dataset import coverage, feature, source_event

    dataset = build_training_dataset(
        (feature(),),
        (source_event(),),
        coverage=coverage(),
        watermark=watermark(),
        requested_start=date(2023, 1, 1),
        requested_end=date(2023, 1, 1),
    )

    def unavailable_link(*args):
        raise OSError("publication interrupted")

    monkeypatch.setattr(os, "link", unavailable_link)
    with pytest.raises(OSError, match="interrupted"):
        freeze_dataset(dataset, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_environment_build_keeps_current_serving_month_and_runs_all_temporal_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    from contextlib import nullcontext

    import ml.monthly as monthly
    from ml.dataset import build_training_dataset
    from tests.test_training_dataset import coverage, feature, source_event

    policy = watermark()
    dates = monthly.generate_scoring_dates(coverage(), policy)
    inspections = tuple(
        source_event(
            event_version_key=index + 1,
            source_record_key=str(scoring),
            event_date=scoring - timedelta(days=1),
            reported_date=scoring - timedelta(days=1),
            knowledge_valid_from=datetime.combine(
                scoring - timedelta(days=1), datetime.min.time(), UTC
            ),
        )
        for index, scoring in enumerate(dates)
    )
    dataset = build_training_dataset(
        tuple(feature(day) for day in dates),
        inspections,
        coverage=coverage(),
        watermark=policy,
    )
    published_features = {}

    class Connection:
        def execute(self, query):
            return self

        def fetchall(self):
            return [("snapshot-1",)]

    class Store:
        def __init__(self, connection):
            self.connection = connection

        def measure(self, **kwargs):
            return policy

        def latest(self):
            return policy

        def persist(self, actual):
            assert actual == policy
            return policy

    class Runner:
        def __init__(self, **kwargs):
            self.configuration = kwargs

        def run(self, command, arguments):
            assert command == "build"
            assert "tag:temporal" in arguments
            requested = json.loads(arguments[-1])["scoring_dates"]
            published_features.clear()
            published_features.update(
                (day, feature(date.fromisoformat(day))) for day in requested
            )

    monkeypatch.setenv("CARRIER_RISK_DATABASE_URL", "offline-fixture")
    monkeypatch.setenv("CARRIER_RISK_TRAINING_OUTPUT", str(tmp_path / "datasets"))
    monkeypatch.setattr(monthly, "training_lock", lambda _: nullcontext(Connection()))
    monkeypatch.setattr(
        monthly, "PostgresBuildLock", lambda factory: lambda: nullcontext(42)
    )
    monkeypatch.setattr(monthly, "PostgresWatermarkStore", Store)
    monkeypatch.setattr(monthly, "DbtRunner", Runner)
    monkeypatch.setattr(
        monthly, "load_training_dataset", lambda *args, **kwargs: dataset
    )
    result = monthly.run_from_environment(
        "build",
        inspection_start=coverage().inspections_start,
        crash_start=coverage().crashes_start,
    )
    assert result.row_count == len(dates)
    assert policy.data_as_of.replace(day=1).isoformat() in published_features
    assert set(dates) <= {row.scoring_date for row in published_features.values()}
    assert list((tmp_path / "datasets").glob("*.json"))
    artifact = tmp_path / "watermark.json"
    artifact.write_text(policy.to_json())
    assert (
        monthly.run_from_environment("measure", watermark_path=artifact).status
        == "eligible"
    )
    assert (
        monthly.run_from_environment(
            "measure", expected_watermark_id=policy.watermark_version
        ).status
        == "eligible"
    )
    with pytest.raises(ValueError, match="changed"):
        monthly.run_from_environment("measure", expected_watermark_id="outdated")
    monkeypatch.delenv("CARRIER_RISK_DATABASE_URL")
    with pytest.raises(ValueError, match="CARRIER_RISK_DATABASE_URL"):
        monthly.run_from_environment("measure")


def test_training_session_lock_is_released_after_failure() -> None:
    from ml.watermark_store import TRAINING_LOCK_KEY, training_lock

    with pytest.raises(RuntimeError, match="forced"), training_lock(POSTGRES_DSN):
        raise RuntimeError("forced fitting failure")
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        assert connection.execute(
            "select pg_try_advisory_lock(%s)", (TRAINING_LOCK_KEY,)
        ).fetchone() == (True,)
        connection.execute("select pg_advisory_unlock(%s)", (TRAINING_LOCK_KEY,))


def test_monthly_cli_outputs_policy_decision_and_actionable_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import sys

    import ml.monthly as monthly

    monkeypatch.setattr(sys, "argv", ["monthly", "measure"])
    monkeypatch.setattr(
        monthly,
        "run_from_environment",
        lambda *args, **kwargs: monthly.MonthlyResult(
            "skipped",
            "grace_exceeds_nine_calendar_months",
            "policy",
        ),
    )
    monthly.main()
    assert '"status": "skipped"' in capsys.readouterr().out

    def invalid_configuration(*args, **kwargs):
        raise ValueError("attested source coverage missing")

    monkeypatch.setattr(monthly, "run_from_environment", invalid_configuration)
    with pytest.raises(SystemExit) as error:
        monthly.main()
    assert error.value.code == 2
    assert "attested source coverage missing" in capsys.readouterr().err


def test_registry_retains_approved_grace_through_incomplete_measurement() -> None:
    """The durable 90 -> incomplete -> 10 sequence must require decrease review."""
    from ml.maturity import training_eligibility

    cutoff = date(2025, 2, 1)
    reports = [
        CrashReportVersion(
            usdot_number="123",
            report_state="MI",
            report_number=str(month),
            event_date=month,
            reported_date=month + timedelta(days=10),
            report_time="1200",
            source_record_key=str(month),
            federal_recordable=True,
            knowledge_valid_from=datetime.combine(
                month + timedelta(days=10),
                datetime.min.time(),
                UTC,
            ),
        )
        for month in mature_cohort_months(cutoff)
    ]
    schema = f"maturity_recovery_{uuid4().hex}"
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema)
        try:
            approved = store.persist(watermark(90))
            incomplete = calculate_watermark(
                reports,
                data_as_of=cutoff,
                computed_at=datetime(2025, 2, 1, tzinfo=UTC),
                source_fingerprint="incomplete-source",
                source_complete=False,
                previous=approved,
                bootstrap_replicates=200,
            )
            store.persist(incomplete, source_audit={"source_complete": False})
            current = store.latest()
            assert current is not None
            assert current.grace_days == 90
            assert current.measured_grace_days is None
            assert not training_eligibility(
                current, label_window_end=date(2024, 1, 1)
            ).eligible
            recovered = calculate_watermark(
                reports,
                data_as_of=cutoff,
                computed_at=datetime(2025, 2, 2, tzinfo=UTC),
                source_fingerprint="complete-source",
                previous=current,
                bootstrap_replicates=200,
            )
            store.persist(recovered, source_audit={"source_complete": True})
            current = store.latest()
            assert current is not None
            assert current.status == "decrease_pending_review"
            assert current.grace_days == 90
            assert current.measured_grace_days == 10
            reviewed = calculate_watermark(
                reports,
                data_as_of=cutoff,
                computed_at=datetime(2025, 2, 3, tzinfo=UTC),
                source_fingerprint="complete-source",
                previous=current,
                reviewed_decrease=True,
                bootstrap_replicates=200,
            )
            store.persist(reviewed)
            assert store.latest() == reviewed
            assert reviewed.grace_days == 10
            rows = connection.execute(
                sql.SQL(
                    "select count(*), count(*) filter(where is_current) "
                    "from {}.label_maturity_watermarks"
                ).format(sql.Identifier(schema))
            ).fetchone()
            assert rows == (4, 1)
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )


@pytest.mark.parametrize("replacement_grace", [None, 10, 100])
def test_registry_rejects_incomplete_import_that_erases_approved_grace(
    replacement_grace: int | None,
) -> None:
    """Even a self-consistent imported record cannot reset the durable floor."""
    import hashlib
    import json
    from dataclasses import replace

    approved = watermark(90)
    incomplete = calculate_watermark(
        [],
        data_as_of=approved.data_as_of,
        computed_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_fingerprint="partial",
        source_complete=False,
        previous=approved,
        bootstrap_replicates=200,
    )
    payload = json.loads(incomplete.to_json())
    for name in ("watermark_version", "computed_at", "sample_size"):
        payload.pop(name)
    payload["grace_days"] = replacement_grace
    identity = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    erased = replace(
        incomplete, grace_days=replacement_grace, watermark_version=identity
    )
    schema = f"maturity_erasure_{uuid4().hex}"
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema)
        try:
            store.persist(approved)
            with pytest.raises(ValueError):
                store.persist(erased)
            assert store.latest() == approved
            rows = connection.execute(
                sql.SQL(
                    "select count(*), count(*) filter(where is_current) "
                    "from {}.label_maturity_watermarks"
                ).format(sql.Identifier(schema))
            ).fetchone()
            assert rows == (1, 1)
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )


def test_registry_without_current_policy_cannot_restart_grace_history(
    database_schemas: tuple[str, str],
) -> None:
    """Removing the current marker is corruption, not first-time measurement."""
    raw_schema, schema = database_schemas
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema, raw_schema=raw_schema)
        try:
            approved = store.persist(watermark(90))
            connection.execute(
                sql.SQL(
                    "update {}.label_maturity_watermarks set is_current = false"
                ).format(sql.Identifier(schema))
            )
            before = connection.execute(
                sql.SQL(
                    "select * from {}.label_maturity_watermarks "
                    "order by watermark_version"
                ).format(sql.Identifier(schema))
            ).fetchall()
            assert before
            with pytest.raises(ValueError, match="current"):
                store.latest()
            with pytest.raises(ValueError, match="current"):
                store.persist(watermark(10))
            dependencies = MonthlyDependencies(
                measure=lambda: store.measure(
                    data_as_of=approved.data_as_of,
                    computed_at=datetime(2025, 2, 1, tzinfo=UTC),
                ),
                build=lambda _: pytest.fail("corrupt policy cannot build features"),
                train=lambda policy, factory: pytest.fail("corrupt policy cannot fit"),
            )
            with pytest.raises(ValueError, match="current"):
                run_monthly("train", dependencies)
            after = connection.execute(
                sql.SQL(
                    "select * from {}.label_maturity_watermarks "
                    "order by watermark_version"
                ).format(sql.Identifier(schema))
            ).fetchall()
            assert after == before
            assert after[0][0] == approved.watermark_version
        finally:
            connection.execute(
                sql.SQL("drop table {}.label_maturity_watermarks").format(
                    sql.Identifier(schema)
                )
            )


def test_registry_current_pointer_cannot_return_to_a_persisted_predecessor() -> None:
    approved = watermark(90)
    reports = [
        CrashReportVersion(
            usdot_number="123",
            report_state="MI",
            report_number=str(month),
            event_date=month,
            reported_date=month + timedelta(days=100),
            report_time="1200",
            source_record_key=str(month),
            federal_recordable=True,
            knowledge_valid_from=datetime.combine(
                month + timedelta(days=100),
                datetime.min.time(),
                UTC,
            ),
        )
        for month in mature_cohort_months(approved.data_as_of)
    ]
    successor = calculate_watermark(
        reports,
        data_as_of=approved.data_as_of,
        computed_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_fingerprint="higher-grace",
        previous=approved,
        bootstrap_replicates=200,
    )
    schema = f"maturity_predecessor_{uuid4().hex}"
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        store = PostgresWatermarkStore(connection, schema=schema)
        try:
            store.persist(approved)
            store.persist(successor)
            with connection.transaction():
                connection.execute(
                    sql.SQL(
                        "update {}.label_maturity_watermarks set is_current=false"
                    ).format(sql.Identifier(schema))
                )
                connection.execute(
                    sql.SQL(
                        "update {}.label_maturity_watermarks set is_current=true "
                        "where watermark_version=%s"
                    ).format(sql.Identifier(schema)),
                    (approved.watermark_version,),
                )
            snapshot_query = sql.SQL(
                "select * from {}.label_maturity_watermarks order by watermark_version"
            ).format(sql.Identifier(schema))
            before = connection.execute(snapshot_query).fetchall()
            with pytest.raises(ValueError, match="successor"):
                store.latest()
            with pytest.raises(ValueError, match="successor"):
                store.persist(successor)
            assert connection.execute(snapshot_query).fetchall() == before
        finally:
            connection.execute(
                sql.SQL("drop schema {} cascade").format(sql.Identifier(schema))
            )
