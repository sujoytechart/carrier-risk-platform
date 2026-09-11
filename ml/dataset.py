"""Monthly, maturity-gated training rows from versioned modeled relations."""

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Literal

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from ml.features import FeatureRow
from ml.maturity import MaturityWatermark, add_months, training_eligibility


@dataclass(frozen=True)
class SourceCoverage:
    """Attested complete event-date coverage, tied to applied snapshot identities.

    Bounds must come from source retention/completeness evidence. An earliest
    event alone cannot establish complete coverage. ``observed_through`` is the
    last acquisition day covered by both feeds, not the current wall clock.
    """

    inspections_start: date
    crashes_start: date
    observed_through: date
    batch_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.batch_ids or len(set(self.batch_ids)) != len(self.batch_ids):
            raise ValueError("coverage requires distinct complete batch identities")
        if max(self.inspections_start, self.crashes_start) > self.observed_through:
            raise ValueError("source coverage starts after its observation cutoff")


@dataclass(frozen=True)
class SourceEvent:
    """An immutable event-history row used for labels and acquisition lineage.

    ``is_model_eligible`` preserves the clean layer's federal-recordability,
    identifier, and incident-key checks. It is never inferred from severity.
    """

    event_version_key: int
    feed_name: Literal["inspections", "crashes"]
    source_record_key: str
    usdot_number: str | None
    event_date: date | None
    reported_date: date
    knowledge_valid_from: datetime
    knowledge_valid_to: datetime | None
    availability_quality: Literal["source_proxy", "observed"]
    record_hash: str
    is_model_eligible: bool
    is_deleted: bool
    first_seen_batch_id: str
    report_state: str | None
    report_number: str | None
    report_time: int | None

    def __post_init__(self) -> None:
        for timestamp in (self.knowledge_valid_from, self.knowledge_valid_to):
            if timestamp is not None and timestamp.utcoffset() is None:
                raise ValueError("knowledge timestamps must include a timezone")
        if self.feed_name not in ("inspections", "crashes"):
            raise ValueError("unknown source feed")
        if self.availability_quality not in ("source_proxy", "observed"):
            raise ValueError("unknown availability quality")

    def visible_at(self, cutoff: date) -> bool:
        """Resolve strict knowledge and reporting clocks, including boundaries."""
        instant = datetime.combine(cutoff, time.min, UTC)
        return (
            self.knowledge_valid_from < instant
            and (self.knowledge_valid_to is None or self.knowledge_valid_to > instant)
            and self.reported_date < cutoff
            and not self.is_deleted
        )

    @property
    def incident_key(self) -> tuple[str | None, str, str, date, int] | None:
        """Deduplicate vehicle records while preserving missing-carrier counts."""
        if (
            self.report_state is None
            or self.report_number is None
            or self.event_date is None
            or self.report_time is None
        ):
            return None
        return (
            self.usdot_number,
            self.report_state,
            self.report_number,
            self.event_date,
            self.report_time,
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> "SourceEvent":
        """Convert typed database rows without coercing malformed source values."""
        # psycopg's class row factory owns conversion at the database boundary.
        # This mapping path is also useful for offline, explicitly typed fixtures.
        required = {field.name for field in fields(cls)}
        if not required <= values.keys():
            raise ValueError("source event is missing required fields")
        return cls(
            event_version_key=_required(values, "event_version_key", int),
            feed_name=_feed(values["feed_name"]),
            source_record_key=_required(values, "source_record_key", str),
            usdot_number=_optional(values, "usdot_number", str),
            event_date=_optional(values, "event_date", date),
            reported_date=_required(values, "reported_date", date),
            knowledge_valid_from=_required(values, "knowledge_valid_from", datetime),
            knowledge_valid_to=_optional(values, "knowledge_valid_to", datetime),
            availability_quality=_quality(values["availability_quality"]),
            record_hash=_required(values, "record_hash", str),
            is_model_eligible=_required(values, "is_model_eligible", bool),
            is_deleted=_required(values, "is_deleted", bool),
            first_seen_batch_id=_required(values, "first_seen_batch_id", str),
            report_state=_optional(values, "report_state", str),
            report_number=_optional(values, "report_number", str),
            report_time=_optional(values, "report_time", int),
        )


@dataclass(frozen=True)
class TrainingRow:
    """One frozen as-of feature row and its matured, deduplicated outcome."""

    features: FeatureRow
    label: int
    label_incident_count: int = 0


@dataclass(frozen=True)
class DatasetProvenance:
    """Actual contributing-version mix and excluded as-of crash reconciliation."""

    availability_quality_counts: tuple[tuple[str, int], ...]
    excluded_crash_rows: int
    excluded_crash_incidents: int
    unkeyable_excluded_crash_rows: int
    source_version_fingerprint: str
    batch_ids: tuple[str, ...]


@dataclass(frozen=True)
class TrainingDataset:
    """A reproducible monthly grid whose zero-population dates remain explicit."""

    rows: tuple[TrainingRow, ...]
    scoring_dates: tuple[date, ...]
    data_as_of: date
    watermark_id: str
    grace_days: int
    provenance: DatasetProvenance
    fingerprint: str

    @property
    def row_count(self) -> int:
        """Count actual carrier/date rows, never a registry population estimate."""
        return len(self.rows)


@dataclass(frozen=True)
class PurgedSplit:
    """The final three dates held out after a six-calendar-month embargo."""

    train: tuple[TrainingRow, ...]
    test: tuple[TrainingRow, ...]
    purged: tuple[TrainingRow, ...]
    train_dates: tuple[date, ...]
    test_dates: tuple[date, ...]
    purged_dates: tuple[date, ...]


def generate_scoring_dates(
    coverage: SourceCoverage,
    watermark: MaturityWatermark | None,
    *,
    requested_start: date | None = None,
    requested_end: date | None = None,
) -> tuple[date, ...]:
    """Generate complete monthly lookbacks with matured labels; fail closed.

    Requested bounds restrict the grid; they cannot override source coverage,
    the six-month label, or the approved maturity watermark.
    """
    if watermark is None or watermark.grace_days is None:
        raise ValueError("a measured, approved maturity watermark is required")
    for bound in (requested_start, requested_end):
        if bound is not None and bound.day != 1:
            raise ValueError("requested scoring bounds must be month-start dates")
    if requested_start and requested_end and requested_start > requested_end:
        raise ValueError("requested scoring range is reversed")
    earliest = max(
        add_months(coverage.inspections_start, 6),
        add_months(coverage.crashes_start, 24),
    )
    scoring_date = earliest.replace(day=1)
    if scoring_date < earliest:
        scoring_date = add_months(scoring_date, 1)
    if requested_start is not None:
        scoring_date = max(scoring_date, requested_start)
    cutoff = min(coverage.observed_through, watermark.data_as_of)
    dates: list[date] = []
    while scoring_date <= cutoff:
        if requested_end is not None and scoring_date > requested_end:
            break
        label_end = add_months(scoring_date, 6)
        if watermark.grace_days > (add_months(label_end, 9) - label_end).days:
            raise ValueError("label-maturity grace exceeds nine calendar months")
        decision = training_eligibility(watermark, label_window_end=label_end)
        if not decision.eligible:
            if label_end + timedelta(days=watermark.grace_days) > cutoff:
                break
            raise ValueError(f"maturity watermark rejects training: {decision.reason}")
        if label_end + timedelta(days=watermark.grace_days) > cutoff:
            break
        dates.append(scoring_date)
        scoring_date = add_months(scoring_date, 1)
    if not dates:
        raise ValueError("no scoring dates have complete history and mature labels")
    return tuple(dates)


def build_training_dataset(
    feature_rows: Sequence[FeatureRow],
    events: Sequence[SourceEvent],
    *,
    coverage: SourceCoverage,
    watermark: MaturityWatermark | None,
    requested_start: date | None = None,
    requested_end: date | None = None,
) -> TrainingDataset:
    """Join existing dbt features to as-of labels, retaining frozen provenance.

    The typed boundary permits deterministic offline testing. Production callers
    use ``load_training_dataset`` to verify complete applied batches and obtain a
    single read-only database snapshot. Feature-population checks reject stale or
    incomplete materializations, including dates missing all their eligible rows.
    """
    dates = generate_scoring_dates(
        coverage,
        watermark,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    assert watermark is not None and watermark.grace_days is not None
    cutoff = min(coverage.observed_through, watermark.data_as_of)
    cutoff_instant = datetime.combine(cutoff, time.min, UTC)
    versions = tuple(
        sorted(
            (
                replace(event, knowledge_valid_to=None)
                if event.knowledge_valid_to is not None
                and event.knowledge_valid_to > cutoff_instant
                else event
                for event in events
                if event.knowledge_valid_from < cutoff_instant
            ),
            key=lambda event: event.event_version_key,
        )
    )
    if len({event.event_version_key for event in versions}) != len(versions):
        raise ValueError("duplicate event version identities")
    batch_ids = set(coverage.batch_ids)
    if any(event.first_seen_batch_id not in batch_ids for event in versions):
        raise ValueError("event lineage is outside attested complete batches")
    selected = tuple(
        sorted(
            (row for row in feature_rows if row.scoring_date in dates),
            key=lambda row: (row.scoring_date, row.usdot_number),
        )
    )
    _validate_population(selected, versions, dates)
    labels = tuple(
        event
        for event in versions
        if event.feed_name == "crashes" and event.visible_at(cutoff)
    )
    rows: list[TrainingRow] = []
    contributing: dict[int, SourceEvent] = {}
    versions_by_carrier: dict[str | None, list[SourceEvent]] = defaultdict(list)
    labels_by_carrier: dict[str | None, list[SourceEvent]] = defaultdict(list)
    for event in versions:
        versions_by_carrier[event.usdot_number].append(event)
    for event in labels:
        labels_by_carrier[event.usdot_number].append(event)
    for row in selected:
        incidents = {
            event.incident_key
            for event in labels_by_carrier[row.usdot_number]
            if event.is_model_eligible
            and event.usdot_number == row.usdot_number
            and event.event_date is not None
            and row.scoring_date <= event.event_date < add_months(row.scoring_date, 6)
            and event.incident_key is not None
        }
        rows.append(TrainingRow(row, int(bool(incidents)), len(incidents)))
        for event in versions_by_carrier[row.usdot_number]:
            if _contributes(event, row, cutoff):
                contributing[event.event_version_key] = event
    provenance = _provenance(versions, labels, contributing, coverage)
    identity = {
        "calculation_version": "monthly-events-v1",
        "rows": [asdict(row) for row in rows],
        "scoring_dates": dates,
        "data_as_of": cutoff,
        "watermark_id": watermark.watermark_version,
        "grace_days": watermark.grace_days,
        "coverage": asdict(coverage),
        "provenance": asdict(provenance),
    }
    return TrainingDataset(
        tuple(rows),
        dates,
        cutoff,
        watermark.watermark_version,
        watermark.grace_days,
        provenance,
        _fingerprint(identity),
    )


def split_purged(dataset: TrainingDataset) -> PurgedSplit:
    """Require fifteen contiguous dates, six train dates, six purge, three test."""
    dates = dataset.scoring_dates
    if len(dates) < 15:
        raise ValueError(
            "retrospective training requires at least fifteen scoring dates"
        )
    if any(day.day != 1 for day in dates) or any(
        right != add_months(left, 1)
        for left, right in zip(dates, dates[1:], strict=False)
    ):
        raise ValueError("purged split requires a contiguous monthly scoring grid")
    if any(row.features.scoring_date not in dates for row in dataset.rows):
        raise ValueError("training row is outside the scoring grid")
    train_dates, purged_dates, test_dates = dates[:-9], dates[-9:-3], dates[-3:]
    train = tuple(
        row for row in dataset.rows if row.features.scoring_date in train_dates
    )
    test = tuple(row for row in dataset.rows if row.features.scoring_date in test_dates)
    purged = tuple(
        row for row in dataset.rows if row.features.scoring_date in purged_dates
    )
    if not train or not test:
        raise ValueError("both train and held-out periods need eligible carrier rows")
    return PurgedSplit(train, test, purged, train_dates, test_dates, purged_dates)


def _validate_population(
    rows: tuple[FeatureRow, ...],
    events: tuple[SourceEvent, ...],
    dates: tuple[date, ...],
) -> None:
    actual = {(row.usdot_number, row.scoring_date) for row in rows}
    if len(actual) != len(rows):
        raise ValueError("duplicate carrier/scoring-date feature rows")
    expected = {
        (event.usdot_number, scoring_date)
        for scoring_date in dates
        for event in events
        if event.feed_name == "inspections"
        and event.is_model_eligible
        and event.usdot_number is not None
        and event.event_date is not None
        and event.visible_at(scoring_date)
        and add_months(scoring_date, -6) <= event.event_date < scoring_date
    }
    if actual != expected:
        raise ValueError(
            "stored dbt features do not match the eligible carrier/date grid"
        )


def _contributes(event: SourceEvent, row: FeatureRow, cutoff: date) -> bool:
    if (
        not event.is_model_eligible
        or event.usdot_number != row.usdot_number
        or event.event_date is None
    ):
        return False
    months = 6 if event.feed_name == "inspections" else 24
    historical = (
        event.visible_at(row.scoring_date)
        and add_months(row.scoring_date, -months) <= event.event_date < row.scoring_date
    )
    outcome = (
        event.feed_name == "crashes"
        and event.visible_at(cutoff)
        and row.scoring_date <= event.event_date < add_months(row.scoring_date, 6)
    )
    return historical or outcome


def _provenance(
    versions: tuple[SourceEvent, ...],
    labels: tuple[SourceEvent, ...],
    contributing: dict[int, SourceEvent],
    coverage: SourceCoverage,
) -> DatasetProvenance:
    excluded = tuple(
        event
        for event in labels
        if not event.is_model_eligible or event.usdot_number is None
    )
    incident_keys = {
        event.incident_key for event in excluded if event.incident_key is not None
    }
    counts = Counter(event.availability_quality for event in contributing.values())
    return DatasetProvenance(
        tuple(sorted(counts.items())),
        len(excluded),
        len(incident_keys),
        sum(event.incident_key is None for event in excluded),
        _fingerprint([asdict(event) for event in versions]),
        tuple(sorted(coverage.batch_ids)),
    )


def _fingerprint(value: object) -> str:
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
        ).encode()
    ).hexdigest()


def _required[T](values: Mapping[str, object], name: str, expected: type[T]) -> T:
    value = values[name]
    if not isinstance(value, expected) or (
        expected in (date, int) and type(value) is not expected
    ):
        raise ValueError(f"{name} must be {expected.__name__}")
    return value


def _optional[T](
    values: Mapping[str, object], name: str, expected: type[T]
) -> T | None:
    return None if values[name] is None else _required(values, name, expected)


def _feed(value: object) -> Literal["inspections", "crashes"]:
    if value == "inspections":
        return "inspections"
    if value == "crashes":
        return "crashes"
    raise ValueError("unknown source feed")


def _quality(value: object) -> Literal["source_proxy", "observed"]:
    if value == "source_proxy":
        return "source_proxy"
    if value == "observed":
        return "observed"
    raise ValueError("unknown availability quality")


def load_training_dataset(
    database_url: str,
    *,
    coverage: SourceCoverage,
    watermark: MaturityWatermark | None,
    modeled_schema: str = "modeled",
    raw_schema: str = "raw",
    requested_start: date | None = None,
    requested_end: date | None = None,
    max_source_rows: int = 20_000_000,
) -> TrainingDataset:
    """Read a bounded repeatable snapshot after verifying both complete feeds.

    An explicit limit rejects oversized extracts instead of silently truncating
    them. Raising the bound is an operational memory decision, never a relaxation
    of source completeness or temporal rules.
    """
    dates = generate_scoring_dates(
        coverage,
        watermark,
        requested_start=requested_start,
        requested_end=requested_end,
    )
    if max_source_rows < 1:
        raise ValueError("max_source_rows must be positive")
    assert watermark is not None
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("set transaction isolation level repeatable read read only")
        batches = connection.execute(
            sql.SQL("""
            select applied.batch_id, applied.feed_name, applied.observed_at
            from {}.event_version_batches applied
            join {}.snapshot_batches raw using (batch_id, feed_name, observed_at)
            where raw.status = 'loaded' and applied.batch_id = any(%s)
        """).format(sql.Identifier(modeled_schema), sql.Identifier(raw_schema)),
            (list(coverage.batch_ids),),
        ).fetchall()
        if {row["batch_id"] for row in batches} != set(coverage.batch_ids):
            raise ValueError(
                "coverage contains unapplied or incomplete snapshot batches"
            )
        for feed in ("inspections", "crashes"):
            observations = [
                row["observed_at"].astimezone(UTC).date()
                for row in batches
                if row["feed_name"] == feed
            ]
            if not observations or max(observations) < coverage.observed_through:
                raise ValueError("complete snapshots do not cover the claimed cutoff")
        columns = sql.SQL(", ").join(
            sql.Identifier(field.name) for field in fields(SourceEvent)
        )
        source_rows = connection.execute(
            sql.SQL("""
            select {} from {}.event_versions
            where knowledge_valid_from < %s
              and (event_date is null or (event_date >= %s and event_date < %s))
            order by event_version_key limit %s
        """).format(columns, sql.Identifier(modeled_schema)),
            (
                datetime.combine(
                    min(coverage.observed_through, watermark.data_as_of), time.min, UTC
                ),
                add_months(dates[0], -24),
                add_months(dates[-1], 6),
                max_source_rows + 1,
            ),
        ).fetchall()
        feature_rows = connection.execute(
            sql.SQL("""
            select * from {}.training_features where scoring_date = any(%s)
            order by scoring_date, usdot_number limit %s
        """).format(sql.Identifier(modeled_schema)),
            (list(dates), max_source_rows + 1),
        ).fetchall()
        if len(source_rows) > max_source_rows or len(feature_rows) > max_source_rows:
            raise ValueError("dataset exceeds the explicit extraction row limit")
        return build_training_dataset(
            tuple(FeatureRow.from_mapping(row) for row in feature_rows),
            tuple(SourceEvent.from_mapping(row) for row in source_rows),
            coverage=coverage,
            watermark=watermark,
            requested_start=requested_start,
            requested_end=requested_end,
        )
