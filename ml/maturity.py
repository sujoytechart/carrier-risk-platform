"""Incident-level empirical label maturity; incomplete measurements fail closed."""

from __future__ import annotations

import calendar
import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import numpy as np

CALCULATION_VERSION = "incident-first-version-histogram-bootstrap-linear-v1"
LABEL_DEFINITION = "at_least_one_federally_recordable_carrier_crash_in_next_6_months"
QUANTILE = 0.995
CONFIDENCE = 0.95
COHORT_COUNT = 12
MAX_GRACE_MONTHS = 9
IncidentKey = tuple[str | None, str, str, date, str]


@dataclass(frozen=True)
class CrashReportVersion:
    """A normalized source version, before reportability or carrier exclusion.

    The incident identity deliberately omits vehicle sequence and source record ID.
    Knowledge timestamps must be timezone-aware. Historical snapshot adapters mark
    ADD_DATE plus one publication day as a source proxy, never as observed history.
    """

    usdot_number: str | None
    report_state: str
    report_number: str
    event_date: date
    report_time: str
    source_record_key: str
    reported_date: date
    knowledge_valid_from: datetime
    federal_recordable: bool
    availability_quality: Literal["source_proxy", "observed"] = "source_proxy"
    is_deleted: bool = False

    @property
    def incident_key(self) -> IncidentKey:
        """Return the stable carrier incident identity without report_seq_no."""
        return (
            self.usdot_number,
            self.report_state,
            self.report_number,
            self.event_date,
            self.report_time,
        )


@dataclass(frozen=True)
class CohortEstimate:
    """The empirical percentile and bootstrap confidence bound for one month."""

    event_month: date
    incident_count: int
    p995_days: float | None
    upper_95_days: float | None


@dataclass(frozen=True)
class MaturityWatermark:
    """Immutable measured policy and the evidence needed to reproduce it.

    A pending decrease retains the previous effective grace. An incomplete source
    or missing cohort retains that same value only as a policy floor for recovery;
    its incomplete status still blocks training. Without prior evidence the floor
    remains unmeasured.
    """

    watermark_version: str
    label_definition: str
    quantile: float
    confidence: float
    calculation_version: str
    data_as_of: date
    computed_at: datetime
    source_fingerprint: str
    bootstrap_seed: int
    bootstrap_replicates: int
    cohorts: tuple[CohortEstimate, ...]
    sample_size: int
    measured_grace_days: int | None
    grace_days: int | None
    status: Literal["measured", "incomplete", "decrease_pending_review"]
    source_complete: bool
    excluded_missing_usdot_rows: int
    excluded_missing_usdot_incidents: int
    excluded_nonreportable_incidents: int
    availability_quality_counts: tuple[tuple[str, int], ...]
    previous_watermark_version: str | None
    reviewed_decrease: bool

    def to_json(self) -> str:
        """Serialize only aggregate evidence; no carrier or source-row data."""
        return json.dumps(asdict(self), default=_json_date, sort_keys=True, indent=2)


@dataclass(frozen=True)
class TrainingEligibility:
    """A fail-closed decision for a particular forward-label window end."""

    eligible: bool
    reason: str


def add_months(value: date, months: int) -> date:
    """Add calendar months, clamping the day to the target month's last day."""
    month_index = value.year * 12 + value.month - 1 + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def mature_cohort_months(data_as_of: date) -> tuple[date, ...]:
    """Select exactly twelve complete months mature by the nine-month policy."""
    latest = data_as_of.replace(day=1)
    while add_months(_month_end(latest), MAX_GRACE_MONTHS) > data_as_of:
        latest = add_months(latest, -1)
    return tuple(add_months(latest, offset) for offset in range(1 - COHORT_COUNT, 1))


def _month_end(value: date) -> date:
    return value.replace(day=calendar.monthrange(value.year, value.month)[1])


def _json_date(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _first_incidents(
    versions: Iterable[CrashReportVersion], data_as_of: date
) -> tuple[list[CrashReportVersion], int, int]:
    first_source_versions: dict[str, CrashReportVersion] = {}
    missing_rows = 0
    missing_incidents: set[IncidentKey] = set()
    for version in versions:
        if version.knowledge_valid_from.utcoffset() is None:
            raise ValueError("Knowledge timestamps must be timezone-aware")
        if (
            version.knowledge_valid_from.date() > data_as_of
            or version.reported_date > data_as_of
        ):
            continue
        if version.reported_date < version.event_date:
            raise ValueError("A negative first-report lag must be quarantined")
        if version.usdot_number is None:
            missing_rows += 1
            missing_incidents.add(version.incident_key)
        prior = first_source_versions.get(version.source_record_key)
        if prior is None or version.knowledge_valid_from < prior.knowledge_valid_from:
            first_source_versions[version.source_record_key] = version
        elif (
            version.knowledge_valid_from == prior.knowledge_valid_from
            and version != prior
        ):
            raise ValueError(
                "Conflicting versions share a source key and knowledge time"
            )
    first: dict[IncidentKey, dict[str, CrashReportVersion]] = {}
    for version in first_source_versions.values():
        if version.usdot_number is None:
            continue
        if not version.usdot_number.isdecimal() or int(version.usdot_number) <= 0:
            raise ValueError("USDOT numbers must be normalized positive integers")
        if version.usdot_number != str(int(version.usdot_number)):
            raise ValueError("USDOT numbers must be normalized positive integers")
        if (
            not version.report_state
            or not version.report_number
            or not version.report_time
        ):
            raise ValueError("An incident needs a complete stable identity")
        existing = first.get(version.incident_key)
        if existing is None:
            first[version.incident_key] = {version.source_record_key: version}
            continue
        earliest = next(iter(existing.values())).knowledge_valid_from
        if version.knowledge_valid_from < earliest:
            first[version.incident_key] = {version.source_record_key: version}
        elif version.knowledge_valid_from == earliest:
            same_source = existing.get(version.source_record_key)
            if same_source is not None and same_source != version:
                raise ValueError(
                    "Conflicting versions share a source key and knowledge time"
                )
            existing[version.source_record_key] = version
    incidents: list[CrashReportVersion] = []
    for source_versions in first.values():
        active = [
            version for version in source_versions.values() if not version.is_deleted
        ]
        reportable = [version for version in active if version.federal_recordable]
        candidates = reportable or active or list(source_versions.values())
        incidents.append(
            min(
                candidates,
                key=lambda item: (item.reported_date, item.source_record_key),
            )
        )
    return incidents, missing_rows, len(missing_incidents)


def _estimate_cohort(
    event_month: date, lags: list[int], seed: int, replicates: int
) -> CohortEstimate:
    if not lags:
        return CohortEstimate(event_month, 0, None, None)
    values, counts = np.unique(np.asarray(lags, dtype=np.int64), return_counts=True)
    sample_size = len(lags)
    # Multinomial counts are exactly sampling n empirical observations with
    # replacement. The histogram avoids allocating replicates × observations.
    random = np.random.Generator(
        np.random.PCG64(np.random.SeedSequence([seed, event_month.toordinal()]))
    )
    sampled_counts = random.multinomial(
        sample_size, counts / sample_size, size=replicates
    )
    cumulative = np.cumsum(sampled_counts, axis=1)
    position = (sample_size - 1) * QUANTILE
    lower_rank = math.floor(position)
    upper_rank = math.ceil(position)
    lower = values[np.argmax(cumulative > lower_rank, axis=1)]
    upper = values[np.argmax(cumulative > upper_rank, axis=1)]
    bootstrap_quantiles = lower + (upper - lower) * (position - lower_rank)
    return CohortEstimate(
        event_month=event_month,
        incident_count=sample_size,
        p995_days=float(np.quantile(lags, QUANTILE, method="linear")),
        upper_95_days=float(
            np.quantile(bootstrap_quantiles, CONFIDENCE, method="linear")
        ),
    )


def calculate_watermark(
    versions: Iterable[CrashReportVersion],
    *,
    data_as_of: date,
    computed_at: datetime,
    source_fingerprint: str,
    bootstrap_seed: int = 20260911,
    bootstrap_replicates: int = 2000,
    source_complete: bool = True,
    source_excluded_missing_usdot_rows: int = 0,
    source_excluded_missing_usdot_incidents: int = 0,
    previous: MaturityWatermark | None = None,
    reviewed_decrease: bool = False,
) -> MaturityWatermark:
    """Measure the maximum cohort p99.5 bootstrap upper-95 bound, rounded up.

    Version selection precedes incident reportability: later revisions never add
    extra lag observations or rewrite the first version. A month with no eligible
    incident is unmeasured and blocks training. Callers must supply complete source
    coverage; a partial acquisition must set ``source_complete=False``. Optional
    source exclusion counts describe missing-carrier rows removed upstream; never
    also include those rows in ``versions``. They remain in the watermark hash.
    Incomplete updates preserve the previous effective grace, so loss of source
    coverage cannot erase the review requirement for a later decrease.
    """
    if computed_at.utcoffset() is None:
        raise ValueError("computed_at must be timezone-aware")
    if computed_at.date() < data_as_of:
        raise ValueError("computed_at cannot precede data_as_of")
    if not source_fingerprint:
        raise ValueError("A source fingerprint is required")
    if bootstrap_seed < 0 or bootstrap_replicates < 100:
        raise ValueError("Use a nonnegative seed and at least 100 bootstrap replicates")
    if (
        not 0
        <= source_excluded_missing_usdot_incidents
        <= source_excluded_missing_usdot_rows
    ):
        raise ValueError("Source exclusion counts must reconcile")
    if previous is not None:
        _validate_watermark(previous)
    if previous is not None and previous.data_as_of > data_as_of:
        raise ValueError("A watermark cannot use older data than its predecessor")
    months = mature_cohort_months(data_as_of)
    lags: dict[date, list[int]] = {month: [] for month in months}
    incidents, missing_rows, missing_incidents = _first_incidents(versions, data_as_of)
    missing_rows += source_excluded_missing_usdot_rows
    missing_incidents += source_excluded_missing_usdot_incidents
    nonreportable = 0
    quality: Counter[str] = Counter()
    for incident in incidents:
        if not incident.federal_recordable or incident.is_deleted:
            nonreportable += 1
            continue
        month = incident.event_date.replace(day=1)
        if month in lags:
            lags[month].append((incident.reported_date - incident.event_date).days)
            quality[incident.availability_quality] += 1
    cohorts = tuple(
        _estimate_cohort(month, lags[month], bootstrap_seed, bootstrap_replicates)
        for month in months
    )
    bounds = [
        cohort.upper_95_days for cohort in cohorts if cohort.upper_95_days is not None
    ]
    complete = source_complete and len(bounds) == COHORT_COUNT
    measured = math.ceil(max(bounds)) if complete else None
    effective = measured
    if not complete and previous is not None:
        effective = previous.grace_days
    status: Literal["measured", "incomplete", "decrease_pending_review"] = (
        "measured" if complete else "incomplete"
    )
    if (
        measured is not None
        and previous is not None
        and previous.grace_days is not None
        and measured < previous.grace_days
        and not reviewed_decrease
    ):
        effective = previous.grace_days
        status = "decrease_pending_review"
    values = {
        "label_definition": LABEL_DEFINITION,
        "quantile": QUANTILE,
        "confidence": CONFIDENCE,
        "calculation_version": CALCULATION_VERSION,
        "data_as_of": data_as_of,
        "source_fingerprint": source_fingerprint,
        "bootstrap_seed": bootstrap_seed,
        "bootstrap_replicates": bootstrap_replicates,
        "cohorts": [asdict(cohort) for cohort in cohorts],
        "measured_grace_days": measured,
        "grace_days": effective,
        "status": status,
        "source_complete": source_complete,
        "excluded_missing_usdot_rows": missing_rows,
        "excluded_missing_usdot_incidents": missing_incidents,
        "excluded_nonreportable_incidents": nonreportable,
        "availability_quality_counts": sorted(quality.items()),
        "previous_watermark_version": previous.watermark_version if previous else None,
        "reviewed_decrease": reviewed_decrease,
    }
    version = hashlib.sha256(
        json.dumps(
            values, default=_json_date, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    return MaturityWatermark(
        watermark_version=version,
        label_definition=LABEL_DEFINITION,
        quantile=QUANTILE,
        confidence=CONFIDENCE,
        calculation_version=CALCULATION_VERSION,
        data_as_of=data_as_of,
        computed_at=computed_at,
        source_fingerprint=source_fingerprint,
        bootstrap_seed=bootstrap_seed,
        bootstrap_replicates=bootstrap_replicates,
        cohorts=cohorts,
        sample_size=sum(cohort.incident_count for cohort in cohorts),
        measured_grace_days=measured,
        grace_days=effective,
        status=status,
        source_complete=source_complete,
        excluded_missing_usdot_rows=missing_rows,
        excluded_missing_usdot_incidents=missing_incidents,
        excluded_nonreportable_incidents=nonreportable,
        availability_quality_counts=tuple(sorted(quality.items())),
        previous_watermark_version=previous.watermark_version if previous else None,
        reviewed_decrease=reviewed_decrease,
    )


def training_eligibility(
    watermark: MaturityWatermark | None, *, label_window_end: date
) -> TrainingEligibility:
    """Require a measured watermark, nine calendar months, and matured labels."""
    if watermark is None:
        return TrainingEligibility(False, "maturity_unmeasured")
    try:
        _validate_watermark(watermark)
    except (ValueError, TypeError, AttributeError):
        return TrainingEligibility(False, "maturity_invalid")
    if watermark.status == "incomplete" or watermark.grace_days is None:
        return TrainingEligibility(False, "maturity_incomplete")
    maximum_days = (
        add_months(label_window_end, MAX_GRACE_MONTHS) - label_window_end
    ).days
    if watermark.grace_days > maximum_days:
        return TrainingEligibility(False, "grace_exceeds_nine_calendar_months")
    if label_window_end + timedelta(days=watermark.grace_days) > watermark.data_as_of:
        return TrainingEligibility(False, "labels_not_mature")
    return TrainingEligibility(True, "eligible")


def _validate_watermark(watermark: MaturityWatermark) -> None:
    """Reject inconsistent policy records before they can permit training."""
    if (
        watermark.label_definition != LABEL_DEFINITION
        or watermark.quantile != QUANTILE
        or watermark.confidence != CONFIDENCE
        or watermark.calculation_version != CALCULATION_VERSION
    ):
        raise ValueError("Watermark policy differs from the committed calculation")
    if (
        type(watermark.source_complete) is not bool
        or type(watermark.reviewed_decrease) is not bool
        or watermark.computed_at.utcoffset() is None
        or watermark.computed_at.date() < watermark.data_as_of
        or not watermark.source_fingerprint
    ):
        raise ValueError("Invalid watermark provenance")
    integers = [
        watermark.bootstrap_seed,
        watermark.bootstrap_replicates,
        watermark.sample_size,
        watermark.excluded_missing_usdot_rows,
        watermark.excluded_missing_usdot_incidents,
        watermark.excluded_nonreportable_incidents,
    ]
    integers.extend(cohort.incident_count for cohort in watermark.cohorts)
    integers.extend(count for _, count in watermark.availability_quality_counts)
    integers.extend(
        value
        for value in (watermark.grace_days, watermark.measured_grace_days)
        if value is not None
    )
    if any(type(value) is not int or value < 0 for value in integers):
        raise ValueError("Watermark counts and grace must be nonnegative integers")
    if watermark.bootstrap_replicates < 100:
        raise ValueError("Insufficient bootstrap replicates")
    if tuple(
        cohort.event_month for cohort in watermark.cohorts
    ) != mature_cohort_months(watermark.data_as_of):
        raise ValueError(
            "Watermark must contain exactly the latest twelve mature cohorts"
        )
    if watermark.sample_size != sum(
        cohort.incident_count for cohort in watermark.cohorts
    ):
        raise ValueError("Watermark sample counts do not reconcile")
    if watermark.sample_size != sum(
        count for _, count in watermark.availability_quality_counts
    ) or any(
        quality not in {"observed", "source_proxy"}
        for quality, _ in watermark.availability_quality_counts
    ):
        raise ValueError("Watermark availability counts do not reconcile")
    bounds: list[float] = []
    for cohort in watermark.cohorts:
        if cohort.incident_count == 0:
            if cohort.p995_days is not None or cohort.upper_95_days is not None:
                raise ValueError("Empty cohorts cannot have measured quantiles")
        elif (
            cohort.p995_days is None
            or cohort.upper_95_days is None
            or not math.isfinite(cohort.p995_days)
            or not math.isfinite(cohort.upper_95_days)
            or cohort.p995_days < 0
            or cohort.upper_95_days < 0
        ):
            raise ValueError("Nonempty cohorts require finite nonnegative quantiles")
        else:
            bounds.append(cohort.upper_95_days)
    complete = watermark.source_complete and len(bounds) == COHORT_COUNT
    if not complete:
        if (
            watermark.status != "incomplete"
            or watermark.measured_grace_days is not None
        ):
            raise ValueError("Incomplete measurement cannot produce a measured grace")
        if (
            watermark.grace_days is not None
            and not watermark.previous_watermark_version
        ):
            raise ValueError("An incomplete policy floor requires a previous watermark")
    else:
        if watermark.measured_grace_days != math.ceil(max(bounds)):
            raise ValueError("Measured grace does not match cohort confidence bounds")
        if watermark.status == "measured":
            if watermark.grace_days != watermark.measured_grace_days:
                raise ValueError("Effective grace differs from measured policy")
        elif watermark.status == "decrease_pending_review":
            if (
                watermark.grace_days is None
                or watermark.measured_grace_days is None
                or watermark.grace_days <= watermark.measured_grace_days
                or not watermark.previous_watermark_version
                or watermark.reviewed_decrease
            ):
                raise ValueError("Pending decrease must retain a previous larger grace")
        else:
            raise ValueError("Complete measurement has an invalid policy status")
    payload = asdict(watermark)
    for field in ("watermark_version", "computed_at", "sample_size"):
        payload.pop(field)
    expected_hash = hashlib.sha256(
        json.dumps(
            payload, default=_json_date, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    if watermark.watermark_version != expected_hash:
        raise ValueError("Watermark content fingerprint does not match")


def watermark_from_dict(payload: Mapping[str, Any]) -> MaturityWatermark:
    """Deserialize aggregate evidence and validate its policy and content hash."""
    if set(payload) != {field.name for field in fields(MaturityWatermark)}:
        raise ValueError("Watermark fields are missing or unexpected")
    try:
        values = dict(payload)
        values["data_as_of"] = date.fromisoformat(values["data_as_of"])
        values["computed_at"] = datetime.fromisoformat(values["computed_at"])
        values["cohorts"] = tuple(
            CohortEstimate(
                **{**cohort, "event_month": date.fromisoformat(cohort["event_month"])}
            )
            for cohort in values["cohorts"]
        )
        values["availability_quality_counts"] = tuple(
            tuple(pair) for pair in values["availability_quality_counts"]
        )
        watermark = MaturityWatermark(**values)
        _validate_watermark(watermark)
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid maturity watermark: {error}") from error
    return watermark


def read_watermark(path: Path) -> MaturityWatermark:
    """Read either a bare watermark or a full aggregate evidence JSON artifact."""
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("A watermark artifact must be a JSON object")
    values = payload.get("watermark", payload)
    if not isinstance(values, dict):
        raise ValueError("The watermark must be a JSON object")
    return watermark_from_dict(values)
