"""A versioned PostgreSQL registry for measured label-maturity policies."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, date, datetime, time
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from ml.dataset import SourceEvent
from ml.maturity import (
    CrashReportVersion,
    MaturityWatermark,
    calculate_watermark,
    watermark_from_dict,
)

TRAINING_LOCK_KEY = 764301235


@contextmanager
def training_lock(database_url: str) -> Iterator[psycopg.Connection[tuple[Any, ...]]]:
    """Serialize measurement, dataset publication, fitting, and model promotion."""
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute("select pg_advisory_lock(%s)", (TRAINING_LOCK_KEY,))
        try:
            yield connection
        finally:
            connection.execute("select pg_advisory_unlock(%s)", (TRAINING_LOCK_KEY,))


class PostgresWatermarkStore:
    """Preserve immutable versions and exactly one transactional current policy.

    This Python-owned operational registry is deliberately outside dbt's modeled
    relation graph. SQL constraints fix its policy and shape; the strict maturity
    reader validates every stored aggregate and its content hash before use.
    """

    def __init__(
        self,
        connection: psycopg.Connection[tuple[Any, ...]],
        *,
        schema: str = "modeled",
        raw_schema: str = "raw",
    ) -> None:
        self.connection = connection
        self.schema = schema
        self.raw_schema = raw_schema
        self.relation = sql.Identifier(schema, "label_maturity_watermarks")

    def _ensure_table(self) -> None:
        self.connection.execute(
            sql.SQL("create schema if not exists {}").format(
                sql.Identifier(self.schema)
            )
        )
        self.connection.execute(
            sql.SQL("""
            create table if not exists {} (
                watermark_version text primary key,
                label_definition text not null,
                quantile numeric not null check (quantile = 0.995),
                confidence numeric not null check (confidence = 0.95),
                cohort_start date not null, cohort_end date not null,
                cohort_count integer not null check (cohort_count = 12),
                sample_size bigint not null check (sample_size >= 0),
                grace_days integer check (grace_days >= 0),
                calculation_version text not null,
                data_as_of date not null, computed_at timestamptz not null,
                is_current boolean not null,
                metadata jsonb not null,
                source_audit jsonb not null,
                check (watermark_version = metadata->>'watermark_version'),
                check (data_as_of <= computed_at::date),
                check (cohort_start <= cohort_end)
            )
        """).format(self.relation)
        )
        self.connection.execute(
            sql.SQL("""
            create unique index if not exists label_maturity_one_current
            on {} (is_current) where is_current
        """).format(self.relation)
        )

    def latest(self) -> MaturityWatermark | None:
        """Read the current policy, rejecting missing or rolled-back current state.

        Only a genuinely empty registry is unmeasured. Treating lost current state
        as a bootstrap would discard the previously approved grace on the next run.
        The policy and history-presence check use the same statement snapshot.
        """
        self._ensure_table()
        row = self.connection.execute(
            sql.SQL("""
                select
                    (select metadata from {} where is_current),
                    exists (select 1 from {}),
                    exists (
                        select 1 from {} current_policy
                        join {} successor
                          on successor.metadata->>'previous_watermark_version'
                             = current_policy.watermark_version
                        where current_policy.is_current
                    )
            """).format(self.relation, self.relation, self.relation, self.relation)
        ).fetchone()
        if row is None:
            raise ValueError("maturity registry lookup returned no status")
        if row[0] is None:
            if row[1]:
                raise ValueError("maturity registry history has no current policy")
            return None
        if row[2]:
            raise ValueError(
                "current maturity policy already has a persisted successor"
            )
        return watermark_from_dict(row[0])

    def persist(
        self,
        watermark: MaturityWatermark,
        *,
        source_audit: dict[str, object] | None = None,
    ) -> MaturityWatermark:
        """Publish atomically; retries preserve the first stored immutable bytes.

        A replacement must name the exact previous version. Unreviewed decreases
        retain its effective grace, and stale imports cannot move current backward.
        """
        verified = watermark_from_dict(json.loads(watermark.to_json()))
        with self.connection.transaction():
            self.connection.execute(
                "select pg_advisory_xact_lock(%s)", (TRAINING_LOCK_KEY,)
            )
            previous = self.latest()
            if (
                previous is not None
                and previous.watermark_version == verified.watermark_version
            ):
                return previous
            expected_previous = None if previous is None else previous.watermark_version
            if verified.previous_watermark_version != expected_previous:
                raise ValueError("watermark does not name the current previous version")
            if previous is not None:
                if (
                    verified.status == "incomplete"
                    and verified.grace_days != previous.grace_days
                ):
                    raise ValueError(
                        "an incomplete watermark must retain the previous grace"
                    )
                if verified.data_as_of < previous.data_as_of:
                    raise ValueError(
                        "a stale watermark cannot replace the current policy"
                    )
                if (
                    previous.grace_days is not None
                    and verified.grace_days is not None
                    and verified.grace_days < previous.grace_days
                    and not verified.reviewed_decrease
                ):
                    raise ValueError(
                        "a watermark grace decrease requires recorded review"
                    )
            self.connection.execute(
                sql.SQL("update {} set is_current = false where is_current").format(
                    self.relation
                )
            )
            self.connection.execute(
                sql.SQL("""
                insert into {} (
                    watermark_version, label_definition, quantile, confidence,
                    cohort_start, cohort_end, cohort_count, sample_size, grace_days,
                    calculation_version, data_as_of, computed_at, is_current,
                    metadata, source_audit
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s::jsonb,%s::jsonb)
            """).format(self.relation),
                (
                    verified.watermark_version,
                    verified.label_definition,
                    verified.quantile,
                    verified.confidence,
                    verified.cohorts[0].event_month,
                    verified.cohorts[-1].event_month,
                    len(verified.cohorts),
                    verified.sample_size,
                    verified.grace_days,
                    verified.calculation_version,
                    verified.data_as_of,
                    verified.computed_at,
                    verified.to_json(),
                    json.dumps(source_audit or {}),
                ),
            )
            return verified

    def measure(
        self,
        *,
        data_as_of: date,
        computed_at: datetime,
        max_source_rows: int = 20_000_000,
    ) -> MaturityWatermark:
        """Measure complete modeled crash history, preserving first-version identity.

        Quarantined or unkeyable history makes coverage incomplete. Missing-carrier
        rows with valid incident keys are passed through for exact exclusion counts.
        Federal recordability is the existing clean-layer eligibility contract,
        never a guess from fatalities, injuries, or tow-away severity.
        """
        if max_source_rows < 1:
            raise ValueError("max_source_rows must be positive")
        with self.connection.transaction():
            self.connection.execute("set transaction isolation level repeatable read")
            with self.connection.cursor(row_factory=dict_row) as cursor:
                rows = cursor.execute(
                    sql.SQL("""
                    select * from {}.event_versions
                    where feed_name = 'crashes' and knowledge_valid_from < %s
                    order by event_version_key limit %s
                """).format(sql.Identifier(self.schema)),
                    (
                        datetime.combine(data_as_of, time.min, UTC),
                        max_source_rows + 1,
                    ),
                ).fetchall()
                batches = cursor.execute(
                    sql.SQL("""
                    select applied.batch_id, applied.observed_at
                    from {}.event_version_batches applied
                    join {}.snapshot_batches raw
                    using (batch_id, feed_name, observed_at)
                    where applied.feed_name = 'crashes' and raw.status = 'loaded'
                """).format(
                        sql.Identifier(self.schema), sql.Identifier(self.raw_schema)
                    )
                ).fetchall()
            if len(rows) > max_source_rows:
                raise ValueError(
                    "maturity source exceeds the explicit extraction row limit"
                )
            batch_ids = {str(row["batch_id"]) for row in batches}
            complete = (
                bool(batches)
                and max(row["observed_at"].astimezone(UTC).date() for row in batches)
                >= data_as_of
            )
            reports: list[CrashReportVersion] = []
            unkeyable = 0
            digest = hashlib.sha256()
            for row in rows:
                event = SourceEvent.from_mapping(row)
                digest.update(
                    json.dumps(asdict(event), default=str, sort_keys=True).encode()
                )
                if event.first_seen_batch_id not in batch_ids:
                    complete = False
                if event.incident_key is None or event.event_date is None:
                    unkeyable += 1
                    complete = False
                    continue
                reason = row.get("exclusion_reason")
                if not event.is_model_eligible and reason not in (
                    "missing_usdot_number",
                    "not_federally_recordable",
                ):
                    complete = False
                reports.append(
                    CrashReportVersion(
                        usdot_number=event.usdot_number,
                        report_state=event.incident_key[1],
                        report_number=event.incident_key[2],
                        event_date=event.event_date,
                        report_time=str(event.incident_key[4]),
                        source_record_key=event.source_record_key,
                        reported_date=event.reported_date,
                        knowledge_valid_from=event.knowledge_valid_from,
                        federal_recordable=event.is_model_eligible
                        or reason == "missing_usdot_number",
                        availability_quality=event.availability_quality,
                        is_deleted=event.is_deleted,
                    )
                )
            digest.update(json.dumps(sorted(batch_ids)).encode())
            previous = self.latest()
            if (
                previous is not None
                and previous.data_as_of == data_as_of
                and previous.source_fingerprint == digest.hexdigest()
                and previous.source_complete == complete
            ):
                return previous
            policy = calculate_watermark(
                reports,
                data_as_of=data_as_of,
                computed_at=computed_at,
                source_fingerprint=digest.hexdigest(),
                source_complete=complete,
                previous=previous,
            )
            return self.persist(
                policy,
                source_audit={
                    "source_version_rows": len(rows),
                    "unkeyable_excluded_rows": unkeyable,
                    "complete_batch_ids": sorted(batch_ids),
                },
            )
