"""Monthly maturity measurement, immutable datasets, and gated training."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import psycopg

from ml.dataset import (
    SourceCoverage,
    TrainingDataset,
    generate_scoring_dates,
    load_training_dataset,
    split_purged,
)
from ml.maturity import (
    MaturityWatermark,
    add_months,
    read_watermark,
    training_eligibility,
)
from ml.tracking import TrackingSettings, TrainingRunResult, run_training
from ml.watermark_store import PostgresWatermarkStore, training_lock
from orchestration.dbt_lock import PostgresBuildLock
from orchestration.dbt_runner import DbtRunner

Action = Literal["measure", "build", "train"]


@dataclass(frozen=True)
class MonthlyDependencies:
    """Replace database and MLflow boundaries without bypassing policy gates."""

    measure: Callable[[], MaturityWatermark]
    build: Callable[[MaturityWatermark], TrainingDataset]
    train: Callable[
        [MaturityWatermark, Callable[[], TrainingDataset]], TrainingRunResult
    ]


@dataclass(frozen=True)
class MonthlyResult:
    """Small durable task result; carrier records never travel through XCom."""

    status: str
    reason: str
    watermark_id: str
    run_id: str | None = None
    dataset_fingerprint: str | None = None
    row_count: int | None = None


def training_branch(watermark: MaturityWatermark) -> str:
    """Choose the Airflow path using the same validated gate as model training."""
    decision = training_eligibility(
        watermark, label_window_end=add_months(watermark.data_as_of, -9)
    )
    return "fit_model" if decision.eligible else "record_skipped_training"


def run_monthly(action: Action, dependencies: MonthlyDependencies) -> MonthlyResult:
    """Measure first, keep extraction lazy, and record every training skip."""
    watermark = dependencies.measure()
    decision = training_eligibility(
        watermark, label_window_end=add_months(watermark.data_as_of, -9)
    )
    if action == "train":
        result = dependencies.train(watermark, lambda: dependencies.build(watermark))
        return MonthlyResult(
            result.status,
            result.reason,
            watermark.watermark_version,
            run_id=result.run_id,
        )
    if not decision.eligible:
        return MonthlyResult("skipped", decision.reason, watermark.watermark_version)
    if action == "measure":
        return MonthlyResult("eligible", decision.reason, watermark.watermark_version)
    dataset = dependencies.build(watermark)
    split_purged(dataset)
    return MonthlyResult(
        "built",
        "mature_monthly_grid",
        watermark.watermark_version,
        dataset_fingerprint=dataset.fingerprint,
        row_count=dataset.row_count,
    )


def freeze_dataset(dataset: TrainingDataset, directory: Path) -> Path:
    """Persist full row/provenance bytes once, rejecting conflicting replay."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dataset.fingerprint}.json"
    payload = json.dumps(asdict(dataset), default=str, sort_keys=True, indent=2) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".dataset-", dir=directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            if path.read_text() != payload:
                raise ValueError(
                    "existing immutable training dataset has conflicting bytes"
                ) from error
    finally:
        temporary.unlink(missing_ok=True)
    return path


def run_from_environment(
    action: Action,
    *,
    watermark_path: Path | None = None,
    data_as_of: date | None = None,
    expected_watermark_id: str | None = None,
    inspection_start: date | None = None,
    crash_start: date | None = None,
    max_source_rows: int = 20_000_000,
) -> MonthlyResult:
    """Run the production adapters under training and dbt publication locks.

    Imported watermark evidence follows the same registry checks as warehouse
    measurements. Explicit source bounds are required only after the policy gate
    permits extraction. No environment variable can shorten temporal thresholds.
    """
    database_url = _required_environment("CARRIER_RISK_DATABASE_URL")
    build_guard = PostgresBuildLock(
        lambda: psycopg.connect(database_url, autocommit=True)
    )
    with training_lock(database_url) as connection, build_guard() as backend_pid:
        store = PostgresWatermarkStore(connection)

        def measure() -> MaturityWatermark:
            if expected_watermark_id is not None:
                current = store.latest()
                if (
                    current is None
                    or current.watermark_version != expected_watermark_id
                ):
                    raise ValueError(
                        "current watermark changed after the Airflow branch"
                    )
                return current
            if watermark_path is not None:
                return store.persist(read_watermark(watermark_path))
            return store.measure(
                data_as_of=data_as_of or datetime.now(UTC).date(),
                computed_at=datetime.now(UTC),
                max_source_rows=max_source_rows,
            )

        def build(watermark: MaturityWatermark) -> TrainingDataset:
            inspections = inspection_start or date.fromisoformat(
                _required_environment("CARRIER_RISK_INSPECTION_START")
            )
            crashes = crash_start or date.fromisoformat(
                _required_environment("CARRIER_RISK_CRASH_START")
            )
            batches = connection.execute(
                "select batch_id from modeled.event_version_batches order by batch_id"
            ).fetchall()
            coverage = SourceCoverage(
                inspections,
                crashes,
                watermark.data_as_of,
                tuple(str(row[0]) for row in batches),
            )
            dates = generate_scoring_dates(coverage, watermark)
            build_dates = sorted({*dates, watermark.data_as_of.replace(day=1)})
            # The outer guard owns the session checked by dbt's existing startup
            # macro. Reuse it instead of deadlocking on a second connection.
            runner = DbtRunner(
                project_dir=Path(os.getenv("CARRIER_RISK_PROJECT_DIR", ".")),
                profiles_dir=Path(os.getenv("CARRIER_RISK_DBT_PROFILES_DIR", "config")),
                build_lock=lambda: nullcontext(backend_pid),
            )
            runner.run(
                "build",
                [
                    "--select",
                    "training_features",
                    "tag:temporal",
                    "--vars",
                    json.dumps(
                        {"scoring_dates": [day.isoformat() for day in build_dates]}
                    ),
                ],
            )
            dataset = load_training_dataset(
                database_url,
                coverage=coverage,
                watermark=watermark,
                max_source_rows=max_source_rows,
            )
            freeze_dataset(
                dataset,
                Path(os.getenv("CARRIER_RISK_TRAINING_OUTPUT", "data/training")),
            )
            return dataset

        settings = TrackingSettings(
            os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db"),
            model_name=os.getenv("CARRIER_RISK_MODEL_NAME", "carrier-risk-v0"),
        )
        return run_monthly(
            action,
            MonthlyDependencies(
                measure,
                build,
                lambda policy, factory: run_training(policy, factory, settings),
            ),
        )


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Set {name} before running an eligible monthly job")
    return value


def main() -> None:
    """Expose monthly jobs with explicit evidence and attested coverage inputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("measure", "build", "train"))
    parser.add_argument(
        "--watermark", type=Path, help="Validated aggregate evidence JSON"
    )
    parser.add_argument("--data-as-of", type=date.fromisoformat)
    parser.add_argument(
        "--inspection-start",
        type=date.fromisoformat,
        help="Attested start of complete inspection event history",
    )
    parser.add_argument(
        "--crash-start",
        type=date.fromisoformat,
        help="Attested start of complete crash event history",
    )
    parser.add_argument(
        "--max-source-rows",
        type=int,
        default=20_000_000,
        help="Memory guard; oversized extracts fail without truncation",
    )
    arguments = parser.parse_args()
    try:
        result = run_from_environment(
            arguments.action,
            watermark_path=arguments.watermark,
            data_as_of=arguments.data_as_of,
            inspection_start=arguments.inspection_start,
            crash_start=arguments.crash_start,
            max_source_rows=arguments.max_source_rows,
        )
    except (ValueError, RuntimeError) as error:
        parser.error(str(error))
    print(json.dumps(asdict(result), sort_keys=True))


if __name__ == "__main__":
    main()
