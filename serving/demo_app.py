"""Explicitly experimental FastAPI service for the four-month learning model."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from time import perf_counter
from typing import Protocol

import mlflow
import mlflow.sklearn
import numpy as np
import psycopg
from fastapi import FastAPI, HTTPException, Response
from mlflow.tracking import MlflowClient
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ml.demo_features import DEMO_FEATURE_NAMES, demo_feature_values
from ml.demo_training import DEMO_MODEL_NAME, TEST_DATE, TRAIN_DATE
from ml.train import ProbabilityModel, positive_probabilities
from serving.service import normalize_usdot

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DemoModel:
    """A resolved experimental version with its training-selected threshold."""

    classifier: ProbabilityModel
    version: str
    threshold: float


class DemoRepository(Protocol):
    """Read one explicit built snapshot; no current-date eligibility claims."""

    def open(self) -> None: ...

    def close(self) -> None: ...

    def lookup(self, usdot: str, scoring_date: date) -> tuple[float, ...] | None: ...


class PostgresDemoRepository:
    """Bounded pooled lookups from the separate contracted demo relation."""

    def __init__(self, database_url: str) -> None:
        self.pool: ConnectionPool[psycopg.Connection[dict[str, object]]] = (
            ConnectionPool(
                database_url,
                min_size=1,
                max_size=12,
                timeout=2,
                open=False,
                kwargs={
                    "row_factory": dict_row,
                    "autocommit": True,
                    "connect_timeout": 3,
                    "options": "-c statement_timeout=2000",
                },
            )
        )

    def open(self) -> None:
        """Fail promptly if the isolated feature store is unavailable."""
        self.pool.open(wait=True, timeout=4)

    def close(self) -> None:
        """Release connections on service shutdown."""
        self.pool.close()

    def lookup(self, usdot: str, scoring_date: date) -> tuple[float, ...] | None:
        """Read only features; held-out outcomes never enter an API prediction."""
        with self.pool.connection() as connection:
            row = connection.execute(
                """
                select inspections_4m, violations_4m, oos_violations_4m,
                    crashes_24m, violations_per_inspection, oos_violation_rate,
                    days_since_last_inspection
                from learning_demo.demo_training_features
                where usdot_number = %s and scoring_date = %s
            """,
                (usdot, scoring_date),
            ).fetchone()
        if row is None:
            return None
        return demo_feature_values(row)


def load_demo_model() -> DemoModel:
    """Require the isolated registry name, explicit demo tags and input contract."""
    uri = os.environ["MLFLOW_TRACKING_URI"]
    mlflow.set_tracking_uri(uri)
    version = MlflowClient(tracking_uri=uri).get_model_version_by_alias(
        DEMO_MODEL_NAME, "demo"
    )
    required = {
        "experimental": "true",
        "validation_fixture": "false",
        "feature_names": ",".join(DEMO_FEATURE_NAMES),
        "label_ascertainment": "retained_snapshot_not_eventual_completeness",
        "data_as_of": "2026-09-03",
    }
    if any(version.tags.get(name) != value for name, value in required.items()):
        raise ValueError("Demo model provenance or feature contract does not match")
    classifier = mlflow.sklearn.load_model(
        f"models:/{DEMO_MODEL_NAME}/{version.version}"
    )
    if classifier.n_features_in_ != 7 or not np.array_equal(
        classifier.classes_, [0, 1]
    ):
        raise ValueError("Incompatible demo estimator")
    threshold = float(version.tags["threshold"])
    if not 0 <= threshold <= 1:
        raise ValueError("Invalid demo threshold")
    return DemoModel(classifier, str(version.version), threshold)


def create_demo_app(
    *,
    repository: DemoRepository | None = None,
    model_loader: Callable[[], DemoModel] = load_demo_model,
) -> FastAPI:
    """Create an isolated demo app with injectable warehouse and model boundaries."""
    registry = CollectorRegistry()
    requests = Counter(
        "carrier_risk_demo_requests_total",
        "Experimental scores",
        ["outcome"],
        registry=registry,
    )
    duration = Histogram(
        "carrier_risk_demo_duration_seconds",
        "Experimental scoring time",
        registry=registry,
    )
    store = repository
    model: DemoModel | None = None
    ready = False

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        nonlocal store, model, ready
        stage = "warehouse_open"
        try:
            if store is None:
                store = PostgresDemoRepository(os.environ["CARRIER_RISK_DATABASE_URL"])
            store.open()
            stage = "model_load"
            model = model_loader()
            ready = True
        except Exception as error:
            ready = False
            LOGGER.error(
                "Demo startup failed: stage=%s error=%s", stage, type(error).__name__
            )
        try:
            yield
        finally:
            if store is not None:
                store.close()

    app = FastAPI(title="Carrier Risk — experimental real-data demo", lifespan=lifespan)

    @app.get("/readyz")
    def readiness() -> Response:
        """Report startup readiness without exposing connection configuration."""
        return Response(
            "ready" if ready else "unavailable", status_code=200 if ready else 503
        )

    @app.get("/demo/score/{identifier}")
    def score(
        identifier: str, scoring_date: date = date(2026, 9, 1)
    ) -> dict[str, object]:
        """Return a snapshot-based estimate with its limitations and model version."""
        started = perf_counter()
        outcome = "unavailable"
        try:
            if not ready or store is None or model is None:
                raise HTTPException(503, "Demo warehouse or model unavailable")
            if scoring_date.isoformat() not in {TRAIN_DATE, TEST_DATE, "2026-09-01"}:
                raise HTTPException(422, "Choose a built demo snapshot date")
            usdot = normalize_usdot(identifier)
            if usdot is None:
                raise HTTPException(422, "Invalid USDOT identifier")
            stage = "feature_lookup"
            try:
                values = store.lookup(usdot, scoring_date)
                if values is None:
                    outcome = "insufficient_history"
                    raise HTTPException(404, "No eligible four-month demo snapshot")
                stage = "feature_validation"
                vector = np.asarray([values], dtype=np.float64)
                if (
                    vector.shape != (1, 7)
                    or not np.isfinite(vector).all()
                    or (vector < 0).any()
                ):
                    raise ValueError("Invalid warehouse feature vector")
                if vector[0, 0] < 1 or vector[0, 6] < 1:
                    raise ValueError("Features require a preceding inspection")
                stage = "prediction"
                probability = float(positive_probabilities(model.classifier, vector)[0])
            except HTTPException:
                raise
            except Exception as error:
                LOGGER.error(
                    "Demo scoring failed: stage=%s error=%s",
                    stage,
                    type(error).__name__,
                )
                raise HTTPException(503, "Demo scoring unavailable") from error
            outcome = "scored"
            return {
                "status": "scored",
                "usdot_number": usdot,
                "risk_score": probability,
                "predicted_positive": probability >= model.threshold,
                "classification_threshold": model.threshold,
                "model_name": DEMO_MODEL_NAME,
                "model_version": model.version,
                "features_as_of": scoring_date,
                "data_as_of": "2026-09-03",
                "computed_at": datetime.now(UTC),
                "experimental": True,
                "validation_fixture": False,
                "inspection_lookback_months": 4,
                "target": "qualifying_recorded_federal_crash_within_six_months",
                "label_ascertainment": "retained_snapshot_not_eventual_completeness",
                "evaluation": "retrospective_later_period_holdout",
            }
        finally:
            requests.labels(outcome=outcome).inc()
            duration.observe(perf_counter() - started)

    @app.get("/metrics")
    def metrics() -> Response:
        """Expose independent demo counters without changing production metrics."""
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/demo/example")
    def example() -> dict[str, object]:
        """Show a real configured carrier estimate while withholding its identity."""
        identifier = os.getenv("CARRIER_RISK_DEMO_EXAMPLE_USDOT")
        if not identifier:
            raise HTTPException(503, "No real-data example configured")
        result = score(identifier)
        result.pop("usdot_number")
        result["identifier_redacted"] = True
        return result

    return app


app = create_demo_app()
