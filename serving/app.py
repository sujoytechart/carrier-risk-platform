"""FastAPI entry point for typed scores, readiness, and Prometheus metrics."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from time import perf_counter
from typing import Literal

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel, Field

from serving.model import load_promoted_model
from serving.repository import PostgresFeatureRepository
from serving.service import (
    FailureStatus,
    FeatureLookup,
    FeatureRepository,
    FeatureStoreUnavailable,
    ModelUnavailable,
    RiskModel,
    ScoredCarrier,
    score_carrier,
)

LOGGER = logging.getLogger(__name__)


class ScoreResponse(BaseModel):
    """A probability whose feature date and registry version are inspectable."""

    status: Literal["scored"] = "scored"
    usdot_number: str
    risk_score: float = Field(ge=0, le=1)
    model_version: str
    features_as_of: date
    computed_at: datetime
    validation_fixture: bool


class FailureResponse(BaseModel):
    """The service emits no probability when its guarantees cannot be satisfied."""

    status: FailureStatus
    usdot_number: str | None
    computed_at: datetime


class ReadinessResponse(BaseModel):
    """Startup availability; requests independently detect later warehouse faults."""

    status: Literal["ready", "unavailable"]
    model_version: str | None
    validation_fixture: bool


class UnavailableRepository:
    """Preserve typed failures when warehouse setup fails at application startup."""

    def lookup(self, usdot_number: str, scoring_date: date) -> FeatureLookup:
        """Fail closed until the operator repairs configuration and restarts."""
        raise FeatureStoreUnavailable("Warehouse startup did not succeed")


@dataclass
class Runtime:
    """Explicit shared state initialized once by the application lifespan."""

    repository: FeatureRepository
    model: RiskModel | None = None
    warehouse_ready: bool = False


def _load_configured_model() -> RiskModel:
    profile = os.getenv("CARRIER_RISK_VALIDATION_PROFILE", "production")
    if profile not in {"production", "synthetic"}:
        raise ModelUnavailable("Unknown serving validation profile")
    return load_promoted_model(
        tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        model_name=os.getenv("CARRIER_RISK_MODEL_NAME", "carrier-risk-v0"),
        alias=os.getenv("CARRIER_RISK_MODEL_ALIAS", "champion"),
        validation_fixture=profile == "synthetic",
    )


def create_app(
    *,
    repository: FeatureRepository | None = None,
    model_loader: Callable[[], RiskModel] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FastAPI:
    """Build an independently instrumented app with injectable external boundaries."""
    registry = CollectorRegistry()
    requests = Counter(
        "carrier_risk_score_requests_total",
        "Score responses by outcome",
        ["outcome"],
        registry=registry,
    )
    duration = Histogram(
        "carrier_risk_score_request_duration_seconds",
        "Scoring handler elapsed time",
        ["outcome"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.12, 0.25, 0.5, 1, 2, 5),
        registry=registry,
    )
    model_available = Gauge(
        "carrier_risk_model_available",
        "A promoted classifier is loaded",
        registry=registry,
    )
    runtime = Runtime(repository or UnavailableRepository())

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        warehouse: PostgresFeatureRepository | None = None
        if repository is not None:
            runtime.warehouse_ready = True
        elif database_url := os.getenv("CARRIER_RISK_DATABASE_URL"):
            warehouse = PostgresFeatureRepository(database_url)
            try:
                warehouse.open()
                runtime.repository = warehouse
                runtime.warehouse_ready = True
            except FeatureStoreUnavailable:
                LOGGER.error("warehouse_startup_unavailable")
        try:
            runtime.model = (model_loader or _load_configured_model)()
        except ModelUnavailable:
            LOGGER.error("promoted_model_startup_unavailable")
        model_available.set(int(runtime.model is not None))
        try:
            yield
        finally:
            if warehouse is not None:
                warehouse.close()

    application = FastAPI(title="Carrier Risk API", version="0.1.0", lifespan=lifespan)

    @application.get(
        "/score/{usdot_number}",
        response_model=ScoreResponse,
        responses={404: {"model": FailureResponse}, 503: {"model": FailureResponse}},
    )
    def score(usdot_number: str) -> ScoreResponse | JSONResponse:
        started = perf_counter()
        result = score_carrier(
            usdot_number,
            repository=runtime.repository,
            model=runtime.model,
            computed_at=clock(),
        )
        requests.labels(outcome=result.status).inc()
        duration.labels(outcome=result.status).observe(perf_counter() - started)
        if isinstance(result, ScoredCarrier):
            return ScoreResponse.model_validate(asdict(result))
        failure = FailureResponse.model_validate(asdict(result))
        return JSONResponse(
            status_code=404 if result.status == "insufficient_history" else 503,
            content=failure.model_dump(mode="json"),
        )

    @application.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @application.get("/health/ready", response_model=ReadinessResponse)
    def ready(response: Response) -> ReadinessResponse:
        is_ready = runtime.model is not None and runtime.warehouse_ready
        response.status_code = 200 if is_ready else 503
        return ReadinessResponse(
            status="ready" if is_ready else "unavailable",
            model_version=runtime.model.version if runtime.model is not None else None,
            validation_fixture=(
                runtime.model.validation_fixture if runtime.model is not None else False
            ),
        )

    @application.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(
            generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    return application


app = create_app()
