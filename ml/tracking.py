"""MLflow run lineage, skip evidence and champion promotion for the frozen model."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np

from ml.dataset import TrainingDataset, split_purged
from ml.features import FEATURE_NAMES
from ml.maturity import MaturityWatermark, add_months, training_eligibility
from ml.train import (
    MODEL_PARAMETERS,
    MODEL_SPECIFICATION,
    ProbabilityModel,
    evaluate_candidate,
)


@dataclass(frozen=True)
class TrackingSettings:
    """Explicit external tracking boundary, with local artifacts by default."""

    tracking_uri: str
    model_name: str = "carrier-risk-v0"
    artifact_root: Path | None = None


@dataclass(frozen=True)
class TrainingRunResult:
    """A skipped, rejected or promoted run, with a durable MLflow identity."""

    run_id: str
    status: str
    reason: str
    model_version: str | None = None


def _json_value(value: object) -> Any:
    return json.loads(json.dumps(value, default=str))


def _champion(client: Any, model_name: str) -> Any:
    """Resolve alias absence separately from registry failures."""
    exceptions: Any = importlib.import_module("mlflow.exceptions")
    try:
        registered = client.get_registered_model(model_name)
    except exceptions.MlflowException as error:
        if error.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        return None
    version = registered.aliases.get("champion")
    return None if version is None else client.get_model_version(model_name, version)


def run_training(
    watermark: MaturityWatermark | None,
    dataset_factory: Callable[[], TrainingDataset],
    settings: TrackingSettings,
) -> TrainingRunResult:
    """Skip before dataset generation when maturity fails; promote only on evidence.

    Callers serialize monthly training with the warehouse training advisory lock.
    An incumbent is re-evaluated on these exact held-out rows; stored historical
    scores are never compared across different holdouts. Fixture models cannot be
    used as production incumbents.
    """
    mlflow: Any = importlib.import_module("mlflow")
    sklearn_api: Any = importlib.import_module("mlflow.sklearn")
    mlflow.set_tracking_uri(settings.tracking_uri)
    client = mlflow.MlflowClient(tracking_uri=settings.tracking_uri)
    experiment = client.get_experiment_by_name(settings.model_name)
    if experiment is None:
        artifact_location = None
        if settings.artifact_root is not None:
            settings.artifact_root.mkdir(parents=True, exist_ok=True)
            artifact_location = settings.artifact_root.resolve().as_uri()
        experiment_id = client.create_experiment(settings.model_name, artifact_location)
    else:
        experiment_id = experiment.experiment_id
    with mlflow.start_run(experiment_id=experiment_id) as run:
        run_id = str(run.info.run_id)
        mlflow.log_params(
            {
                **MODEL_PARAMETERS,
                "model_specification": MODEL_SPECIFICATION,
                "evaluation_metric": "average_precision",
                "feature_names": ",".join(FEATURE_NAMES),
            }
        )
        mlflow.set_tag("validation_fixture", "false")
        if watermark is not None:
            mlflow.log_dict(_json_value(asdict(watermark)), "maturity.json")
            mlflow.log_param("watermark_version", watermark.watermark_version)
        # This preliminary gate prevents even dataset work when the current
        # measurement is unusable. Each actual label boundary is checked below.
        reference = (
            date(2000, 1, 1)
            if watermark is None
            else add_months(watermark.data_as_of, -9)
        )
        eligibility = training_eligibility(watermark, label_window_end=reference)
        if not eligibility.eligible:
            mlflow.set_tags(
                {"training_status": "skipped", "reason": eligibility.reason}
            )
            return TrainingRunResult(run_id, "skipped", eligibility.reason)
        dataset = dataset_factory()
        if watermark is None or dataset.watermark_id != watermark.watermark_version:
            raise ValueError("Dataset watermark does not match the measured policy")
        if (
            dataset.grace_days != watermark.grace_days
            or dataset.data_as_of != watermark.data_as_of
        ):
            raise ValueError(
                "Dataset maturity cutoff does not match the measured policy"
            )
        for scoring_date in dataset.scoring_dates:
            if not training_eligibility(
                watermark, label_window_end=add_months(scoring_date, 6)
            ).eligible:
                raise ValueError("Dataset contains an immature label window")
        split = split_purged(dataset)
        manifest = {
            "fingerprint": dataset.fingerprint,
            "watermark_id": dataset.watermark_id,
            "data_as_of": dataset.data_as_of,
            "grace_days": dataset.grace_days,
            "row_count": dataset.row_count,
            "provenance": asdict(dataset.provenance),
            "train_dates": split.train_dates,
            "purged_dates": split.purged_dates,
            "test_dates": split.test_dates,
        }
        mlflow.log_dict(_json_value(manifest), "dataset.json")
        mlflow.log_param("training_set_fingerprint", dataset.fingerprint)
        mlflow.set_tags(
            {
                "training_end": split.train_dates[-1].isoformat(),
                "heldout_start": split.test_dates[0].isoformat(),
            }
        )
        incumbent: ProbabilityModel | None = None
        incumbent_version: str | None = None
        registered = _champion(client, settings.model_name)
        if registered is not None:
            if registered.tags.get("validation_fixture") == "true":
                raise ValueError("A fixture model cannot be a production incumbent")
            if registered.tags.get("feature_names") != ",".join(FEATURE_NAMES):
                raise ValueError("Incumbent has an incompatible feature contract")
            trained_through = registered.tags.get("training_end")
            if not trained_through or date.fromisoformat(trained_through) >= add_months(
                split.test_dates[0], -6
            ):
                raise ValueError("Incumbent training overlaps this purged holdout")
            incumbent_version = str(registered.version)
            loaded = sklearn_api.load_model(
                f"models:/{settings.model_name}/{incumbent_version}"
            )
            if loaded.n_features_in_ != len(FEATURE_NAMES) or not np.array_equal(
                loaded.classes_, np.asarray([0, 1])
            ):
                raise ValueError("Incumbent has an incompatible feature/class contract")
            incumbent = cast(ProbabilityModel, loaded)
        evaluation = evaluate_candidate(split.train, split.test, incumbent=incumbent)
        mlflow.log_metrics(evaluation.metrics)
        decision = evaluation.decision
        if not decision.promote:
            mlflow.set_tags({"training_status": "rejected", "reason": decision.reason})
            return TrainingRunResult(run_id, "rejected", decision.reason)
        values = np.asarray([row.features.numeric_values() for row in split.train[:5]])
        info = sklearn_api.log_model(
            evaluation.model,
            name="model",
            signature=mlflow.models.infer_signature(
                values, evaluation.model.predict_proba(values)
            ),
            input_example=values,
            pyfunc_predict_fn="predict_proba",
        )
        version = mlflow.register_model(info.model_uri, settings.model_name)
        version_number = str(version.version)
        for key, value in {
            "validation_fixture": "false",
            "training_end": split.train_dates[-1].isoformat(),
            "dataset_fingerprint": dataset.fingerprint,
            "watermark_version": dataset.watermark_id,
            "model_specification": MODEL_SPECIFICATION,
            "feature_names": ",".join(FEATURE_NAMES),
        }.items():
            client.set_model_version_tag(
                settings.model_name, version_number, key, value
            )
        # The surrounding advisory lock serializes cooperating publishers. Verify
        # an operator has not changed champion during fitting before replacing it.
        latest = _champion(client, settings.model_name)
        current_version = None if latest is None else str(latest.version)
        if current_version != incumbent_version:
            raise RuntimeError("Champion changed during evaluation; retry against it")
        client.set_registered_model_alias(
            settings.model_name, "champion", version_number
        )
        mlflow.set_tags({"training_status": "promoted", "reason": decision.reason})
        return TrainingRunResult(run_id, "promoted", decision.reason, version_number)
