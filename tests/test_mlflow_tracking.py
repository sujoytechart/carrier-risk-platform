"""MLflow records skipped runs and only points champion at validated models."""

from pathlib import Path

import pytest
from mlflow import MlflowClient

from ml.tracking import TrackingSettings, run_training


def test_missing_maturity_logs_skip_without_building_dataset(tmp_path: Path) -> None:
    settings = TrackingSettings(
        f"sqlite:///{tmp_path}/tracking.db", "test-model", tmp_path / "artifacts"
    )

    def forbidden():
        raise AssertionError("dataset must not be built for an unmeasured watermark")

    result = run_training(None, forbidden, settings)
    assert result.status == "skipped"
    assert result.reason == "maturity_unmeasured"
    run = MlflowClient(tracking_uri=settings.tracking_uri).get_run(result.run_id)
    assert run.data.tags["training_status"] == "skipped"
    assert not run.data.metrics
    assert not MlflowClient(
        tracking_uri=settings.tracking_uri
    ).search_registered_models()


def training_fixture():
    from datetime import UTC, date, datetime

    from ml.dataset import DatasetProvenance, TrainingDataset, TrainingRow
    from ml.features import FeatureRow
    from ml.maturity import (
        CrashReportVersion,
        add_months,
        calculate_watermark,
        mature_cohort_months,
    )

    cutoff = date(2026, 9, 3)
    reports = tuple(
        CrashReportVersion(
            "1",
            "MI",
            str(i),
            month,
            "1200",
            str(i),
            month.replace(day=2),
            datetime(month.year, month.month, 2, tzinfo=UTC),
            True,
        )
        for i, month in enumerate(mature_cohort_months(cutoff))
    )
    watermark = calculate_watermark(
        reports,
        data_as_of=cutoff,
        computed_at=datetime(2026, 9, 3, tzinfo=UTC),
        source_fingerprint="synthetic-unit-fixture",
        bootstrap_replicates=2000,
    )
    dates = tuple(add_months(date(2022, 1, 1), i) for i in range(18))
    values = tuple(
        TrainingRow(
            FeatureRow(str(i + 1), d, 10, i, 0, 0, i / 10, None if i == 0 else 0.0, 1),
            int(i >= 10),
        )
        for d in dates
        for i in range(20)
    )
    dataset = TrainingDataset(
        values,
        dates,
        cutoff,
        watermark.watermark_version,
        1,
        DatasetProvenance(
            (("source_proxy", 12),), 0, 0, 0, "synthetic", ("synthetic",)
        ),
        "synthetic-dataset-fingerprint",
    )
    return watermark, dataset


def test_real_mlflow_registers_winner_then_preserves_incumbent_on_tie(
    tmp_path: Path,
) -> None:
    from mlflow import MlflowClient

    settings = TrackingSettings(
        f"sqlite:///{tmp_path}/tracking.db",
        "test-registered-model",
        tmp_path / "artifacts",
    )
    watermark, dataset = training_fixture()
    result = run_training(watermark, lambda: dataset, settings)
    assert result.status == "promoted"
    client = MlflowClient(tracking_uri=settings.tracking_uri)
    champion = client.get_model_version_by_alias(settings.model_name, "champion")
    assert str(champion.version) == result.model_version
    assert champion.tags["watermark_version"] == watermark.watermark_version
    assert (
        client.get_run(result.run_id).data.metrics["candidate_average_precision"] == 1.0
    )
    rejected = run_training(watermark, lambda: dataset, settings)
    assert rejected.status == "rejected"
    assert rejected.reason == "did_not_beat_incumbent"
    assert (
        client.get_model_version_by_alias(settings.model_name, "champion").version
        == champion.version
    )


def test_dataset_policy_mismatch_never_registers(tmp_path: Path) -> None:
    from dataclasses import replace

    import pytest

    settings = TrackingSettings(
        f"sqlite:///{tmp_path}/tracking.db", "mismatch", tmp_path / "artifacts"
    )
    watermark, dataset = training_fixture()
    with pytest.raises(ValueError, match="watermark"):
        run_training(
            watermark, lambda: replace(dataset, watermark_id="different"), settings
        )


@pytest.mark.parametrize("defect", ["feature_order", "class_order", "feature_width"])
def test_incompatible_incumbent_cannot_participate_in_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defect: str
) -> None:
    from datetime import date
    from types import SimpleNamespace

    import mlflow.sklearn
    import numpy as np

    import ml.tracking
    from ml.features import FEATURE_NAMES
    from ml.train import evaluate_candidate
    from tests.test_model_training import rows

    watermark, dataset = training_fixture()
    incumbent = evaluate_candidate(rows(date(2022, 1, 1)), rows(date(2023, 1, 1))).model
    tags = {
        "validation_fixture": "false",
        "training_end": "2022-01-01",
        "feature_names": ",".join(FEATURE_NAMES),
    }
    if defect == "feature_order":
        tags["feature_names"] = ",".join(reversed(FEATURE_NAMES))
    elif defect == "class_order":
        incumbent.classes_ = np.array([1, 0])
    else:
        incumbent.n_features_in_ = 8
    monkeypatch.setattr(
        ml.tracking,
        "_champion",
        lambda client, name: SimpleNamespace(version="1", tags=tags),
    )
    monkeypatch.setattr(mlflow.sklearn, "load_model", lambda uri: incumbent)
    settings = TrackingSettings(
        f"sqlite:///{tmp_path}/tracking.db", "bad-incumbent", tmp_path / "artifacts"
    )
    with pytest.raises(ValueError, match="contract"):
        run_training(watermark, lambda: dataset, settings)
    assert not MlflowClient(
        tracking_uri=settings.tracking_uri
    ).search_registered_models()
