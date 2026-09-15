"""Registry provenance and estimator contracts fail closed in demo serving."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest

from ml.demo_features import DEMO_FEATURE_NAMES
from serving.demo_app import PostgresDemoRepository, load_demo_model


@pytest.fixture
def version(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    value = SimpleNamespace(
        version="7",
        tags={
            "experimental": "true",
            "validation_fixture": "false",
            "feature_names": ",".join(DEMO_FEATURE_NAMES),
            "label_ascertainment": "retained_snapshot_not_eventual_completeness",
            "data_as_of": "2026-09-03",
            "threshold": "0.25",
        },
    )
    client = SimpleNamespace(get_model_version_by_alias=lambda *args: value)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "unit-uri")
    monkeypatch.setattr("serving.demo_app.mlflow.set_tracking_uri", lambda *args: None)
    monkeypatch.setattr("serving.demo_app.MlflowClient", lambda **kwargs: client)
    monkeypatch.setattr(
        "serving.demo_app.mlflow.sklearn.load_model",
        lambda *args: SimpleNamespace(n_features_in_=7, classes_=np.array([0, 1])),
    )
    return value


@pytest.mark.parametrize(
    "name,value",
    [
        ("experimental", "false"),
        ("validation_fixture", "true"),
        ("feature_names", "inspections_6m"),
        ("data_as_of", "2026-01-01"),
        ("label_ascertainment", "complete"),
    ],
)
def test_incompatible_provenance_rejected(
    version: SimpleNamespace, name: str, value: str
) -> None:
    version.tags[name] = value
    with pytest.raises(ValueError, match="provenance"):
        load_demo_model()


@pytest.mark.parametrize("threshold", ["nan", "1.5", "-0.1"])
def test_invalid_threshold_rejected(version: SimpleNamespace, threshold: str) -> None:
    version.tags["threshold"] = threshold
    with pytest.raises(ValueError, match="threshold"):
        load_demo_model()


def test_wrong_estimator_class_contract_rejected(
    version: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "serving.demo_app.mlflow.sklearn.load_model",
        lambda *args: SimpleNamespace(n_features_in_=7, classes_=np.array([1, 0])),
    )
    with pytest.raises(ValueError, match="estimator"):
        load_demo_model()


def test_pool_reads_four_month_features_in_order_and_null_rate_as_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = MagicMock()
    monkeypatch.setattr("serving.demo_app.ConnectionPool", lambda *args, **kwargs: pool)
    repository = PostgresDemoRepository("unit-dsn")
    row = dict(zip(DEMO_FEATURE_NAMES, [2, 0, 0, 3, 0.0, None, 4], strict=True))
    connection = pool.connection.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = row
    repository.open()
    from datetime import date

    assert repository.lookup("123", date(2026, 9, 1)) == (2, 0, 0, 3, 0, 0, 4)
    connection.execute.return_value.fetchone.return_value = None
    assert repository.lookup("999", date(2026, 9, 1)) is None
    repository.close()
    pool.close.assert_called_once()
