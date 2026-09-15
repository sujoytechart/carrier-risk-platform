"""Exercise registry publication and serving isolation with synthetic unit rows."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from mlflow.tracking import MlflowClient

from ml.demo_features import DEMO_FEATURE_NAMES
from ml.demo_pipeline import build_demo, run_demo, train_demo
from serving.demo_app import load_demo_model


def training_rows() -> list[dict[str, object]]:
    rows = []
    for scoring_date in (date(2024, 2, 1), date(2024, 9, 1)):
        for number in range(20):
            rows.append(
                dict(
                    zip(
                        DEMO_FEATURE_NAMES,
                        (
                            2,
                            number,
                            0,
                            number % 3,
                            number / 2,
                            None if number == 0 else 0.0,
                            5,
                        ),
                        strict=True,
                    )
                )
                | {
                    "usdot_number": str(number + 1),
                    "scoring_date": scoring_date,
                    "label": int(number >= 10),
                }
            )
    return rows


def test_registered_demo_scores_and_never_uses_production_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "source-provenance.json").write_text(
        json.dumps(
            {
                "data_as_of": "2026-09-03",
                "parquet_sha256": {"test": "unit-fixture"},
            }
        )
    )
    connection = MagicMock()
    connection.__enter__.return_value.execute.return_value.fetchall.return_value = (
        training_rows()
    )
    monkeypatch.setattr("ml.demo_pipeline.psycopg.connect", lambda *a, **k: connection)
    monkeypatch.setattr("ml.demo_pipeline.DEMO_MODEL_NAME", "unit-learning-demo")
    monkeypatch.setattr("serving.demo_app.DEMO_MODEL_NAME", "unit-learning-demo")
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    result = train_demo("unused", uri, tmp_path)
    assert result["metrics"]["training_rows"] == 20
    assert result["metrics"]["heldout_rows"] == 20
    assert result["tags"]["experimental"] == "true"
    assert result["training_complete_before_test_scoring"] is False
    model = load_demo_model()
    assert model.version == result["model_version"]
    client = MlflowClient(tracking_uri=uri)
    with pytest.raises(Exception, match="champion"):
        client.get_model_version_by_alias("unit-learning-demo", "champion")
    assert (tmp_path / "training-evidence.json").exists()


def test_wrong_snapshot_is_rejected_before_reading_rows(tmp_path: Path) -> None:
    (tmp_path / "source-provenance.json").write_text('{"data_as_of": "2026-01-01"}')
    with pytest.raises(ValueError, match="frozen experiment"):
        train_demo("unused", "unused", tmp_path)


def test_missing_positive_class_stops_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "source-provenance.json").write_text('{"data_as_of": "2026-09-03"}')
    rows = training_rows()
    for row in rows:
        row["label"] = 0
    connection = MagicMock()
    connection.__enter__.return_value.execute.return_value.fetchall.return_value = rows
    monkeypatch.setattr("ml.demo_pipeline.psycopg.connect", lambda *a, **k: connection)
    with pytest.raises(ValueError, match="both classes"):
        train_demo("unused", "unused", tmp_path)


@pytest.mark.parametrize("missing_feature", ["violations_4m", "crashes_24m"])
def test_missing_required_feature_stops_before_model_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_feature: str,
) -> None:
    (tmp_path / "source-provenance.json").write_text('{"data_as_of": "2026-09-03"}')
    rows = training_rows()
    rows[0][missing_feature] = None
    connection = MagicMock()
    connection.__enter__.return_value.execute.return_value.fetchall.return_value = rows
    monkeypatch.setattr("ml.demo_pipeline.psycopg.connect", lambda *a, **k: connection)
    estimator = MagicMock()
    estimator.fit.side_effect = AssertionError("Invalid features reached model fitting")
    monkeypatch.setattr(
        "sklearn.ensemble.GradientBoostingClassifier", lambda **k: estimator
    )
    with pytest.raises(ValueError, match=missing_feature):
        train_demo("unused", "unused", tmp_path)


@pytest.mark.parametrize("action", ["extract", "build", "train", "invalid"])
def test_manual_steps_release_serialization_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    connection = MagicMock()
    monkeypatch.setattr("ml.demo_pipeline.psycopg.connect", lambda *a, **k: connection)
    monkeypatch.setenv("CARRIER_RISK_DATABASE_URL", "unit-dsn")
    monkeypatch.setenv("CARRIER_RISK_DEMO_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("CARRIER_RISK_DEMO_DERIVED_ROOT", str(tmp_path))
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "unit-uri")
    monkeypatch.setattr(
        "ml.demo_pipeline.extract_demo_sources", lambda *a, **k: {"status": "extracted"}
    )
    monkeypatch.setattr("ml.demo_pipeline.build_demo", lambda *a: None)
    monkeypatch.setattr("ml.demo_pipeline.train_demo", lambda *a: {"status": "trained"})
    if action == "invalid":
        with pytest.raises(ValueError, match="Demo action"):
            run_demo(action)
    else:
        assert run_demo(action)["status"] in {"extracted", "built", "trained"}
    assert "unlock" in connection.__enter__.return_value.execute.call_args.args[0]


def test_dbt_failure_propagates_instead_of_reporting_built(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CARRIER_RISK_DBT_PROFILES_DIR", str(tmp_path))
    runner = MagicMock()
    runner.run.side_effect = RuntimeError("contract failed")
    monkeypatch.setattr("ml.demo_pipeline.DbtRunner", lambda **k: runner)
    with pytest.raises(RuntimeError, match="contract failed"):
        build_demo("unit-dsn", tmp_path)
