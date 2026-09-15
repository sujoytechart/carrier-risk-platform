"""Manual, isolated real-data training with explicit snapshot ascertainment."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import mlflow
import mlflow.sklearn
import numpy as np
import psycopg
from mlflow.tracking import MlflowClient
from psycopg.rows import dict_row

from ml.demo_features import DEMO_FEATURE_NAMES, demo_feature_values
from ml.demo_source import extract_demo_sources
from ml.demo_training import (
    DEMO_MODEL_NAME,
    TEST_DATE,
    TRAIN_DATE,
    evaluate_demo,
    select_threshold,
)
from ml.train import MODEL_PARAMETERS, positive_probabilities
from orchestration.dbt_lock import PostgresBuildLock
from orchestration.dbt_runner import DbtRunner


def build_demo(database_url: str, output_dir: Path) -> None:
    """Build only the explicit demo models under the existing publication lock."""
    runner = DbtRunner(
        project_dir=Path(os.getenv("CARRIER_RISK_PROJECT_DIR", ".")),
        profiles_dir=Path(os.environ["CARRIER_RISK_DBT_PROFILES_DIR"]),
        build_lock=PostgresBuildLock(
            lambda: psycopg.connect(database_url, autocommit=True)
        ),
    )
    runner.run(
        "build",
        [
            "--select",
            "demo_training_features",
            "demo_inspection_identity",
            "--vars",
            '{"learning_demo": true}',
            "--target-path",
            str(output_dir / "dbt-target"),
        ],
    )


def train_demo(
    database_url: str, tracking_uri: str, output_dir: Path
) -> dict[str, Any]:
    """Fit once, evaluate later outcomes and register a separate demo version.

    Registration proves the platform path and does not assert v0 promotion or
    eventual label maturity. All identifiers and individual vectors stay local.
    """
    provenance = json.loads((output_dir / "source-provenance.json").read_text())
    if provenance["data_as_of"] != "2026-09-03":
        raise ValueError("The frozen experiment requires the September 3 snapshot")
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        rows = connection.execute("""
            select * from learning_demo.demo_training_features
            where scoring_date in (date '2024-02-01', date '2024-09-01')
            order by scoring_date, usdot_number
        """).fetchall()
    partitions = [
        [row for row in rows if str(row["scoring_date"]) == scoring_date]
        for scoring_date in (TRAIN_DATE, TEST_DATE)
    ]
    for partition in partitions:
        if {row["label"] for row in partition} != {0, 1}:
            raise ValueError("Each experiment cohort requires both classes")
    values = [
        np.asarray(
            [demo_feature_values(row) for row in partition],
            dtype=np.float64,
        )
        for partition in partitions
    ]
    labels = [
        np.asarray([row["label"] for row in part], dtype=np.int64)
        for part in partitions
    ]
    ensemble: Any = importlib.import_module("sklearn.ensemble")
    classifier = ensemble.GradientBoostingClassifier(**MODEL_PARAMETERS)
    classifier.fit(values[0], labels[0])
    threshold = select_threshold(
        labels[0], positive_probabilities(classifier, values[0])
    )
    metrics = evaluate_demo(
        labels[1],
        positive_probabilities(classifier, values[1]),
        values[1][:, DEMO_FEATURE_NAMES.index("crashes_24m")],
        threshold,
    )
    metrics["training_rows"] = float(len(partitions[0]))
    metrics["training_prevalence"] = float(labels[0].mean())
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, default=str).encode() + b"\n")
    tags = {
        "experimental": "true",
        "validation_fixture": "false",
        "feature_names": ",".join(DEMO_FEATURE_NAMES),
        "label_ascertainment": "retained_snapshot_not_eventual_completeness",
        "availability_quality": "source_proxy",
        "data_as_of": "2026-09-03",
        "evaluation": "retrospective_later_period_holdout",
        "dataset_sha256": digest.hexdigest(),
        "threshold": str(threshold),
    }
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("carrier-risk-learning-demo")
    with mlflow.start_run(tags=tags) as run:
        mlflow.log_params(
            {
                **MODEL_PARAMETERS,
                "inspection_months": 4,
                "crash_months": 24,
                "label_months": 6,
                "train_date": TRAIN_DATE,
                "test_date": TEST_DATE,
                "threshold_selection": "training_f1_only",
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.log_dict(provenance, "source-provenance.json")
        mlflow.sklearn.log_model(
            classifier,
            name="model",
            input_example=values[0][:2],
            registered_model_name=DEMO_MODEL_NAME,
        )
        client = MlflowClient(tracking_uri=tracking_uri)
        versions = [
            v
            for v in client.search_model_versions(f"name='{DEMO_MODEL_NAME}'")
            if v.run_id == run.info.run_id
        ]
        if len(versions) != 1:
            raise ValueError(
                "Expected exactly one registered demo version for this run"
            )
        version = versions[0]
        for name, value in tags.items():
            client.set_model_version_tag(DEMO_MODEL_NAME, version.version, name, value)
        client.set_registered_model_alias(DEMO_MODEL_NAME, "demo", version.version)
        evidence = {
            "run_id": run.info.run_id,
            "model_name": DEMO_MODEL_NAME,
            "model_version": str(version.version),
            "alias": "demo",
            "metrics": metrics,
            "provenance": provenance,
            "tags": tags,
            "baseline_beaten": metrics["candidate_average_precision"]
            > metrics["baseline_average_precision"],
            "training_complete_before_test_scoring": False,
        }
    (output_dir / "training-evidence.json").write_text(
        json.dumps(evidence, indent=2) + "\n"
    )
    return evidence


def run_demo(action: str) -> dict[str, Any]:
    """Run a manual demo step from discoverable environment configuration."""
    database_url = os.environ["CARRIER_RISK_DATABASE_URL"]
    output_dir = Path(os.environ["CARRIER_RISK_DEMO_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)
    # Separate from the canonical training and dbt publication locks.
    with psycopg.connect(database_url, autocommit=True) as guard:
        guard.execute("select pg_advisory_lock(764301236)")
        try:
            if action == "extract":
                evidence = extract_demo_sources(
                    Path(os.environ["CARRIER_RISK_DEMO_DERIVED_ROOT"]),
                    database_url,
                    data_as_of=date(2026, 9, 3),
                )
                (output_dir / "source-provenance.json").write_text(
                    json.dumps(evidence, indent=2)
                )
                return evidence
            if action == "build":
                build_demo(database_url, output_dir)
                return {"status": "built", "experimental": True}
            if action == "train":
                return train_demo(
                    database_url, os.environ["MLFLOW_TRACKING_URI"], output_dir
                )
            raise ValueError("Demo action must be extract, build or train")
        finally:
            guard.execute("select pg_advisory_unlock(764301236)")


def main() -> None:
    """Execute an explicit experiment without altering the production gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("extract", "build", "train"))
    args = parser.parse_args()
    evidence = run_demo(args.action)
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
