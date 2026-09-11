"""Create an isolated, explicitly synthetic PostgreSQL/MLflow validation fixture.

This is a load-test substrate, not a retrospective training run or quality claim.
It accepts only a dedicated loopback database and the fixture registry name.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import psycopg
from mlflow.tracking import MlflowClient
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from ml.features import FEATURE_NAMES, FeatureRow
from ml.train import MODEL_PARAMETERS, MODEL_SPECIFICATION
from serving.model import FIXTURE_MODEL_NAME

FIXTURE_DATABASE = "carrier_risk_phase3_serving"


def fixture_rows() -> list[FeatureRow]:
    """Produce 512 deterministic fictional carriers on the current monthly grid."""
    scoring_date = datetime.now(UTC).date().replace(day=1)
    rows = []
    for number in range(1, 513):
        inspections = 1 + number % 12
        violations = number % 17
        oos = number % (violations + 1)
        rows.append(
            FeatureRow(
                str(number),
                scoring_date,
                inspections,
                violations,
                oos,
                number % 4,
                violations / inspections,
                oos / violations if violations else None,
                1 + number % 60,
            )
        )
    return rows


def prepare_fixture(directory: Path, database_url: str) -> dict[str, object]:
    """Populate the dedicated fixture DB and register a tagged synthetic model."""
    parameters = conninfo_to_dict(database_url)
    if parameters.get("dbname") != FIXTURE_DATABASE or parameters.get("host") not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        raise ValueError("Fixture setup requires its dedicated loopback database")
    admin_url = make_conninfo(database_url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True) as connection:
        exists = connection.execute(
            "select 1 from pg_database where datname = %s", (FIXTURE_DATABASE,)
        ).fetchone()
        if exists is None:
            connection.execute(
                sql.SQL("create database {}").format(sql.Identifier(FIXTURE_DATABASE))
            )
    rows = fixture_rows()
    with psycopg.connect(database_url) as connection:
        connection.execute("create schema if not exists modeled")
        connection.execute("""
            create table if not exists modeled.training_features (
                usdot_number text not null, scoring_date date not null,
                inspections_6m bigint not null, violations_6m bigint not null,
                oos_violations_6m bigint not null, crashes_24m bigint not null,
                violations_per_inspection double precision not null,
                oos_violation_rate double precision,
                days_since_last_inspection integer not null,
                primary key (usdot_number, scoring_date)
            )
        """)
        connection.execute("""
            create table if not exists modeled.inspections (
                usdot_number text primary key, event_date date not null,
                reported_date date not null, knowledge_valid_from timestamptz not null,
                knowledge_valid_to timestamptz, is_deleted boolean not null
            )
        """)
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                insert into modeled.training_features
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (usdot_number, scoring_date) do update set
                    inspections_6m=excluded.inspections_6m,
                    violations_6m=excluded.violations_6m,
                    oos_violations_6m=excluded.oos_violations_6m,
                    crashes_24m=excluded.crashes_24m,
                    violations_per_inspection=excluded.violations_per_inspection,
                    oos_violation_rate=excluded.oos_violation_rate,
                    days_since_last_inspection=excluded.days_since_last_inspection
            """,
                [
                    (
                        row.usdot_number,
                        row.scoring_date,
                        row.inspections_6m,
                        row.violations_6m,
                        row.oos_violations_6m,
                        row.crashes_24m,
                        row.violations_per_inspection,
                        row.oos_violation_rate,
                        row.days_since_last_inspection,
                    )
                    for row in rows
                ],
            )
            inspection_rows = []
            for row in rows:
                event_date = row.scoring_date - timedelta(
                    days=row.days_since_last_inspection
                )
                observed_at = datetime.combine(event_date, time.min, tzinfo=UTC)
                inspection_rows.append(
                    (row.usdot_number, event_date, event_date, observed_at)
                )
            cursor.executemany(
                """
                insert into modeled.inspections values (%s,%s,%s,%s,null,false)
                on conflict (usdot_number) do update set
                    event_date=excluded.event_date,
                    reported_date=excluded.reported_date,
                    knowledge_valid_from=excluded.knowledge_valid_from,
                    knowledge_valid_to=null, is_deleted=false
            """,
                inspection_rows,
            )
        connection.execute("analyze modeled.training_features")
        connection.execute("analyze modeled.inspections")
    directory.mkdir(parents=True, exist_ok=True)
    tracking_uri = f"sqlite:///{directory.resolve() / 'fixture-mlflow.db'}"
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_name = "synthetic-serving-validation"
    experiment = client.get_experiment_by_name(experiment_name)
    experiment_id = (
        experiment.experiment_id
        if experiment is not None
        else client.create_experiment(
            experiment_name,
            artifact_location=(directory.resolve() / "artifacts").as_uri(),
        )
    )
    matrix = np.asarray([row.numeric_values() for row in rows], dtype=np.float64)
    labels = np.asarray(
        [int((index % 7) < (row.crashes_24m + 1)) for index, row in enumerate(rows)]
    )
    sklearn = importlib.import_module("sklearn")
    ensemble = importlib.import_module("sklearn.ensemble")
    classifier = ensemble.GradientBoostingClassifier(**MODEL_PARAMETERS)
    classifier.fit(matrix, labels)
    with mlflow.start_run(
        experiment_id=experiment_id, tags={"validation_fixture": "true"}
    ) as run:
        mlflow.log_params(
            {"validation_fixture": True, "fixture_rows": len(rows), **MODEL_PARAMETERS}
        )
        mlflow.sklearn.log_model(
            classifier,
            name="classifier",
            registered_model_name=FIXTURE_MODEL_NAME,
            input_example=matrix[:1],
            pip_requirements=[f"scikit-learn=={sklearn.__version__}"],
        )
        versions = client.search_model_versions(f"name='{FIXTURE_MODEL_NAME}'")
        version = next(
            item.version for item in versions if item.run_id == run.info.run_id
        )
    client.set_model_version_tag(
        FIXTURE_MODEL_NAME, version, "validation_fixture", "true"
    )
    client.set_model_version_tag(
        FIXTURE_MODEL_NAME, version, "feature_names", ",".join(FEATURE_NAMES)
    )
    client.set_registered_model_alias(FIXTURE_MODEL_NAME, "champion", version)
    result: dict[str, object] = {
        "validation_fixture": True,
        "dataset": "512 fictional carriers; no federal data",
        "features_as_of": rows[0].scoring_date.isoformat(),
        "carriers": len(rows),
        "model_name": FIXTURE_MODEL_NAME,
        "model_version": version,
        "model_specification": MODEL_SPECIFICATION,
        "model_parameters": MODEL_PARAMETERS,
        "tracking_uri": tracking_uri,
    }
    (directory / "fixture.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    """Prepare local validation data without contacting any cloud service."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    arguments = parser.parse_args()
    result = prepare_fixture(
        arguments.output_dir, os.environ["CARRIER_RISK_DATABASE_URL"]
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
