"""Independent dbt evidence for the Python-owned maturity registry."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ml.maturity import read_watermark
from ml.watermark_store import PostgresWatermarkStore
from tests import dbt_support

DATABASE_NAME = "carrier_risk_maturity_test"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def isolated_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[psycopg.Connection[tuple[object, ...]], Path]]:
    """Use a dedicated database so another suite's modeled schema is untouched."""
    source_dsn = dbt_support.POSTGRES_DSN
    with psycopg.connect(source_dsn, autocommit=True) as admin:
        if (
            admin.execute(
                "select 1 from pg_database where datname = %s", (DATABASE_NAME,)
            ).fetchone()
            is None
        ):
            admin.execute(
                sql.SQL("create database {}").format(sql.Identifier(DATABASE_NAME))
            )
    isolated_dsn = make_conninfo(source_dsn, dbname=DATABASE_NAME)
    monkeypatch.setattr(dbt_support, "POSTGRES_DSN", isolated_dsn)
    monkeypatch.setenv("CARRIER_RISK_TEST_DATABASE_URL", isolated_dsn)
    monkeypatch.setenv("CARRIER_RISK_DATABASE_URL", isolated_dsn)
    dbt_support.write_test_profile(tmp_path, threads=1)
    with psycopg.connect(isolated_dsn, autocommit=True) as connection:
        connection.execute("select pg_advisory_lock(764301236::bigint)")
        connection.execute("drop schema if exists modeled cascade")
        try:
            yield connection, tmp_path
        finally:
            connection.execute("drop schema if exists modeled cascade")
            connection.execute("select pg_advisory_unlock(764301236::bigint)")


def _test_registry(profile: Path, *, should_pass: bool) -> None:
    result = dbt_support.execute_dbt(
        profile, "test", "--select", "label_maturity_policy"
    )
    artifact = profile / "target" / "run_results.json"
    assert artifact.exists(), result.stdout + result.stderr
    results = [
        item
        for item in json.loads(artifact.read_text())["results"]
        if item["unique_id"].startswith("test.")
    ]
    assert len(results) == 1, result.stdout + result.stderr
    assert results[0]["unique_id"].endswith(".label_maturity_policy")
    if should_pass:
        assert result.returncode == 0, result.stdout + result.stderr
        assert results[0]["status"] == "pass"
        assert results[0]["failures"] == 0
    else:
        (profile / "negative-run-results.json").write_text(artifact.read_text())
        assert result.returncode != 0, result.stdout + result.stderr
        assert results[0]["status"] == "fail", result.stdout + result.stderr
        assert results[0]["failures"] > 0


def test_registry_guard_accepts_absence_and_large_grace_but_detects_summary_drift(
    isolated_registry: tuple[psycopg.Connection[tuple[object, ...]], Path],
) -> None:
    connection, profile = isolated_registry
    _test_registry(profile, should_pass=True)
    measured = read_watermark(
        ROOT / "docs/evidence/phase-3/maturity-september-3-source-proxy.json"
    )
    assert measured.grace_days == 495
    PostgresWatermarkStore(connection).persist(measured)
    _test_registry(profile, should_pass=True)

    # The SQL shape constraints allow this positive grace. The independent dbt
    # guard must catch its disagreement with the immutable JSON measurement.
    connection.execute(
        "update modeled.label_maturity_watermarks set grace_days = grace_days - 1"
    )
    _test_registry(profile, should_pass=False)
    connection.execute(
        "update modeled.label_maturity_watermarks set grace_days = %s",
        (measured.grace_days,),
    )
    _test_registry(profile, should_pass=True)
    assert PostgresWatermarkStore(connection).latest() == measured


@pytest.mark.parametrize(
    ("adapter_type", "execute"), [("snowflake", True), ("postgres", False)]
)
def test_guard_does_not_probe_registry_or_emit_postgres_json_outside_pg_execution(
    adapter_type: str, execute: bool
) -> None:
    from types import SimpleNamespace

    from jinja2 import Environment, StrictUndefined

    def unexpected_lookup(**kwargs: object) -> None:
        raise AssertionError("Non-Postgres execution must not inspect the registry")

    template = Environment(undefined=StrictUndefined).from_string(
        (ROOT / "dbt/tests/label_maturity_policy.sql").read_text()
    )
    rendered = template.render(
        config=lambda **kwargs: "",
        execute=execute,
        target=SimpleNamespace(type=adapter_type, database="isolated_target"),
        adapter=SimpleNamespace(get_relation=unexpected_lookup),
        dbt=SimpleNamespace(type_string=lambda: "varchar"),
    )
    assert "where false" in rendered
    assert "jsonb" not in rendered
    assert "->" not in rendered
    assert "label_maturity_watermarks" not in rendered


def test_nonempty_registry_requires_exactly_one_current_policy(
    isolated_registry: tuple[psycopg.Connection[tuple[object, ...]], Path],
) -> None:
    from types import SimpleNamespace

    from jinja2 import Environment, StrictUndefined

    connection, _ = isolated_registry
    measured = read_watermark(
        ROOT / "docs/evidence/phase-3/maturity-september-3-source-proxy.json"
    )
    PostgresWatermarkStore(connection).persist(measured)

    def registry_relation(**kwargs: object) -> str:
        assert kwargs == {
            "database": DATABASE_NAME,
            "schema": "modeled",
            "identifier": "label_maturity_watermarks",
        }
        return "modeled.label_maturity_watermarks"

    rendered = (
        Environment(undefined=StrictUndefined)
        .from_string((ROOT / "dbt/tests/label_maturity_policy.sql").read_text())
        .render(
            config=lambda **kwargs: "",
            execute=True,
            target=SimpleNamespace(type="postgres", database=DATABASE_NAME),
            adapter=SimpleNamespace(get_relation=registry_relation),
            dbt=SimpleNamespace(type_string=lambda: "text"),
        )
    )
    assert connection.execute(rendered).fetchall() == []
    connection.execute(
        "update modeled.label_maturity_watermarks set is_current = false"
    )
    assert connection.execute(rendered).fetchall() == [(measured.watermark_version,)]
    connection.execute("update modeled.label_maturity_watermarks set is_current = true")
    assert connection.execute(rendered).fetchall() == []
