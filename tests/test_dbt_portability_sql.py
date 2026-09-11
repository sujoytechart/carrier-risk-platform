"""Offline encoding checks; live warehouse regressions live in dbt/tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from jinja2 import Environment, StrictUndefined

MACRO_PATH = Path(__file__).resolve().parents[1] / "dbt/macros/portable_sql.sql"


@pytest.mark.parametrize(
    "value",
    [
        "MI",
        'quote " and backslash \\ and slash /',
        "Unicode café 雪",
        "".join(chr(code) for code in range(1, 32)),
        "a,b|c\x1fd",
        "",
    ],
)
def test_source_identity_json_scalar_matches_postgres_encoding(value: str) -> None:
    """Execute the shared string operations against an independent JSON oracle.

    SQLite provides the same replace/concatenation operations used in this one
    Snowflake expression. This proves escaping, not Snowflake SQL execution.
    PostgreSQL text cannot contain NUL, so it is outside cross-target identity.
    """
    environment = Environment(undefined=StrictUndefined)
    module = environment.from_string(MACRO_PATH.read_text()).make_module()
    expression = module.snowflake__portable_json_string("source_value")
    with sqlite3.connect(":memory:") as connection:
        connection.create_function("chr", 1, chr)
        result = connection.execute(
            f"select {expression} from (select ? as source_value)", (value,)
        ).fetchone()
    assert result == (json.dumps(value, ensure_ascii=False),)


@pytest.mark.parametrize("target", ["postgres", "snowflake"])
def test_models_compile_offline_with_enforced_contracts(
    tmp_path: Path, target: str
) -> None:
    """Compile both adapters without introspection, credentials, or a warehouse."""
    import subprocess
    import sys

    project_root = MACRO_PATH.parents[2]
    (tmp_path / "profiles.yml").write_text(
        """carrier_risk_platform:
  target: postgres
  outputs:
    postgres:
      type: postgres
      host: localhost
      port: 5432
      user: offline
      password: offline
      dbname: offline
      schema: public
      threads: 1
    snowflake:
      type: snowflake
      account: offline
      user: offline
      password: offline
      database: offline
      warehouse: offline
      schema: public
      threads: 1
"""
    )
    result = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "compile",
            "--no-introspect",
            "--no-populate-cache",
            "--no-partial-parse",
            "--profiles-dir",
            str(tmp_path),
            "--target",
            target,
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
            "--vars",
            "{load_test_fixtures: true}",
            "--select",
            "path:dbt/models/clean",
            "path:dbt/models/modeled",
            "portable_scalar_regressions",
            "tag:temporal",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads((tmp_path / "target/manifest.json").read_text())
    nodes = manifest["nodes"]
    modeled = [
        node
        for node in nodes.values()
        if node["resource_type"] == "model" and node["schema"] == "modeled"
    ]
    assert {node["name"] for node in modeled} == {
        "event_versions",
        "inspections",
        "crashes",
        "crash_incidents",
        "events_union",
        "current_events",
        "training_features",
    }
    assert all(node["config"]["contract"]["enforced"] for node in modeled)
    assert all(node["compiled_code"].strip() for node in modeled)
    features = nodes["model.carrier_risk_platform.training_features"]
    for ratio in ("violations_per_inspection", "oos_violation_rate"):
        assert features["columns"][ratio]["data_type"] == "decimal(38,6)"
    expected_timestamp = "timestamp_tz" if target == "snowflake" else "timestamptz"
    for node in modeled:
        for column in ("knowledge_valid_from", "knowledge_valid_to"):
            if column in node["columns"]:
                assert node["columns"][column]["data_type"] == expected_timestamp
    temporal_tests = [node for node in nodes.values() if "temporal" in node["tags"]]
    assert len(temporal_tests) == 3
    assert all(node["config"]["severity"] == "error" for node in temporal_tests)
    assert all(node["compiled_code"].strip() for node in temporal_tests)
    seed = nodes["seed.carrier_risk_platform.raw_snapshot_batches"]
    assert seed["config"]["column_types"]["observed_at"] == expected_timestamp


@pytest.mark.parametrize(
    "state,report_number",
    [
        ("MI", "A42"),
        ("a|b", "c"),
        ("a", "b|c"),
        ('a"b', "control\n\t\\雪"),
    ],
)
def test_complete_fallback_identity_matches_existing_json_bytes(
    state: str, report_number: str
) -> None:
    """Check encoding and delimiters end to end using synthetic scalar inputs."""
    module = (
        Environment(undefined=StrictUndefined)
        .from_string(MACRO_PATH.read_text())
        .make_module()
    )
    expression = module.snowflake__portable_crash_source_key(
        "state", "report_number", "event_date", "report_time", "sequence"
    )
    with sqlite3.connect(":memory:") as connection:
        connection.create_function("chr", 1, chr)
        connection.create_function("to_char", 2, lambda value, _: value)
        connection.create_function("hex_encode", 1, lambda value: value.encode().hex())
        result = connection.execute(
            f"select {expression} from (select ? as state, ? as report_number, "
            "'2024-02-29' as event_date, 930 as report_time, 2 as sequence)",
            (state, report_number),
        ).fetchone()
    encoded = json.dumps(
        [state, report_number, "2024-02-29", 930, 2], ensure_ascii=False
    ).encode()
    assert result == ("fallback:" + encoded.hex(),)


@pytest.mark.parametrize(
    "numerator,denominator,expected",
    [
        (1, 2_000_001, "0.000000"),
        (1, 2_000_000, "0.000001"),
        (1, 1_999_999, "0.000001"),
        (3, 2_000_001, "0.000001"),
        (3, 2_000_000, "0.000002"),
        (3, 1_999_999, "0.000002"),
        (1, 3, "0.333333"),
        (2, 3, "0.666667"),
        (2_000_002, 2_000_001, "1.000000"),
        (0, 3, "0.000000"),
        (1, 0, None),
        (None, 3, None),
        (1, None, None),
    ],
)
def test_count_ratio_rounds_exactly_at_six_places(
    numerator: int | None, denominator: int | None, expected: str | None
) -> None:
    """Execute rendered integer arithmetic at and around halfway boundaries.

    SQLite executes the quotient/remainder formula on these bounded integers;
    this is an offline arithmetic proof, not a Snowflake numeric-type emulator.
    The final decimal representation is checked against explicit expected values
    and an independent high-precision Decimal division.
    """
    from decimal import ROUND_HALF_UP, Decimal, localcontext

    module = (
        Environment(undefined=StrictUndefined)
        .from_string(MACRO_PATH.read_text())
        .make_module()
    )
    expression = module.portable_ratio("numerator", "denominator")
    with sqlite3.connect(":memory:") as connection:
        connection.create_function(
            "mod", 2, lambda n, d: None if n is None or d is None else n % d
        )
        row = connection.execute(
            f"select {expression} from (select ? numerator, ? denominator)",
            (numerator, denominator),
        ).fetchone()
    assert row is not None
    if expected is None:
        assert row[0] is None
    else:
        assert Decimal(str(row[0])) == Decimal(expected)
        with localcontext() as context:
            context.prec = 60
            reference = (Decimal(numerator) / Decimal(denominator)).quantize(
                Decimal("0.000001"), rounding=ROUND_HALF_UP
            )
        assert reference == Decimal(expected)
