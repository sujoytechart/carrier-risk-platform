"""Render production Snowflake SQL without claiming warehouse execution."""

from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]


def render_macro(name: str, *arguments: str) -> str:
    """Load the actual SQL macros; only dbt's context is supplied locally."""
    sources = list((ROOT / "dbt/macros/snowflake").glob("*.sql"))
    assert sources, "Snowflake history implementation is missing"
    source = "\n".join(path.read_text() for path in sorted(sources))
    module = (
        Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"])
        .from_string(source)
        .make_module(
            {"model": SimpleNamespace(columns={}), "return": lambda value: value}
        )
    )
    return str(getattr(module, name)(*arguments))


def test_publication_has_explicit_rollback_and_no_transactional_ddl() -> None:
    sql = render_macro(
        "snowflake_publish_candidates",
        "DB.I.CANDIDATES",
        "DB.I.BATCHES",
        "DB.I.STAGED_CANDIDATES",
        "DB.I.STAGED_BATCHES",
    ).lower()
    assert "begin transaction;" in sql
    transaction = sql.split("begin transaction;", 1)[1]
    assert "rollback;" in transaction and "raise;" in transaction
    assert not any(
        word in transaction for word in ("create ", "drop ", "alter ", "truncate ")
    )
    assert "delete from db.i.candidates" in transaction
    assert "delete from db.i.batches" in transaction
    assert "candidate_count" in transaction and "row_count" in transaction
    assert "observed_at" in transaction and "having count(*) > 1" in transaction


def test_history_applies_ordered_batches_in_one_dml_transaction() -> None:
    sql = render_macro(
        "snowflake_apply_history",
        "DB.M.HISTORY",
        "DB.M.APPLIED",
        "DB.M.EXPIRIES",
        "DB.M.INPUT",
        "DB.I.BATCHES",
        "DB.M.PRIOR",
    ).lower()
    assert "order by observed_at, batch_id" in sql
    transaction = sql.split("begin transaction;", 1)[1]
    assert not any(
        word in transaction for word in ("create ", "drop ", "alter ", "truncate ")
    )
    assert "rollback;" in transaction and "raise;" in transaction
    assert transaction.index("end for;") < transaction.index("commit;")
    loop = transaction.split("for pending_batch in pending_batches do", 1)[1]
    assert "into :feed_has_history" in loop
    assert "applied.observed_at > :batch_observed_at" in loop
    assert "insert into db.m.applied" in loop
    assert "insert into db.m.expiries" in loop
    assert "knowledge_valid_to = :batch_observed_at" in loop
    assert "superseded_by_version_key = successor.event_version_key" in loop


def test_transition_keeps_source_proxy_exclusion_retention_and_utc_semantics() -> None:
    sql = render_macro(
        "snowflake_apply_history_batch",
        "DB.M.HISTORY",
        "DB.M.APPLIED",
        "DB.M.EXPIRIES",
        "DB.M.INPUT",
        "DB.M.PRIOR",
    ).lower()
    assert "when :feed_has_history then :batch_observed_at" in sql
    assert "to_char(candidates.source_proxy_valid_from" in sql
    assert "to_char(candidates.source_add_at" in sql
    assert "to_char(candidates.source_change_at" in sql
    assert (
        "candidates.event_date > convert_timezone('utc', :batch_observed_at)::date"
        in sql
    )
    assert "prior.event_date < :minimum_inspection_date" in sql
    assert "prior.event_date >= :minimum_inspection_date" in sql
    assert "prior.record_hash <> candidates.record_hash" in sql
    assert "last_seen_batch_id = :batch_id" in sql
    assert sql.index("insert into db.m.history") < sql.index("with candidate_versions")


def test_candidate_validation_rejects_ambiguous_or_incomplete_publications() -> None:
    """Execute the actual validation SELECT using SQLite's shared SQL subset."""
    import sqlite3

    query = render_macro("snowflake_validate_candidates", "candidates", "batches")
    query = query.split(";", 1)[0].replace("into :violations", "")
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "create table batches(batch_id, feed_name, observed_at, row_count)"
        )
        connection.execute(
            "create table candidates(batch_id, feed_name, observed_at, record_hash, "
            "is_model_eligible, event_type, source_record_key)"
        )
        connection.execute(
            "insert into batches values ('b', 'crashes', '2026-01-01', 0)"
        )
        assert connection.execute(query).fetchone() == (0,)
        connection.execute(
            "insert into candidates values "
            "('b', 'crashes', '2026-01-01', 'hash', true, 'crash', 'source')"
        )
        assert connection.execute(query).fetchone()[0] > 0
        connection.execute("update batches set row_count = 1")
        assert connection.execute(query).fetchone() == (0,)
        for assignment in (
            "observed_at = '2026-01-02'",
            "feed_name = 'inspections'",
            "event_type = 'inspection'",
            "record_hash = null",
            "batch_id = 'unknown'",
            "is_model_eligible = null",
        ):
            connection.execute("savepoint valid_publication")
            connection.execute(f"update candidates set {assignment}")
            assert connection.execute(query).fetchone()[0] > 0, assignment
            connection.execute("rollback to valid_publication")
            connection.execute("release valid_publication")
        connection.execute("insert into candidates select * from candidates")
        connection.execute("update batches set row_count = 2")
        assert connection.execute(query).fetchone()[0] > 0
        connection.execute("delete from candidates")
        connection.execute("update batches set row_count = 0")
        connection.execute("insert into batches select * from batches")
        assert connection.execute(query).fetchone()[0] > 0


def test_schema_type_checks_reject_narrowing_scale_and_timezone_drift() -> None:
    """Execute the catalog predicates against independent metadata examples."""
    import sqlite3

    examples = [
        ("bigint", "NUMBER", None, 38, 0, None, True),
        ("integer", "NUMBER", None, 38, 0, None, True),
        ("bigint", "NUMBER", None, 37, 0, None, False),
        ("integer", "NUMBER", None, 38, 1, None, False),
        ("integer", "FLOAT", None, None, None, None, False),
        ("text", "TEXT", 16777216, None, None, None, True),
        ("text", "TEXT", 134217728, None, None, None, True),
        ("text", "TEXT", 2, None, None, None, False),
        ("timestamp_tz", "TIMESTAMP_TZ", None, None, None, 9, True),
        ("timestamp_tz", "TIMESTAMP_NTZ", None, None, None, 9, False),
    ]
    with sqlite3.connect(":memory:") as connection:
        for (
            expected,
            actual,
            width,
            precision,
            scale,
            time_precision,
            matches,
        ) in examples:
            predicate = render_macro("snowflake_type_matches", expected)
            row = connection.execute(
                f"select {predicate} from (select ? data_type, "
                "? character_maximum_length, ? numeric_precision, "
                "? numeric_scale, ? datetime_precision)",
                (actual, width, precision, scale, time_precision),
            ).fetchone()
            assert row == (int(matches),), (expected, actual, width, precision, scale)


def test_snowflake_adapter_preserves_scripting_transaction_as_one_statement() -> None:
    from dbt.adapters.snowflake.connections import SnowflakeConnectionManager

    sql = render_macro(
        "snowflake_apply_history",
        "DB.M.HISTORY",
        "DB.M.APPLIED",
        "DB.M.EXPIRIES",
        "DB.M.INPUT",
        "DB.I.BATCHES",
        "DB.M.PRIOR",
    )
    statements = SnowflakeConnectionManager._split_queries(sql)
    assert len(statements) == 1
    assert "exception" in statements[0] and "rollback;" in statements[0]


def test_all_models_compile_including_atomic_candidate_materialization(
    tmp_path: Path,
) -> None:
    import json
    import subprocess
    import sys

    (tmp_path / "profiles.yml").write_text(
        "carrier_risk_platform:\n  target: snowflake\n  outputs:\n    snowflake:\n"
        "      type: snowflake\n      account: offline\n      user: offline\n"
        "      password: offline\n      database: offline\n      warehouse: offline\n"
        "      schema: public\n      threads: 1\n"
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
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
            "--vars",
            "{load_test_fixtures: true}",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = json.loads((tmp_path / "target/manifest.json").read_text())
    models = [
        node for node in manifest["nodes"].values() if node["resource_type"] == "model"
    ]
    assert all(node["compiled_code"].strip() for node in models)
    candidate = next(
        node for node in models if node["name"] == "event_change_candidates"
    )
    assert candidate["config"]["materialized"] == "snowflake_candidate_publication"
    assert candidate["config"]["post-hook"] == []
    assert "cardinality(" not in candidate["compiled_code"]
    assert "get(parse_reasons, 0)::varchar" in candidate["compiled_code"]


def test_materializations_render_persisted_schema_checks_before_transactions() -> None:
    """Render dbt materialization bodies with actual relation objects and schema."""
    import re

    import yaml
    from dbt.adapters.snowflake.relation import SnowflakeRelation

    history_model = yaml.safe_load(
        (ROOT / "dbt/models/modeled/schema.yml").read_text()
    )["models"][0]
    columns = {
        column["name"]: {
            "data_type": (
                "timestamp_tz"
                if "timestamp_tz" in column["data_type"]
                else column["data_type"]
            )
        }
        for column in history_model["columns"]
    }
    sources = [
        path.read_text()
        for path in sorted((ROOT / "dbt/macros/snowflake").glob("*.sql"))
    ]
    materializations = ROOT / "dbt/macros/materializations"
    for filename in (
        "snowflake_event_version_history.sql",
        "snowflake_candidate_publication.sql",
    ):
        body = (materializations / filename).read_text()
        body = re.sub(
            r"{% materialization (\w+), adapter='snowflake' %}",
            r"{% macro materialization_\1() %}",
            body,
        ).replace("{% endmaterialization %}", "{% endmacro %}")
        sources.append(body)
    statements: dict[str, str] = {}

    def statement(name: str, auto_begin: bool, caller: object) -> str:
        assert auto_begin is False
        assert callable(caller)
        statements[name] = caller()
        return ""

    relation = SnowflakeRelation.create(
        database="offline",
        schema="modeled",
        identifier="event_versions",
        quote_policy={"database": False, "schema": False, "identifier": False},
    )
    module = (
        Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"])
        .from_string("\n".join(sources))
        .make_module(
            {
                "model": SimpleNamespace(columns=columns),
                "execute": False,
                "return": lambda value: value,
                "this": relation,
                "adapter": SimpleNamespace(quote=lambda value: f'"{value}"'),
                "config": {"contract": SimpleNamespace(enforced=True)},
                "ref": lambda name: relation.incorporate(
                    path={"schema": "intermediate", "identifier": name}
                ),
                "sql": "select * from offline.intermediate.event_change_candidates",
                "statement": statement,
            }
        )
    )
    module.materialization_event_version_history()
    assert list(statements) == [
        "prepare_history",
        "history_contract",
        "stage_history",
        "main",
    ]
    assert '"OFFLINE".information_schema.columns' in statements["history_contract"]
    assert "table_name = 'EVENT_VERSIONS'" in statements["history_contract"]
    assert "is_identity = 'YES'" in statements["history_contract"]
    assert "source_record_key" in statements["prepare_history"]
    assert "autoincrement start 1 increment 1" in statements["prepare_history"]
    assert "begin transaction;" not in "".join(list(statements.values())[:-1])
    statements.clear()
    module.materialization_snowflake_candidate_publication()
    assert list(statements) == ["prepare_publication", "main"]
    assert "create table if not exists" in statements["prepare_publication"]
    assert "create or replace temporary table" in statements["prepare_publication"]
    assert "begin transaction;" not in statements["prepare_publication"]


def test_timestamp_precision_contract_uses_describe_not_inapplicable_metadata() -> None:
    import pytest

    class ContractFailure(Exception):
        """A persisted schema mismatch surfaced before mutation."""

    def fail(message: str) -> None:
        raise ContractFailure(message)

    relation = SimpleNamespace(
        database="offline",
        schema="modeled",
        identifier="event_versions",
        quote_policy=SimpleNamespace(database=False, schema=False, identifier=False),
    )
    source = (ROOT / "dbt/macros/snowflake/history_contract.sql").read_text()
    assert "datetime_precision =" not in source
    for actual_type in ("TIMESTAMP_TZ(9)", "TIMESTAMP_TZ(6)", "TIMESTAMP_NTZ(9)"):
        context = {
            "execute": True,
            "run_query": lambda sql, actual_type=actual_type: SimpleNamespace(
                rows=[{"name": "KNOWLEDGE_VALID_FROM", "type": actual_type}]
            ),
            "exceptions": SimpleNamespace(raise_compiler_error=fail),
            "adapter": SimpleNamespace(quote=lambda value: f'"{value}"'),
        }
        module = (
            Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"])
            .from_string(source)
            .make_module(context)
        )
        columns = {"knowledge_valid_from": {"data_type": "timestamp_tz"}}
        if actual_type == "TIMESTAMP_TZ(9)":
            rendered = module.snowflake_assert_relation_contract(relation, columns)
            assert "data_type = 'TIMESTAMP_TZ'" in rendered
        else:
            with pytest.raises(ContractFailure, match="requires TIMESTAMP_TZ"):
                module.snowflake_assert_relation_contract(relation, columns)
