"""Render the real dbt guard against Snowflake-shaped SHOW LOCKS results."""

import re
from pathlib import Path
from types import SimpleNamespace

import agate
import jinja2
import pytest


class CompilerFailure(Exception):
    """A dbt startup refusal surfaced by the rendering boundary."""


def render_guard(owner: str, rows: list[tuple[object, ...]]) -> str:
    def fail(message: str) -> None:
        raise CompilerFailure(message)

    table = agate.Table(
        rows,
        ["resource", "transaction", "status"],
        [agate.Text(), agate.Number(), agate.Text()],
    )
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parents[1] / "dbt" / "macros"),
        extensions=["jinja2.ext.do"],
    )
    environment.globals.update(
        execute=True,
        flags=SimpleNamespace(WHICH="seed"),
        target=SimpleNamespace(type="snowflake", database="FIXTURE_DB"),
        modules=SimpleNamespace(re=re),
        env_var=lambda name, default="": owner,
        exceptions=SimpleNamespace(raise_compiler_error=fail),
        run_query=lambda sql: table,
    )
    return str(
        environment.get_template("assert_build_lock.sql").module.assert_build_lock()
    )


def test_exact_snowflake_lock_accepts_full_precision_transaction_identifier() -> None:
    rendered = render_guard(
        "1234567890123456789",
        [
            (
                "FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD",
                1234567890123456789,
                "HOLDING",
            ),
        ],
    )
    assert rendered.strip() == "select 1;"


@pytest.mark.parametrize(
    "owner, rows",
    [
        ("", []),
        ("1234567890123456789", []),
        (
            "1234567890123456789",
            [
                (
                    "OTHER_DB.CARRIER_RISK_CONTROL.BUILD_GUARD",
                    1234567890123456789,
                    "HOLDING",
                )
            ],
        ),
        (
            "1234567890123456789",
            [
                (
                    "FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD",
                    1234567890123456790,
                    "HOLDING",
                )
            ],
        ),
        (
            "1234567890123456789",
            [
                (
                    "FIXTURE_DB.CARRIER_RISK_CONTROL.BUILD_GUARD",
                    1234567890123456789,
                    "WAITING",
                )
            ],
        ),
    ],
)
def test_direct_or_unowned_snowflake_command_is_rejected(
    owner: str, rows: list[tuple[object, ...]]
) -> None:
    with pytest.raises(CompilerFailure, match="build lock"):
        render_guard(owner, rows)
