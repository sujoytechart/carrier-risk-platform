"""The safe dbt launcher must be discoverable without credentials."""

import subprocess
import sys


def test_dbt_launcher_help_explains_command_and_connection() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "orchestration.dbt_cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "build" in result.stdout
    assert "seed" in result.stdout
    assert "CARRIER_RISK_DATABASE_URL" in result.stdout


def test_snowflake_requires_its_own_configuration() -> None:
    import os

    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(
            ("SNOWFLAKE_", "DBT_ENV_SECRET_SNOWFLAKE_", "CARRIER_RISK_DATABASE_URL")
        )
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "orchestration.dbt_cli",
            "--warehouse-target",
            "snowflake",
            "seed",
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "SNOWFLAKE_ACCOUNT" in result.stderr
    assert "Set CARRIER_RISK_DATABASE_URL" not in result.stderr


def test_snowflake_launcher_rejects_conflicting_dbt_target() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "orchestration.dbt_cli",
            "--warehouse-target",
            "snowflake",
            "build",
            "--target",
            "local",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "controls the dbt target" in result.stderr


def test_snowflake_main_passes_an_explicit_target_and_guard(monkeypatch) -> None:
    """A cloud selection must not accidentally use the PostgreSQL guard."""
    from unittest.mock import MagicMock

    from orchestration import dbt_cli
    from orchestration.snowflake_lock import SnowflakeBuildLock

    values = {
        "SNOWFLAKE_ACCOUNT": "fixture-account",
        "SNOWFLAKE_USER": "FIXTURE_USER",
        "SNOWFLAKE_ROLE": "FIXTURE_ROLE",
        "SNOWFLAKE_DATABASE": "FIXTURE_DB",
        "SNOWFLAKE_WAREHOUSE": "FIXTURE_WH",
        "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PATH": "/private/fixture/key.p8",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("CARRIER_RISK_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["dbt_cli", "--warehouse-target", "snowflake", "seed"]
    )
    runner_factory = MagicMock()
    monkeypatch.setattr(dbt_cli, "DbtRunner", runner_factory)

    dbt_cli.main()

    assert isinstance(runner_factory.call_args.kwargs["build_lock"], SnowflakeBuildLock)
    runner_factory.return_value.run.assert_called_once_with(
        "seed", ["--target", "snowflake"]
    )


def test_snowflake_connector_uses_keypair_and_bounded_sessions(monkeypatch) -> None:
    """Use key-pair authentication and bounded sessions."""
    from unittest.mock import MagicMock

    import snowflake.connector

    from orchestration.dbt_cli import _connect_snowflake

    values = {
        "SNOWFLAKE_ACCOUNT": "fixture-account",
        "SNOWFLAKE_USER": "FIXTURE_USER",
        "SNOWFLAKE_ROLE": "FIXTURE_ROLE",
        "SNOWFLAKE_DATABASE": "FIXTURE_DB",
        "SNOWFLAKE_WAREHOUSE": "FIXTURE_WH",
        "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PATH": "/private/fixture/key.p8",
        "DBT_ENV_SECRET_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "fixture-passphrase",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    connect = MagicMock()
    monkeypatch.setattr(snowflake.connector, "connect", connect)

    result = _connect_snowflake()

    assert result is connect.return_value
    settings = connect.call_args.kwargs
    assert settings["authenticator"] == "SNOWFLAKE_JWT"
    assert settings["database"] == "FIXTURE_DB"
    assert settings["private_key_file"] == "/private/fixture/key.p8"
    assert settings["private_key_file_pwd"] == "fixture-passphrase"
    assert settings["client_session_keep_alive"] is False
    assert settings["network_timeout"] == 15
    assert settings["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 60
    assert settings["session_parameters"]["ABORT_DETACHED_QUERY"] is True
