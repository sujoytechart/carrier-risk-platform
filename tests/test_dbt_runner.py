from __future__ import annotations

import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from orchestration.dbt_runner import DbtCommandError, DbtRunner


class RecordingExecutor:
    def __init__(self, *, returncode: int = 0) -> None:
        self.returncode = returncode
        self.commands: list[tuple[str, ...]] = []

    def __call__(
        self,
        command: tuple[str, ...],
        *,
        cwd: Path,
        environment: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert cwd == Path("/workspace")
        assert environment["POSTGRES_PASSWORD"] == "secret"
        assert environment["CARRIER_RISK_DBT_LOCK_PID"] == "1234"
        self.commands.append(command)
        return subprocess.CompletedProcess(
            command,
            self.returncode,
            stdout="dbt output",
            stderr="dbt error",
        )


@contextmanager
def held_build_lock() -> Iterator[int]:
    """Stand in for the external database session in command-construction tests."""
    yield 1234


def test_build_passes_explicit_scoring_dates_without_using_a_shell() -> None:
    executor = RecordingExecutor()
    runner = DbtRunner(
        project_dir=Path("/workspace"),
        profiles_dir=Path("/profiles"),
        environment={"POSTGRES_PASSWORD": "secret"},
        executor=executor,
        build_lock=held_build_lock,
    )

    runner.build(scoring_dates=("2026-03-01", "2026-06-01"))

    assert executor.commands == [
        (
            "dbt",
            "build",
            "--project-dir",
            "/workspace",
            "--profiles-dir",
            "/profiles",
            "--vars",
            '{"scoring_dates": ["2026-03-01", "2026-06-01"]}',
        )
    ]


def test_failure_includes_dbt_diagnostics() -> None:
    runner = DbtRunner(
        project_dir=Path("/workspace"),
        profiles_dir=Path("/profiles"),
        environment={"POSTGRES_PASSWORD": "secret"},
        executor=RecordingExecutor(returncode=1),
        build_lock=held_build_lock,
    )

    with pytest.raises(DbtCommandError) as error:
        runner.build()

    assert "dbt output" in str(error.value)
    assert "dbt error" in str(error.value)


def test_build_holds_and_releases_lock_when_dbt_fails() -> None:
    events: list[str] = []

    @contextmanager
    def lock() -> Iterator[int]:
        events.append("acquired")
        try:
            yield 1234
        finally:
            events.append("released")

    def execute(
        command: tuple[str, ...], *, cwd: Path, environment: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        assert events == ["acquired"]
        events.append("executed")
        return subprocess.CompletedProcess(command, 1, "failure", "")

    runner = DbtRunner(
        project_dir=Path("/workspace"),
        profiles_dir=Path("/profiles"),
        build_lock=lock,
        executor=execute,
    )

    with pytest.raises(DbtCommandError):
        runner.build()

    assert events == ["acquired", "executed", "released"]
