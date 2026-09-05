"""Checked subprocess boundary for running the repository's dbt project."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Protocol


class CommandExecutor(Protocol):
    """Execute one dbt command with an explicit process context."""

    def __call__(
        self,
        command: tuple[str, ...],
        *,
        cwd: Path,
        environment: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        """Return the completed command without raising for its exit status."""


class DbtCommandError(RuntimeError):
    """Raised when dbt rejects a build or one of its blocking tests."""


class DbtRunner:
    """Run dbt with explicit paths, variables, and environment settings."""

    def __init__(
        self,
        *,
        project_dir: Path,
        profiles_dir: Path,
        build_lock: Callable[[], AbstractContextManager[int]],
        environment: Mapping[str, str] | None = None,
        executable: str = "dbt",
        executor: CommandExecutor | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir
        self._environment = dict(os.environ if environment is None else environment)
        self._executable = executable
        self._executor = executor or _execute_command
        self._build_lock = build_lock

    def build(self, *, scoring_dates: Sequence[str] = ()) -> None:
        """Build all models and fail when contracts or temporal tests fail."""
        arguments: list[str] = []
        if scoring_dates:
            arguments.extend(
                ["--vars", json.dumps({"scoring_dates": list(scoring_dates)})]
            )
        self.run("build", arguments)

    def run(self, command_name: str, arguments: Sequence[str] = ()) -> None:
        """Run a model command while holding the warehouse publication lock."""
        if command_name not in {"build", "run", "seed"}:
            raise ValueError(
                "The locked launcher supports only dbt build, run, or seed"
            )
        command = [
            self._executable,
            command_name,
            "--project-dir",
            str(self._project_dir),
            "--profiles-dir",
            str(self._profiles_dir),
        ]
        command.extend(arguments)
        with self._build_lock() as backend_pid:
            environment = dict(self._environment)
            environment["CARRIER_RISK_DBT_LOCK_PID"] = str(backend_pid)
            result = self._executor(
                tuple(command),
                cwd=self._project_dir,
                environment=environment,
            )
        if result.returncode != 0:
            diagnostics = "\n".join(
                part for part in (result.stdout, result.stderr) if part
            )
            raise DbtCommandError(f"dbt {command_name} failed:\n{diagnostics}")
        logging.getLogger(__name__).info(
            "dbt %s completed\n%s", command_name, result.stdout
        )


def _execute_command(
    command: tuple[str, ...],
    *,
    cwd: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Execute dbt without a shell so arguments cannot be reinterpreted."""
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
