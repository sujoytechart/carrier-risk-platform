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
