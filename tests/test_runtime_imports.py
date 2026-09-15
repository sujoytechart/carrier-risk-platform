"""Runtime imports must not require development-only typing packages."""

from __future__ import annotations

import subprocess
import sys


def test_production_modules_import_without_boto_type_stubs() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys

class NoTypingPackages(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith('types_boto3'):
            raise ModuleNotFoundError(fullname)

sys.meta_path.insert(0, NoTypingPackages())
import ingest.landing
import orchestration.runtime
import orchestration.backfill_service
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
