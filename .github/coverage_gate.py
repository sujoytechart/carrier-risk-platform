"""Enforce project branch coverage and coverage of changed executable Python lines."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path

PROJECT_BASELINE = Decimal("88.09963099630997")
CHANGED_LINE_MINIMUM = Decimal("80")
SOURCE_DIRECTORIES = ("ingest", "orchestration", "ml", "serving")
HUNK_HEADER = re.compile(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def git(*arguments: str) -> str:
    """Run Git without shell interpolation or filename newline conversion."""
    result = subprocess.run(
        ["git", "--literal-pathspecs", *arguments],
        check=True,
        capture_output=True,
    )
    return result.stdout.decode(errors="surrogateescape")


def resolve_commit(revision: str) -> str | None:
    """Return a commit identifier, or None for an absent/unavailable revision."""
    if not revision:
        return None
    try:
        return git(
            "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"
        ).strip()
    except subprocess.CalledProcessError:
        return None


def comparison_base(requested: str, fallback: str) -> str | None:
    """Use the event base, a distinct merge-base, or all tracked source lines."""
    if base := resolve_commit(requested):
        print(f"Comparing event base {base} with HEAD")
        return base
    if fallback_commit := resolve_commit(fallback):
        try:
            base = git("merge-base", "HEAD", fallback_commit).strip()
        except subprocess.CalledProcessError:
            base = ""
        if base and base != resolve_commit("HEAD"):
            print(f"Event base unavailable. Comparing merge-base {base} with HEAD")
            return base
    print("Event base unavailable. Checking all tracked executable source lines")
    return None


def changed_lines(base: str, filename: str) -> set[int]:
    """Read added line numbers. Renamed destinations count as entirely new files."""
    diff = git(
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--text",
        "--unified=0",
        base,
        "HEAD",
        "--",
        filename,
    )
    additions: set[int] = set()
    for line in diff.splitlines():
        if not line.startswith("@@"):
            continue
        match = HUNK_HEADER.match(line)
        if match is None:
            raise ValueError(f"Unrecognized diff hunk for {filename!r}: {line!r}")
        start = int(match[1])
        count = int(match[2]) if match[2] is not None else 1
        additions.update(range(start, start + count))
    return additions


def line_numbers(value: object, filename: str) -> set[int]:
    """Reject incomplete or malformed coverage line metadata."""
    if not isinstance(value, list) or any(
        type(number) is not int or number < 1 for number in value
    ):
        raise ValueError(f"Invalid coverage line metadata for {filename!r}")
    return set(value)


def main() -> None:
    """Fail if coverage is incomplete, below baseline, or below the changed-line bar."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, default=Path("coverage.json"))
    parser.add_argument("--base", default="", help="PR base SHA or push before SHA")
    parser.add_argument("--fallback-base", default="refs/remotes/origin/main")
    arguments = parser.parse_args()

    try:
        coverage = json.loads(arguments.coverage.read_text(), parse_float=Decimal)
        if coverage["meta"]["branch_coverage"] is not True:
            raise ValueError("Coverage must include branch measurements")
        project_percent = Decimal(str(coverage["totals"]["percent_covered"]))
        if not project_percent.is_finite() or not 0 <= project_percent <= 100:
            raise ValueError("Invalid project coverage percentage")
        tracked_files = {
            name
            for name in git("ls-files", "-z", "--", *SOURCE_DIRECTORIES).split("\0")
            if name.endswith(".py")
        }
        if not tracked_files:
            raise ValueError("No tracked Python source files found")
        base = comparison_base(arguments.base, arguments.fallback_base)
        changed_files = (
            set(
                git(
                    "diff",
                    "--no-ext-diff",
                    "--no-renames",
                    "--diff-filter=ACMT",
                    "--name-only",
                    "-z",
                    base,
                    "HEAD",
                    "--",
                    *SOURCE_DIRECTORIES,
                ).split("\0")
            )
            if base
            else tracked_files
        )
        covered_count = 0
        executable_count = 0
        for filename in sorted(tracked_files):
            details = coverage["files"][filename]
            executed = line_numbers(details["executed_lines"], filename)
            missing = line_numbers(details["missing_lines"], filename)
            if executed & missing:
                raise ValueError(f"Conflicting coverage line metadata for {filename!r}")
            if filename not in changed_files:
                continue
            executable = executed | missing
            additions = changed_lines(base, filename) if base else executable
            changed_executable = additions & executable
            covered = len(changed_executable & executed)
            covered_count += covered
            executable_count += len(changed_executable)
            print(f"{filename!r}: {covered}/{len(changed_executable)} changed lines")
        changed_percent = (
            Decimal(100) * covered_count / executable_count
            if executable_count
            else Decimal(100)
        )
        print(
            f"Project branch coverage: {project_percent}% (minimum {PROJECT_BASELINE}%)"
        )
        print(
            f"Changed executable lines: {covered_count}/{executable_count} "
            f"= {changed_percent:.3f}% (minimum {CHANGED_LINE_MINIMUM}%)"
        )
        if project_percent < PROJECT_BASELINE or changed_percent < CHANGED_LINE_MINIMUM:
            raise ValueError("Coverage threshold failed")
    except (
        KeyError,
        InvalidOperation,
        TypeError,
        ValueError,
        OSError,
        subprocess.CalledProcessError,
    ) as error:
        parser.exit(1, f"Coverage gate failed: {error}\n")


if __name__ == "__main__":
    main()
