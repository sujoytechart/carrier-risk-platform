"""Render the Phase 4 figures from immutable, committed aggregate evidence.

No data acquisition, SQL execution, model training, or benchmark is performed.
Input hashes and numerical reconciliation fail closed before any figure is written.
See chart-provenance.md for the command, environment, and interpretation limits.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "risk-charts"))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import PIL
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import PercentFormatter

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INPUT_HASHES = {
    "docs/evidence/phase-2/athena-results.json": (
        "2a17669634165fc88caf5c1db62bd5d049714eeb1885b47c0ad846b58dd54396"
    ),
    "docs/evidence/phase-2/parquet-reconciliation.json": (
        "5bbabc64c063d8cfc41e8822cab4832dbad52f9eca4f93d8e9cd33573c75a641"
    ),
    "analytics/athena/report_lag_derived.sql": (
        "dfbb2a2a3d07f913d81cec791e695e2ba123503e7258c56fd9f3b59aae0a8ccf"
    ),
    "docs/evidence/phase-3/latency-session-32/summary.json": (
        "33e402c6f3e6aaa4ea7edb1b8401631230ed41a2e2c414adf34e8bdfd683c0cb"
    ),
    "docs/evidence/phase-3/latency-session-32/manifest.json": (
        "a4382d372aea24a0b308f34fd60cbd4f54b0ce229ef017e06457e2c00816f86e"
    ),
}
LAG_BUCKETS = (
    "00 same day",
    "01 1-2 days",
    "02 3-7 days",
    "03 8-14 days",
    "04 15-30 days",
    "05 31-60 days",
    "06 61-90 days",
    "07 91-180 days",
    "08 181-270 days",
    "09 271+ days",
)
BACKGROUND = "#F7F9FC"
INK = "#172F46"
MUTED = "#506477"
GRID = "#DDE5EE"
BLUE = "#2766A8"
TEAL = "#008477"
AMBER = "#A66A16"


@dataclass(frozen=True)
class LagDistribution:
    """A complete source-row histogram with the recorded Athena quantiles."""

    feed: str
    sample_size: int
    bucket_counts: list[int]
    p50_days: int
    p95_days: int
    p99_days: int
    minimum_days: int
    maximum_days: int


def require(condition: bool, message: str) -> None:
    """Reject inconsistent evidence, including under optimized Python execution."""
    if not condition:
        raise ValueError(message)


def sha256(path: Path) -> str:
    """Return the digest of the exact bytes used or produced by this renderer."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(relative_path: str) -> Any:
    """Read a committed evidence file relative to this script's repository."""
    return json.loads((REPOSITORY_ROOT / relative_path).read_text())


def load_lag_distributions() -> list[LagDistribution]:
    """Reconcile all plotted bins with Athena and the CSV-to-Parquet proof."""
    evidence = read_json("docs/evidence/phase-2/athena-results.json")
    reconciliation = read_json("docs/evidence/phase-2/parquet-reconciliation.json")
    require(evidence["status"] == "passed", "Athena acceptance did not pass")
    queries = {query["name"]: query for query in evidence["queries"]}
    for query in queries.values():
        require(query["state"] == "SUCCEEDED", "Athena query did not succeed")
    validation = {row["feed"]: row for row in queries["validate_derived"]["rows"]}
    distributions = []
    for feed in ("crashes", "inspections"):
        rows = [
            row for row in queries["report_lag_derived"]["rows"] if row["feed"] == feed
        ]
        summary = rows[0]
        sample_size = int(summary["lag_sample_size"])
        by_bucket = {row["lag_bucket"]: row for row in rows}
        require(len(by_bucket) == len(rows), f"Duplicate {feed} bucket")
        require(set(by_bucket) <= set(LAG_BUCKETS), f"Unknown {feed} bucket")
        counts = [
            int(by_bucket[bucket]["lag_bucket_row_count"]) if bucket in by_bucket else 0
            for bucket in LAG_BUCKETS
        ]
        require(all(count >= 0 for count in counts), f"Negative {feed} count")
        require(sum(counts) == sample_size, f"Incomplete {feed} histogram")
        for row in rows:
            for field in summary.keys() - {
                "lag_bucket",
                "lag_bucket_row_count",
                "lag_bucket_fraction",
            }:
                require(row[field] == summary[field], f"Inconsistent {feed} {field}")
            require(
                math.isclose(
                    int(row["lag_bucket_row_count"]) / sample_size,
                    float(row["lag_bucket_fraction"]),
                    rel_tol=1e-12,
                ),
                f"Invalid {feed} bucket fraction",
            )
        checked = validation[feed]
        require(summary["availability_quality"] == "source_proxy", "Unexpected scope")
        require(summary["acquisition_date"] == "2026-09-03", "Unexpected snapshot")
        for field in ("manifest_row_count", "catalog_row_count", "lag_sample_size"):
            require(int(checked[field]) == sample_size, f"Mismatched {feed} {field}")
        for field in (
            "row_count_delta",
            "excluded_from_lag_count",
            "negative_lag_count",
        ):
            require(int(checked[field]) == 0, f"Nonzero {feed} {field}")
        converted = reconciliation[feed]["reconciliation"]
        require(
            converted["source_row_count"]
            == converted["parquet_row_count"]
            == sample_size,
            f"Mismatched {feed} conversion row count",
        )
        require(
            converted["source_value_sha256"] == converted["parquet_value_sha256"],
            f"Mismatched {feed} conversion values",
        )
        for field, converted_field in (
            ("content_sha256", "source_content_sha256"),
            ("object_sha256", "source_object_sha256"),
        ):
            require(
                checked[field] == reconciliation[feed][converted_field],
                f"Mismatched {feed} source lineage",
            )
        distributions.append(
            LagDistribution(
                feed,
                sample_size,
                counts,
                int(summary["p50_lag_days"]),
                int(summary["p95_lag_days"]),
                int(summary["p99_lag_days"]),
                int(summary["minimum_lag_days"]),
                int(summary["maximum_lag_days"]),
            )
        )
    return distributions


def load_latency() -> dict[str, Any]:
    """Check the preserved benchmark's scope, completion, and throughput math."""
    evidence = read_json("docs/evidence/phase-3/latency-session-32/summary.json")
    require(evidence["validation_fixture"] is True, "Unexpected latency scope")
    stages = evidence["stages"]
    require(
        [stage["target_rps"] for stage in stages] == [50, 100, 200, 300],
        "Stages changed",
    )
    for stage in stages:
        require(
            stage["completed_requests"]
            == stage["scheduled_requests"]
            == stage["target_rps"] * evidence["measured_seconds_per_stage"],
            "Incomplete benchmark stage",
        )
        require(stage["failures"] == 0, "Benchmark failures must be disclosed")
        require(
            math.isclose(
                stage["achieved_rps"],
                stage["completed_requests"] / stage["elapsed_seconds_including_drain"],
                rel_tol=1e-12,
            ),
            "Throughput does not reconcile",
        )
        require(
            0 <= stage["p50_ms"] <= stage["p95_ms"] <= stage["p99_ms"],
            "Invalid percentile ordering",
        )
    require(stages[2]["p99_ms"] <= 120, "Committed 200-rps latency budget failed")
    return evidence


def style_axes(axes: Axes, *, horizontal: bool = False) -> None:
    """Use restrained grid lines and typography shared by both figures."""
    axes.set_facecolor(BACKGROUND)
    axes.spines[["top", "right", "left"]].set_visible(False)
    axes.spines["bottom"].set_color(GRID)
    axes.tick_params(axis="both", length=0, labelcolor=MUTED, pad=8)
    axes.grid(axis="x" if horizontal else "y", color=GRID, linewidth=0.8)
    axes.set_axisbelow(True)


def save_figure(figure: Figure, path: Path) -> None:
    """Write a deterministic PNG; no wall-clock timestamps enter the metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(
        path, dpi=180, facecolor=BACKGROUND, metadata={"Software": "Matplotlib"}
    )
    plt.close(figure)


def render_report_lag(distributions: list[LagDistribution], path: Path) -> None:
    """Plot complete categorical proportions on identical linear percentage axes."""
    figure, panels = plt.subplots(1, 2, figsize=(13, 8.5))
    figure.subplots_adjust(left=0.11, right=0.95, bottom=0.22, top=0.70, wspace=0.36)
    figure.text(
        0.06,
        0.94,
        "Source records arrive after their events",
        fontsize=23,
        weight="bold",
    )
    figure.text(
        0.06,
        0.893,
        "report_lag_days  ·  snapshot acquired 03 Sep 2026  ·  source_proxy",
        color=MUTED,
        fontsize=12,
    )
    labels = [
        "Same day",
        "1–2",
        "3–7",
        "8–14",
        "15–30",
        "31–60",
        "61–90",
        "91–180",
        "181–270",
        "271+",
    ]
    for axes, distribution, color, title in zip(
        panels,
        distributions,
        (BLUE, TEAL),
        ("Crash-file rows", "Inspection-file rows"),
        strict=True,
    ):
        percentages = [
            count * 100 / distribution.sample_size
            for count in distribution.bucket_counts
        ]
        positions = list(range(len(labels)))
        axes.barh(positions, percentages, height=0.60, color=color, zorder=3)
        axes.set_yticks(positions, labels)
        axes.invert_yaxis()
        axes.set_xlim(0, 50)
        axes.set_xticks([0, 10, 20, 30, 40, 50])
        axes.xaxis.set_major_formatter(PercentFormatter(100, decimals=0))
        axes.set_xlabel("Share of source rows", labelpad=12)
        axes.set_ylabel("Lag bucket (days)", labelpad=12)
        style_axes(axes, horizontal=True)
        for position, percent in zip(positions, percentages, strict=True):
            if percent == 0:
                axes.plot(0, position, "o", markersize=4, color=color, clip_on=False)
            axes.text(
                percent + 0.9,
                position,
                f"{percent:.2f}%",
                va="center",
                fontsize=10.5,
                color=color,
            )
        left = axes.get_position().x0
        figure.text(left, 0.819, title, fontsize=17, weight="bold", color=color)
        figure.text(
            left, 0.781, f"{distribution.sample_size:,} rows", fontsize=11, color=MUTED
        )
        figure.text(
            left,
            0.737,
            f"p50  {distribution.p50_days:,}d     "
            f"p95  {distribution.p95_days:,}d     p99  {distribution.p99_days:,}d",
            fontsize=11,
            weight="bold",
        )
    figure.text(
        0.06,
        0.129,
        "Lag = date(source add timestamp + 1 publication day) − event date.  "
        "Percentiles are Athena approximations.",
        fontsize=10,
        color=MUTED,
    )
    figure.text(
        0.06,
        0.097,
        "Current snapshot rows; crash-file rows are not distinct incidents. "
        "This is not the first-report label-maturity calculation.",
        fontsize=10,
        color=MUTED,
    )
    figure.text(
        0.06,
        0.049,
        "All 10 SQL bins shown, including same-day zeros  ·  "
        "linear percentage axes  ·  no excluded or negative-lag rows",
        fontsize=10,
        color=MUTED,
    )
    save_figure(figure, path)


def render_latency(evidence: dict[str, Any], path: Path) -> None:
    """Show HTTP percentiles, throughput, and the committed acceptance point."""
    stages = evidence["stages"]
    targets = [stage["target_rps"] for stage in stages]
    achieved = [stage["achieved_rps"] for stage in stages]
    acceptance_index = targets.index(200)
    acceptance_stage = stages[acceptance_index]
    acceptance_rps = acceptance_stage["target_rps"]
    figure, (latency, throughput) = plt.subplots(1, 2, figsize=(13, 9))
    figure.subplots_adjust(left=0.08, right=0.96, bottom=0.40, top=0.73, wspace=0.27)
    figure.text(
        0.06,
        0.946,
        "Local synthetic serving-path benchmark",
        fontsize=23,
        weight="bold",
    )
    figure.text(
        0.06,
        0.901,
        "Locust 2.46.5  ·  loopback HTTP + PostgreSQL lookup + "
        "synthetic MLflow model  ·  11 Sep 2026",
        color=MUTED,
        fontsize=11.5,
    )
    figure.text(
        0.06,
        0.838,
        f"{acceptance_rps} rps stage: {acceptance_stage['p99_ms']:.0f} ms "
        "HTTP p99 / 120 ms budget",
        fontsize=17,
        weight="bold",
        color=BLUE,
    )
    figure.text(
        0.06,
        0.798,
        f"{acceptance_stage['completed_requests']:,} of "
        f"{acceptance_stage['scheduled_requests']:,} requests completed  ·  "
        f"{acceptance_stage['failures']} failures  ·  "
        f"{acceptance_stage['achieved_rps']:.3f} achieved requests/s",
        fontsize=11,
        color=MUTED,
    )
    for field, label, color, marker in (
        ("p50_ms", "p50", TEAL, "o"),
        ("p95_ms", "p95", AMBER, "s"),
        ("p99_ms", "p99", BLUE, "D"),
    ):
        latency.plot(
            targets,
            [stage[field] for stage in stages],
            color=color,
            marker=marker,
            markersize=6,
            linewidth=2,
            label=label,
            zorder=3,
        )
    latency.axhline(120, color=MUTED, linewidth=1.2, linestyle=(0, (5, 4)))
    latency.text(
        52,
        123,
        f"Committed p99 ceiling at {acceptance_rps} rps",
        fontsize=10,
        color=MUTED,
    )
    latency.axvline(acceptance_rps, color=BLUE, linewidth=1, alpha=0.25)
    latency.scatter(
        [acceptance_rps],
        [acceptance_stage["p99_ms"]],
        s=160,
        facecolor="none",
        edgecolor=BLUE,
        linewidth=1.5,
        zorder=4,
    )
    latency.set_ylim(0, 137)
    latency.set_yticks([0, 30, 60, 90, 120])
    latency.set_ylabel("HTTP response time (ms)", labelpad=10)
    latency.legend(
        loc="upper left",
        bbox_to_anchor=(0, 0.86),
        frameon=False,
        ncol=3,
        fontsize=10,
        handlelength=1.7,
        columnspacing=1,
    )
    throughput.plot(
        targets,
        achieved,
        color=TEAL,
        marker="o",
        linewidth=2.4,
        markersize=7,
        label="Achieved (includes drain)",
        zorder=3,
    )
    throughput.plot(
        targets,
        targets,
        color=MUTED,
        linewidth=1.1,
        linestyle=(0, (5, 4)),
        label="Requested rate",
        zorder=4,
    )
    throughput.set_ylim(0, 335)
    throughput.set_yticks([0, 100, 200, 300])
    throughput.set_ylabel("Completed requests / elapsed second", labelpad=10)
    throughput.legend(loc="upper left", frameon=False, fontsize=10)
    for axes in (latency, throughput):
        axes.set_xticks(targets)
        axes.set_xlim(40, 310)
        axes.set_xlabel("Scheduled arrival rate (requests/s)", labelpad=10)
        style_axes(axes)
    table_axes = figure.add_axes((0.075, 0.195, 0.885, 0.125))
    table_axes.axis("off")
    table = table_axes.table(
        cellText=[
            [
                str(stage["target_rps"]),
                f"{stage['achieved_rps']:.3f}",
                f"{stage['p50_ms']:.0f}",
                f"{stage['p95_ms']:.0f}",
                f"{stage['p99_ms']:.0f}",
                str(stage["failures"]),
            ]
            for stage in stages
        ],
        colLabels=[
            "Target rps",
            "Achieved rps",
            "p50 ms",
            "p95 ms",
            "p99 ms",
            "Failures",
        ],
        cellLoc="center",
        colLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10.5)
    table.scale(1, 1.5)
    acceptance_row = acceptance_index + 1
    for (row, _column), cell in table.get_celld().items():
        cell.set_edgecolor(BACKGROUND)
        cell.set_facecolor("#E4EDF6" if row == acceptance_row else BACKGROUND)
        cell.set_text_props(
            color=INK, weight="bold" if row in (0, acceptance_row) else "normal"
        )
    figure.text(
        0.06,
        0.113,
        f"{evidence['measured_seconds_per_stage']} s per stage after "
        f"{evidence['warmup_seconds_per_stage']} s warmup; all completions drained. "
        "Percentiles use Locust’s rounded histogram, including failures.",
        fontsize=10,
        color=MUTED,
    )
    figure.text(
        0.06,
        0.082,
        f"At {acceptance_rps} rps: scheduled-arrival p99 "
        f"{acceptance_stage['scheduled_arrival_to_response_p99_ms']:.2f} ms; "
        f"maximum scheduler lag {acceptance_stage['maximum_scheduler_lag_ms']:.2f} ms. "
        "HTTP timing starts when the request is sent.",
        fontsize=10,
        color=MUTED,
    )
    figure.text(
        0.06,
        0.038,
        "512 fictional carriers  ·  isolated local fixture  ·  "
        "does not establish real-data model quality or cloud/production latency",
        fontsize=10,
        color=MUTED,
    )
    save_figure(figure, path)


def main() -> None:
    """Verify inputs, render both figures, and write their hash-linked provenance."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPOSITORY_ROOT,
        help="Destination root for docs/ figures and provenance",
    )
    output_root = parser.parse_args().output_root.resolve()
    for relative_path, expected_hash in INPUT_HASHES.items():
        require(
            sha256(REPOSITORY_ROOT / relative_path) == expected_hash,
            f"Input changed: {relative_path}; review provenance before rendering",
        )
    distributions = load_lag_distributions()
    latency = load_latency()
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "text.color": INK,
            "axes.labelcolor": INK,
            "figure.facecolor": BACKGROUND,
        }
    )
    render_report_lag(distributions, output_root / "docs/report_lag.png")
    render_latency(latency, output_root / "docs/latency.png")
    outputs = {
        relative_path: sha256(output_root / relative_path)
        for relative_path in ("docs/report_lag.png", "docs/latency.png")
    }
    provenance = {
        "purpose": (
            "Phase 4 charts from preserved aggregate evidence; no new measurements"
        ),
        "inputs_sha256": INPUT_HASHES,
        "renderer_sha256": sha256(Path(__file__)),
        "outputs_sha256": outputs,
        "runtime": {
            "python": platform.python_version(),
            "matplotlib": matplotlib.__version__,
            "numpy": np.__version__,
            "pillow": PIL.__version__,
            "backend": "Agg",
            "font": "DejaVu Sans",
            "dpi": 180,
        },
        "report_lag": {
            "availability_quality": "source_proxy",
            "acquisition_date": "2026-09-03",
            "buckets": LAG_BUCKETS,
            "percentiles": (
                "Recorded Athena approx_percentile, not recalculated from bins"
            ),
            "distributions": [asdict(distribution) for distribution in distributions],
            "scope": (
                "Current source snapshot rows; not distinct crash incidents "
                "or label maturity"
            ),
            "validation": (
                "Counts, fractions, source lineage and conversion values reconcile; "
                "zero exclusions and negative lags"
            ),
        },
        "latency": {
            "validation_fixture": True,
            "completed_at": latency["completed_at"],
            "stages": latency["stages"],
            "committed_target": "HTTP p99 <= 120 ms at 200 requested rps",
            "scope": (
                "Local synthetic fixture; not real-data model quality "
                "or production/cloud latency"
            ),
        },
    }
    provenance_path = output_root / "docs/evidence/phase-4/chart-provenance.json"
    provenance_path.parent.mkdir(parents=True, exist_ok=True)
    provenance_path.write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({"validated": True, "outputs_sha256": outputs}, indent=2))


if __name__ == "__main__":
    main()
