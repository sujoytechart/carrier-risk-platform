"""Render aggregate holdout evidence without exposing individual carrier records."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render(evidence_path: Path, output_path: Path) -> None:
    """Plot the baseline comparison, positive detection and confusion counts."""
    evidence = json.loads(evidence_path.read_text())
    metrics = evidence["metrics"]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
    figure, axes = plt.subplots(
        1, 3, figsize=(16, 6), gridspec_kw={"width_ratios": [1, 1, 1.25]}
    )
    figure.patch.set_facecolor("white")
    figure.suptitle(
        "Real-data model · later-period holdout",
        x=0.055,
        y=0.98,
        ha="left",
        fontsize=22,
        weight="bold",
        color="#243244",
    )
    figure.text(
        0.055,
        0.88,
        f"{metrics['training_rows']:,.0f} training carriers → "
        f"{metrics['heldout_rows']:,.0f} test carriers · 4 months of inspections · "
        "6-month recorded-crash target",
        color="#526278",
        fontsize=11,
    )
    groups = [
        (
            axes[0],
            "Ranking quality",
            ["Prior-crash\nbaseline", "Experimental\nmodel"],
            [
                metrics["baseline_average_precision"],
                metrics["candidate_average_precision"],
            ],
            ["#DDEBFA", "#DDF1E6"],
        ),
        (
            axes[1],
            "Positive outcomes",
            ["Recorded positives\ndetected", "Positive predictions\ncorrect"],
            [metrics["positive_recall"], metrics["precision"]],
            ["#EADFF4", "#F5E1E5"],
        ),
    ]
    for axis, title, labels, values, colors in groups:
        bars = axis.bar(
            labels,
            [value * 100 for value in values],
            color=colors,
            edgecolor="#7A8EA1",
            width=0.65,
        )
        axis.set_title(title, loc="left", weight="bold", color="#243244", pad=20)
        axis.set_ylim(0, 60)
        axis.set_yticks([0, 20, 40, 60], ["0%", "20%", "40%", "60%"])
        axis.grid(axis="y", alpha=0.16)
        axis.set_axisbelow(True)
        axis.spines[["top", "right", "left"]].set_visible(False)
        axis.spines["bottom"].set_color("#CBD5E1")
        axis.tick_params(length=0, pad=10, colors="#526278")
        for bar, value in zip(bars, values, strict=True):
            axis.text(
                bar.get_x() + bar.get_width() / 2,
                value * 100 + 2,
                f"{value:.1%}",
                ha="center",
                weight="bold",
                fontsize=15,
                color="#243244",
            )
    axes[0].set_ylabel("Average precision", color="#526278")
    axes[2].axis("off")
    axes[2].set_title(
        "Actual vs predicted", loc="left", weight="bold", color="#243244", pad=20
    )
    counts = [
        [metrics["true_negative"], metrics["false_positive"]],
        [metrics["false_negative"], metrics["true_positive"]],
    ]
    table = axes[2].table(
        cellText=[[f"{value:,.0f}" for value in row] for row in counts],
        colLabels=["Predicted\nnegative", "Predicted\npositive"],
        rowLabels=["No recorded\ncrash", "Recorded\ncrash"],
        cellLoc="center",
        bbox=[0.14, 0.30, 0.85, 0.58],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    for (row, column), cell in table.get_celld().items():
        cell.set_edgecolor("#DAE2EB")
        cell.set_text_props(color="#243244")
        cell.set_facecolor(
            "#F0F3FA"
            if row == 0 or column == -1
            else "#DDF1E6"
            if (row, column) in {(1, 0), (2, 1)}
            else "#FFF0CC"
        )
    axes[2].text(
        0.14,
        0.10,
        f"Test positive prevalence: {metrics['heldout_prevalence']:.1%}\n"
        f"Training-selected threshold: {metrics['threshold']:.4f}",
        color="#526278",
        transform=axes[2].transAxes,
    )
    figure.text(
        0.055,
        0.12,
        f"Overall accuracy {metrics['accuracy']:.1%} · always-negative accuracy "
        f"{metrics['always_negative_accuracy']:.1%}. "
        "Accuracy alone hides the class imbalance.",
        color="#243244",
        weight="bold",
    )
    figure.text(
        0.055,
        0.055,
        "Experimental retrospective estimate · retained labels are not all "
        "actual crashes · historical public availability and eventual "
        "completeness are unproven.",
        color="#526278",
        fontsize=10,
    )
    figure.subplots_adjust(left=0.065, right=0.98, top=0.75, bottom=0.30, wspace=0.58)
    figure.savefig(output_path, dpi=170, facecolor="white", bbox_inches="tight")
    plt.close(figure)


def main() -> None:
    """Render the saved machine-readable training result as a shareable PNG."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    render(args.evidence, args.output)


if __name__ == "__main__":
    main()
