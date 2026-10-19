"""Functional plotting helpers for quick one-off charts.

Each helper renders a matplotlib figure, saves it into an output directory
and returns the written path. They complement :mod:`analytics.visualization.charts`
for scripting and pipeline jobs.

Example::

    from analytics.visualization.plots import plot_time_series

    path = plot_time_series(df, x="recorded_at", y="temperature", output_dir="output")
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "output"
COLOR_PRIMARY = "#2563eb"
GRID_LINESTYLE = "--"
GRID_ALPHA = 0.4


def _finish(fig: Figure, output_dir: Path | str, filename: str, dpi: int = 150) -> Path:
    """Save a figure to ``output_dir/filename`` and close it.

    Args:
        fig: Figure to persist.
        output_dir: Destination directory (created when missing).
        filename: Output file name (png/svg/pdf/...).
        dpi: Output resolution.

    Returns:
        The resolved output path.
    """
    out_path = Path(output_dir) / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    LOGGER.info("Plot written to %s", out_path)
    return out_path


def plot_time_series(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = "Time Series",
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    filename: str | None = None,
) -> Path:
    """Plot ``y`` over ``x`` and save the figure.

    Args:
        df: Source DataFrame.
        x: Time column for the x axis.
        y: Numeric column for the y axis.
        title: Chart title.
        output_dir: Directory where the PNG is written.
        filename: Optional file name; defaults to ``<y>_timeseries.png``.

    Returns:
        Path to the saved image.
    """
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df[x], df[y], color=COLOR_PRIMARY, linewidth=1.4)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.grid(True, linestyle=GRID_LINESTYLE, alpha=GRID_ALPHA)
    fig.autofmt_xdate()
    return _finish(fig, output_dir, filename or f"{y}_timeseries.png")


def plot_distribution(
    series: pd.Series,
    bins: int = 30,
    title: str = "Distribution",
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    filename: str | None = None,
) -> Path:
    """Plot a histogram of a numeric series and save the figure.

    Args:
        series: Numeric series to plot.
        bins: Number of histogram bins.
        title: Chart title.
        output_dir: Directory where the PNG is written.
        filename: Optional file name; defaults to ``<name>_distribution.png``.

    Returns:
        Path to the saved image.
    """
    values = pd.to_numeric(series, errors="coerce").dropna()
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(values, bins=bins, color="#10b981", edgecolor="white")
    ax.axvline(float(values.mean()), color="#ef4444", linestyle=GRID_LINESTYLE, label=f"mean={values.mean():.2f}")
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel(series.name or "value")
    ax.set_ylabel("frequency")
    ax.grid(True, axis="y", linestyle=GRID_LINESTYLE, alpha=GRID_ALPHA)
    ax.legend()
    name = series.name or "series"
    return _finish(fig, output_dir, filename or f"{name}_distribution.png")


def plot_correlation_matrix(
    df: pd.DataFrame,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    filename: str | None = None,
) -> Path:
    """Plot the correlation matrix of the numeric columns via imshow.

    Uses ``pandas.DataFrame.corr`` and renders it with
    ``matplotlib.axes.Axes.imshow``.

    Args:
        df: Source DataFrame.
        output_dir: Directory where the PNG is written.
        filename: Optional file name; defaults to ``correlation_matrix.png``.

    Returns:
        Path to the saved image.
    """
    corr = df.corr(numeric_only=True)
    fig, ax = plt.subplots(figsize=(8, 6))
    image = ax.imshow(corr, cmap="coolwarm", vmin=-1.0, vmax=1.0)
    ax.set_title("Correlation Matrix", fontsize=14, fontweight="bold")
    ax.set_xticks(range(len(corr.columns)))
    ax.set_yticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right")
    ax.set_yticklabels(corr.columns)
    for row in range(len(corr.index)):
        for col in range(len(corr.columns)):
            value = np.nan_to_num(corr.iloc[row, col])
            ax.text(col, row, f"{value:.2f}", ha="center", va="center", fontsize=9)
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    return _finish(fig, output_dir, filename or "correlation_matrix.png")
