"""Reusable chart builder with consistent Thunders styling.

The :class:`ChartBuilder` wraps matplotlib and returns styled
:class:`matplotlib.figure.Figure` objects so callers can render, tweak or
save them.

Example::

    from analytics.visualization.charts import ChartBuilder

    builder = ChartBuilder()
    fig = builder.line(df, x="recorded_at", y="temperature", title="Temperature")
    ChartBuilder.save(fig, "output/temperature.png")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional, Sequence

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

LOGGER = logging.getLogger(__name__)

# Consistent palette: primary blue, amber, green, red, violet.
PALETTE: list[str] = ["#2563eb", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6"]

GRID_LINESTYLE = "--"
GRID_ALPHA = 0.4
DEFAULT_FIGSIZE: tuple[float, float] = (10.0, 5.0)


class ChartBuilder:
    """Build consistently styled matplotlib figures.

    Args:
        palette: Color cycle used across charts.
        figsize: Default figure size in inches.
    """

    def __init__(
        self,
        palette: Sequence[str] = tuple(PALETTE),
        figsize: tuple[float, float] = DEFAULT_FIGSIZE,
    ) -> None:
        self.palette: list[str] = list(palette)
        self.figsize = figsize

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _new_axes(self, title: str, xlabel: str, ylabel: str) -> tuple[Figure, plt.Axes]:
        """Create a styled figure/axes pair with title and axis labels."""
        fig, ax = plt.subplots(figsize=self.figsize)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(True, linestyle=GRID_LINESTYLE, alpha=GRID_ALPHA)
        ax.set_prop_cycle(color=self.palette)
        return fig, ax

    # ------------------------------------------------------------------ #
    # Public chart builders
    # ------------------------------------------------------------------ #
    def line(
        self,
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str = "Line Chart",
        group: Optional[str] = None,
    ) -> Figure:
        """Render a line chart, optionally splitting one line per group.

        Args:
            df: Source DataFrame.
            x: Column for the x axis.
            y: Column for the y axis.
            title: Chart title.
            group: Optional categorical column to draw one line per value.

        Returns:
            Styled matplotlib Figure.
        """
        fig, ax = self._new_axes(title, x, y)
        if group and group in df.columns:
            for color, (key, sub) in zip(
                self.palette * (1 + len(df[group].dropna().unique()) // len(self.palette)),
                df.groupby(group),
            ):
                ax.plot(sub[x], sub[y], label=str(key), color=color, linewidth=1.4)
            ax.legend(title=group)
        else:
            ax.plot(df[x], df[y], color=self.palette[0], linewidth=1.4)
        fig.autofmt_xdate()
        fig.tight_layout()
        return fig

    def bar(
        self,
        df: pd.DataFrame,
        x: str,
        y: Optional[str] = None,
        title: str = "Bar Chart",
        agg: str = "sum",
    ) -> Figure:
        """Render a bar chart.

        Args:
            df: Source DataFrame.
            x: Categorical column for the bars.
            y: Numeric column to aggregate; when omitted, row counts per
                category are plotted.
            title: Chart title.
            agg: Aggregation applied to ``y`` (``sum`` or ``mean``).

        Returns:
            Styled matplotlib Figure.
        """
        fig, ax = self._new_axes(title, x, y or "count")
        if y is None:
            counts = df.groupby(x).size().sort_values(ascending=False)
            ax.bar(counts.index.astype(str), counts.values, color=self.palette[0])
        else:
            grouped = df.groupby(x)[y]
            values = getattr(grouped, agg)().sort_values(ascending=False)
            ax.bar(values.index.astype(str), values.values, color=self.palette[1])
        fig.tight_layout()
        return fig

    def heatmap(
        self,
        df: pd.DataFrame,
        columns: Optional[Iterable[str]] = None,
        title: str = "Correlation Heatmap",
        annotate: bool = True,
    ) -> Figure:
        """Render a correlation heatmap for the numeric columns of a frame.

        Args:
            df: Source DataFrame.
            columns: Optional subset of columns; defaults to all numeric ones.
            title: Chart title.
            annotate: Whether to print correlation values in the cells.

        Returns:
            Styled matplotlib Figure.
        """
        frame = df[list(columns)] if columns is not None else df.select_dtypes(include="number")
        corr = frame.corr(numeric_only=True)

        fig, ax = plt.subplots(figsize=self.figsize)
        image = ax.imshow(corr, cmap="coolwarm", vmin=-1.0, vmax=1.0)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.set_xticks(range(len(corr.columns)))
        ax.set_yticks(range(len(corr.columns)))
        ax.set_xticklabels(corr.columns, rotation=45, ha="right")
        ax.set_yticklabels(corr.columns)
        if annotate:
            for row in range(len(corr.index)):
                for col in range(len(corr.columns)):
                    ax.text(col, row, f"{corr.iloc[row, col]:.2f}", ha="center", va="center", fontsize=9)
        fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #
    @staticmethod
    def save(fig: Figure, path: Path | str, dpi: int = 150) -> Path:
        """Save a figure and close it to free memory.

        Args:
            fig: Figure produced by any ChartBuilder method.
            path: Destination image path (parent dirs are created).
            dpi: Output resolution.

        Returns:
            The resolved output path.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        LOGGER.info("Chart saved to %s", path)
        return path
