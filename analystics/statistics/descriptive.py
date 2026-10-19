"""Descriptive statistics helpers.

Extended per-column summaries beyond ``DataFrame.describe`` plus a frequency
table builder for categorical columns.

Example::

    from analytics.statistics.descriptive import describe, freq_summary

    stats = describe(df)
    table = freq_summary(df["status"])
"""

from __future__ import annotations

import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)


def describe(df: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Compute extended descriptive statistics for every numeric column.

    Includes mean, median, standard deviation, skewness, kurtosis and the
    quartiles.

    Args:
        df: Source DataFrame.

    Returns:
        Mapping of column name to a statistics dictionary. Columns with no
        numeric values are skipped (a warning is logged).
    """
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        LOGGER.warning("describe(): no numeric columns found in DataFrame.")
        return {}

    stats: dict[str, dict[str, float]] = {}
    for column in numeric.columns:
        series = numeric[column].dropna()
        if series.empty:
            LOGGER.debug("Column '%s' contains only NaN values; skipped.", column)
            continue
        stats[column] = {
            "count": int(series.count()),
            "mean": float(series.mean()),
            "median": float(series.median()),
            "std": float(series.std()),
            "min": float(series.min()),
            "q1": float(series.quantile(0.25)),
            "q2": float(series.quantile(0.50)),
            "q3": float(series.quantile(0.75)),
            "max": float(series.max()),
            "skew": float(series.skew()),
            "kurtosis": float(series.kurtosis()),
        }
    return stats


def freq_summary(series: pd.Series, top_n: int = 10) -> pd.DataFrame:
    """Build a frequency table with counts and proportions.

    Args:
        series: Categorical series to summarize.
        top_n: Maximum number of categories to include.

    Returns:
        DataFrame indexed by category with ``count`` and ``proportion``
        columns, sorted by descending count.
    """
    if series.empty:
        LOGGER.warning("freq_summary(): empty series; returning empty table.")
        return pd.DataFrame(columns=["count", "proportion"])

    counts = series.value_counts(dropna=False).head(top_n)
    table = pd.DataFrame({"count": counts})
    table["proportion"] = (table["count"] / len(series)).round(4)
    table.index.name = series.name or "category"
    return table
