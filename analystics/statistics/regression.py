"""Regression helpers.

- :func:`simple_linear_regression` uses ``scipy.stats.linregress`` when scipy
  is available and falls back to ``numpy.polyfit`` (without a p-value)
  otherwise.
- :func:`multiple_regression` is a placeholder built on ``numpy.linalg.lstsq``
  (coefficients only; no standard errors yet).
- :func:`interpret_regression` renders a human-readable summary.

Example::

    from analytics.statistics.regression import simple_linear_regression

    result = simple_linear_regression(df["humidity"], df["temperature"])
    print(interpret_regression(result))
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np
import pandas as pd

try:
    from scipy import stats as scipy_stats
except ImportError:  # pragma: no cover - exercised only without scipy installed
    scipy_stats = None
    logging.getLogger(__name__).warning(
        "scipy not installed; simple_linear_regression falls back to numpy (no p_value)."
    )

LOGGER = logging.getLogger(__name__)


def _as_clean_arrays(x: Sequence[float] | pd.Series, y: Sequence[float] | pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """Coerce two series to float arrays, dropping pairwise NaNs/Infs."""
    x_arr = np.asarray(pd.to_numeric(pd.Series(x), errors="coerce"), dtype=float)
    y_arr = np.asarray(pd.to_numeric(pd.Series(y), errors="coerce"), dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    if not mask.any():
        raise ValueError("No finite paired observations available for regression.")
    return x_arr[mask], y_arr[mask]


def simple_linear_regression(
    x: Sequence[float] | pd.Series,
    y: Sequence[float] | pd.Series,
) -> dict[str, float | None]:
    """Fit an ordinary least squares simple linear regression.

    Args:
        x: Predictor values.
        y: Response values (aligned with ``x``).

    Returns:
        Mapping with ``slope``, ``intercept``, ``r2`` and ``p_value``
        (``p_value`` is None when scipy is unavailable).
    """
    x_arr, y_arr = _as_clean_arrays(x, y)

    if scipy_stats is not None:
        result = scipy_stats.linregress(x_arr, y_arr)
        return {
            "slope": float(result.slope),
            "intercept": float(result.intercept),
            "r2": float(result.rvalue**2),
            "p_value": float(result.pvalue),
        }

    slope, intercept = np.polyfit(x_arr, y_arr, 1)
    fitted = slope * x_arr + intercept
    ss_res = float(np.sum((y_arr - fitted) ** 2))
    ss_tot = float(np.sum((y_arr - y_arr.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot else 0.0
    LOGGER.warning("scipy unavailable; p_value not computed for simple regression.")
    return {"slope": float(slope), "intercept": float(intercept), "r2": r2, "p_value": None}


def multiple_regression(
    y: Sequence[float] | pd.Series,
    X: pd.DataFrame | Sequence[Sequence[float]],
) -> dict[str, Any]:
    """Placeholder multiple linear regression via least squares.

    Fits ``y = b0 + b1*x1 + ... + bn*xn`` with ``numpy.linalg.lstsq`` and
    reports the coefficients and R-squared.

    TODO: add standard errors, t-statistics and p-values (e.g. switch to
    statsmodels OLS) plus residual diagnostics.

    Args:
        y: Response values.
        X: Predictor columns (DataFrame or 2D sequence).

    Returns:
        Mapping with ``coefficients`` (intercept first), ``r2`` and
        ``n_observations``.
    """
    y_arr = np.asarray(pd.to_numeric(pd.Series(y), errors="coerce"), dtype=float)
    x_matrix = np.asarray(X, dtype=float)
    if x_matrix.ndim == 1:
        x_matrix = x_matrix.reshape(-1, 1)

    mask = np.isfinite(y_arr) & np.isfinite(x_matrix).all(axis=1)
    y_arr, x_matrix = y_arr[mask], x_matrix[mask]
    if len(y_arr) <= x_matrix.shape[1]:
        raise ValueError("Not enough observations for the number of predictors.")

    design = np.column_stack([np.ones(len(x_matrix)), x_matrix])
    coefficients, _, _, _ = np.linalg.lstsq(design, y_arr, rcond=None)

    fitted = design @ coefficients
    ss_res = float(np.sum((y_arr - fitted) ** 2))
    ss_tot = float(np.sum((y_arr - y_arr.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot else 0.0

    return {
        "coefficients": [round(float(value), 6) for value in coefficients],
        "r2": float(r2),
        "n_observations": int(len(y_arr)),
    }


def interpret_regression(result: dict[str, Any]) -> str:
    """Produce a short plain-language interpretation of a regression result.

    Args:
        result: Mapping from :func:`simple_linear_regression` (or a mapping
            containing at least ``slope`` and ``r2``).

    Returns:
        Human-readable interpretation string.
    """
    slope = result.get("slope")
    r2 = result.get("r2")
    p_value = result.get("p_value")

    direction = "positive" if (slope or 0) > 0 else "negative" if (slope or 0) < 0 else "flat"
    strength = (
        "strong"
        if (r2 or 0) >= 0.7
        else "moderate"
        if (r2 or 0) >= 0.4
        else "weak"
    )
    parts = [
        f"Slope is {direction} ({slope}) with a {strength} fit (R2={r2})."
    ]
    if p_value is not None:
        parts.append(
            "The relationship is statistically significant at alpha=0.05."
            if p_value < 0.05
            else "The relationship is not statistically significant at alpha=0.05."
        )
    else:
        parts.append("p_value unavailable (scipy not installed).")
    return " ".join(parts)
