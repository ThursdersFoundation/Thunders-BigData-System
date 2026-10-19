"""Hypothesis testing helpers.

All tests return a uniform payload: ``statistic``, ``p_value``,
``reject_null`` and a plain-language ``interpretation`` string.

- :func:`t_test`: two-sample t-test (Welch by default).
- :func:`paired_t_test`: paired t-test on differences.
- :func:`chi_square`: chi-square test of independence on a contingency table.

scipy is used when installed; a documented normal-approximation fallback
covers the t-tests so the helpers still run in minimal environments.
"""

from __future__ import annotations

import logging
import math
from statistics import NormalDist
from typing import Sequence

import numpy as np

try:
    from scipy import stats as scipy_stats
except ImportError:  # pragma: no cover - exercised only without scipy installed
    scipy_stats = None
    logging.getLogger(__name__).warning(
        "scipy not installed; t-tests use a normal approximation fallback and "
        "chi_square returns p_value=None."
    )

LOGGER = logging.getLogger(__name__)


def _decision(p_value: float | None, alpha: float) -> bool | None:
    """Return the reject-null decision (None when p_value is unavailable)."""
    if p_value is None:
        return None
    return bool(p_value < alpha)


def _normal_two_sided_p(z: float) -> float:
    """Two-sided p-value from a z statistic via the normal distribution."""
    return float(2.0 * (1.0 - NormalDist().cdf(abs(z))))


def _clean(a: Sequence[float], b: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
    """Coerce two sequences to numeric arrays and drop pairwise non-finite values."""
    a_arr = np.asarray(a, dtype=float)
    b_arr = np.asarray(b, dtype=float)
    return a_arr[np.isfinite(a_arr)], b_arr[np.isfinite(b_arr)]


def _welch_fallback(a: np.ndarray, b: np.ndarray) -> tuple[float, float | None]:
    """Welch t statistic with a normal-approximation p-value (scipy-free).

    Returns:
        Tuple of (t_statistic, p_value_or_None).
    """
    n_a, n_b = len(a), len(b)
    if n_a < 2 or n_b < 2:
        raise ValueError("Each sample needs at least 2 observations for a t-test.")
    var_a, var_b = a.var(ddof=1), b.var(ddof=1)
    se = math.sqrt(var_a / n_a + var_b / n_b)
    if se == 0:
        return 0.0, 1.0
    t_stat = float((a.mean() - b.mean()) / se)
    return t_stat, _normal_two_sided_p(t_stat)


def t_test(
    a: Sequence[float],
    b: Sequence[float],
    alpha: float = 0.05,
    equal_var: bool = False,
) -> dict[str, object]:
    """Two-sample t-test comparing the means of groups ``a`` and ``b``.

    Args:
        a: First sample.
        b: Second sample.
        alpha: Significance level for the decision.
        equal_var: When False (default) run Welch's unequal-variance t-test.

    Returns:
        Mapping with ``statistic``, ``p_value``, ``reject_null`` and
        ``interpretation``.
    """
    a_arr, b_arr = _clean(a, b)
    if scipy_stats is not None:
        result = scipy_stats.ttest_ind(a_arr, b_arr, equal_var=equal_var)
        statistic, p_value = float(result.statistic), float(result.pvalue)
    else:
        statistic, p_value = _welch_fallback(a_arr, b_arr)

    reject_null = _decision(p_value, alpha)
    test_name = "Welch's t-test" if not equal_var else "Student's t-test"
    interpretation = (
        f"{test_name}: t={statistic:.4f}, p={p_value if p_value is None else round(p_value, 4)}. "
        + (
            "Reject the null hypothesis: the group means differ significantly "
            f"at alpha={alpha}."
            if reject_null
            else f"Fail to reject the null hypothesis at alpha={alpha}: no significant mean difference."
            if reject_null is not None
            else "p_value unavailable; inspect the statistic manually."
        )
    )
    return {"statistic": statistic, "p_value": p_value, "reject_null": reject_null, "interpretation": interpretation}


def paired_t_test(
    a: Sequence[float],
    b: Sequence[float],
    alpha: float = 0.05,
) -> dict[str, object]:
    """Paired t-test on aligned samples (e.g. before/after measurements).

    Args:
        a: First measurement sample.
        b: Second measurement sample (same length/order as ``a``).
        alpha: Significance level for the decision.

    Returns:
        Mapping with ``statistic``, ``p_value``, ``reject_null`` and
        ``interpretation``.
    """
    a_arr = np.asarray(a, dtype=float)
    b_arr = np.asarray(b, dtype=float)
    if len(a_arr) != len(b_arr):
        raise ValueError("Paired samples must have the same length.")
    mask = np.isfinite(a_arr) & np.isfinite(b_arr)
    differences = a_arr[mask] - b_arr[mask]
    if len(differences) < 2:
        raise ValueError("At least 2 aligned pairs are required.")

    if scipy_stats is not None:
        result = scipy_stats.ttest_rel(a_arr[mask], b_arr[mask])
        statistic, p_value = float(result.statistic), float(result.pvalue)
    else:
        mean_diff = differences.mean()
        se = differences.std(ddof=1) / math.sqrt(len(differences))
        if se == 0:
            statistic, p_value = 0.0, 1.0
        else:
            statistic = float(mean_diff / se)
            p_value = _normal_two_sided_p(statistic)

    reject_null = _decision(p_value, alpha)
    interpretation = (
        f"Paired t-test: t={statistic:.4f}, p={p_value if p_value is None else round(p_value, 4)}. "
        + (
            f"Reject the null hypothesis at alpha={alpha}: paired means differ significantly."
            if reject_null
            else f"Fail to reject the null hypothesis at alpha={alpha}: no significant paired difference."
            if reject_null is not None
            else "p_value unavailable; inspect the statistic manually."
        )
    )
    return {"statistic": statistic, "p_value": p_value, "reject_null": reject_null, "interpretation": interpretation}


def chi_square(table: Sequence[Sequence[int]], alpha: float = 0.05) -> dict[str, object]:
    """Chi-square test of independence on a contingency table.

    Args:
        table: Contingency table as a 2D sequence of counts.
        alpha: Significance level for the decision.

    Returns:
        Mapping with ``statistic``, ``p_value``, ``reject_null`` and
        ``interpretation``. ``p_value`` and ``reject_null`` are None when
        scipy is unavailable (the statistic is still computed).
    """
    observed = np.asarray(table, dtype=float)
    if observed.ndim != 2 or observed.shape[0] < 2 or observed.shape[1] < 2:
        raise ValueError("chi_square expects a 2D contingency table with >= 2 rows and columns.")
    if (observed < 0).any() or observed.sum() == 0:
        raise ValueError("Contingency table must contain non-negative counts summing above zero.")

    if scipy_stats is not None:
        statistic, p_value, dof, _expected = scipy_stats.chi2_contingency(observed)
        p_value = float(p_value)
    else:
        expected = np.outer(observed.sum(axis=1), observed.sum(axis=0)) / observed.sum()
        with np.errstate(divide="ignore", invalid="ignore"):
            terms = np.where(expected > 0, (observed - expected) ** 2 / expected, 0.0)
        statistic = float(terms.sum())
        p_value = None

    reject_null = _decision(p_value, alpha)
    interpretation = (
        f"Chi-square test: chi2={statistic:.4f}, "
        + (f"p={round(p_value, 4)}. " if p_value is not None else "p unavailable. ")
        + (
            f"Reject the null hypothesis at alpha={alpha}: the variables are not independent."
            if reject_null
            else f"Fail to reject the null hypothesis at alpha={alpha}: no evidence against independence."
            if reject_null is not None
            else "Install scipy to compute the p-value."
        )
    )
    return {"statistic": statistic, "p_value": p_value, "reject_null": reject_null, "interpretation": interpretation}
