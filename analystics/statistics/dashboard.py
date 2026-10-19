"""Streamlit analytics dashboard for the Thunders BigData System.

Run from the repository root with::

    streamlit run analytics/visualization/dashboard.py

The dashboard reads ``data/gold`` CSVs when available and falls back to the
``data/raw`` sample files. Sidebar filters cover the date range and sensor
selection; the main area shows KPI metrics and charts.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import pandas as pd

try:
    import streamlit as st
except ImportError:  # pragma: no cover - only hit when streamlit is missing
    st = None

LOGGER = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = REPO_ROOT / "data" / "gold"
RAW_DIR = REPO_ROOT / "data" / "raw"

ERROR_STATUSES = {"error", "failed"}


def _identity(func: Callable) -> Callable:
    """No-op decorator used as a cache fallback when Streamlit is absent."""
    return func


cache_data: Callable[[Callable], Callable]
if st is not None:
    cache_data = getattr(st, "cache_data", _identity)
else:
    cache_data = _identity


@cache_data
def load_csv(filename: str) -> pd.DataFrame:
    """Load a CSV preferring the gold layer over raw.

    Args:
        filename: CSV file name, e.g. ``sensor_logs.csv``.

    Returns:
        Parsed DataFrame, or an empty DataFrame when unavailable.
    """
    for base_dir in (GOLD_DIR, RAW_DIR):
        candidate = base_dir / filename
        if candidate.exists():
            return pd.read_csv(candidate)
    return pd.DataFrame()


def _prepare(frame: pd.DataFrame, time_column: str) -> pd.DataFrame:
    """Parse the time column and add a lowercase status helper column."""
    if frame.empty or time_column not in frame.columns:
        return frame
    frame = frame.copy()
    frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce")
    return frame


def main() -> None:
    """Render the Streamlit dashboard."""
    if st is None:  # pragma: no cover - exercised only outside streamlit runtime
        raise SystemExit("Streamlit is required. Install it with: pip install streamlit")

    st.set_page_config(page_title="Thunders Analytics", layout="wide")
    st.title("Thunders Analytics")
    st.caption("Sensor and transaction overview built from the sample data pipeline.")

    sensors = _prepare(load_csv("sensor_logs.csv"), "recorded_at")
    transactions = _prepare(load_csv("transactions.csv"), "created_at")

    if sensors.empty:
        st.warning("No sensor data found under data/gold or data/raw.")
        return

    # ---- Sidebar filters -------------------------------------------------
    st.sidebar.header("Filters")
    default_start = sensors["recorded_at"].min().date()
    default_end = sensors["recorded_at"].max().date()
    date_range = st.sidebar.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=default_start,
        max_value=default_end,
    )
    all_sensors = sorted(sensors["sensor_id"].dropna().astype(str).unique())
    selected_sensors = st.sidebar.multiselect("Sensors", options=all_sensors, default=all_sensors)

    start_date, end_date = (date_range[0], date_range[-1]) if isinstance(date_range, tuple) else (default_start, default_end)

    sensors_filtered = sensors[
        (sensors["recorded_at"].dt.date >= start_date)
        & (sensors["recorded_at"].dt.date <= end_date)
        & (sensors["sensor_id"].astype(str).isin(selected_sensors))
    ]

    transactions_filtered = transactions
    if not transactions.empty and "created_at" in transactions.columns:
        transactions_filtered = transactions[
            (transactions["created_at"].dt.date >= start_date)
            & (transactions["created_at"].dt.date <= end_date)
        ]

    # ---- KPI row ---------------------------------------------------------
    total_readings = len(sensors_filtered)
    avg_temp = float(sensors_filtered["temperature"].mean()) if "temperature" in sensors_filtered else 0.0
    tx_count = len(transactions_filtered)
    error_rate = 0.0
    if total_readings and "status" in sensors_filtered.columns:
        statuses = sensors_filtered["status"].astype(str).str.lower()
        error_rate = float(statuses.isin(ERROR_STATUSES).mean())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Sensor readings", f"{total_readings:,}")
    col2.metric("Avg temperature", f"{avg_temp:.2f}")
    col3.metric("Transactions", f"{tx_count:,}")
    col4.metric("Sensor error rate", f"{error_rate:.2%}")

    # ---- Charts ----------------------------------------------------------
    st.subheader("Temperature over time")
    if not sensors_filtered.empty and "temperature" in sensors_filtered.columns:
        series = sensors_filtered.set_index("recorded_at")["temperature"]
        st.line_chart(series)
    else:
        st.info("No temperature readings in the selected range.")

    st.subheader("Transactions by status")
    if not transactions_filtered.empty and "status" in transactions_filtered.columns:
        counts = transactions_filtered.groupby("status").size()
        st.bar_chart(counts)
    else:
        st.info("No transactions in the selected range.")

    st.caption("Data source: data/gold with fallback to data/raw sample CSVs.")


if __name__ == "__main__":
    main()
