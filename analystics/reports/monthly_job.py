"""Monthly snapshot: partition export to data/warehouse plus summary report."""

from __future__ import annotations

import sys

import pandas as pd

from pipelines import DATA_DIR, GOLD_DIR, get_logger

logger = get_logger(__name__)


def summary_report(df: pd.DataFrame) -> dict[str, object]:
    """Build a compact summary dict for the exported snapshot."""
    report: dict[str, object] = {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "nulls": int(df.isna().sum().sum()),
        "duplicates": int(df.duplicated().sum()),
    }
    for column in df.select_dtypes(include="number").columns:
        report[f"{column}_avg"] = round(float(df[column].mean()), 4)
    return report


def run_monthly(month: str | None = None, source_table: str = "sensor_logs_daily") -> int:
    """Export a month-partitioned snapshot under data/warehouse/month=YYYY-MM.

    Args:
        month: Partition label (YYYY-MM). Defaults to the latest month found.
        source_table: Gold table name to snapshot.
    """
    try:
        source_path = GOLD_DIR / f"{source_table}.csv"
        if not source_path.exists():
            raise FileNotFoundError(f"Gold table not found: {source_path}")
        df = pd.read_csv(source_path)
        if "date" not in df.columns:
            raise KeyError("Gold table must contain a 'date' column for partitioning.")
        month_values = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m")
        month = month or month_values.dropna().max()
        if not month:
            raise ValueError("No valid dates available to derive a month partition.")
        partition = df.loc[month_values == month].reset_index(drop=True)
        target = DATA_DIR / "warehouse" / f"month={month}"
        target.mkdir(parents=True, exist_ok=True)
        out_path = target / f"{source_table}.csv"
        partition.to_csv(out_path, index=False)
        report = summary_report(partition)
        report.update({"month": month, "partition_path": str(out_path)})
        logger.info("[monthly] snapshot written: %s", report)
        # TODO: also export to data/lake as parquet with Hive-style partitioning.
        return 0
    except Exception as exc:  # noqa: BLE001 - batch jobs exit non-zero instead of raising
        logger.error("[monthly] job FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(run_monthly())
