"""Daily ETL job: ingest -> clean -> transform -> load into silver/gold.

Runnable via ``python -m pipelines.batch.daily_job``; logs every stage
and exits non-zero when any stage fails.
"""

from __future__ import annotations

import sys

import pandas as pd

from pipelines import GOLD_DIR, RAW_DIR, SILVER_DIR, get_logger
from pipelines.cleaning.missing_values import handle_missing
from pipelines.cleaning.normalize import normalize_column_names, normalize_timestamps
from pipelines.cleaning.remove_duplicates import deduplicate
from pipelines.cleaning.validate import DataFrameValidator, ValidationRule
from pipelines.ingestion.csv_ingestion import CsvIngestion
from pipelines.transformation.aggregation import aggregate_daily
from pipelines.transformation.feature_engineering import build_features

logger = get_logger(__name__)


def ingest_stage(source: str = "sensor_logs") -> pd.DataFrame:
    """Stage 1: read the raw CSV source into a validated DataFrame."""
    logger.info("[daily] Stage 1/4: ingest source '%s'", source)
    return CsvIngestion().ingest(RAW_DIR / f"{source}.csv")


def clean_stage(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Stage 2: dedupe, normalise, impute, validate; writes silver CSV."""
    logger.info("[daily] Stage 2/4: clean")
    cleaned, stats = deduplicate(raw_df)
    cleaned = normalize_column_names(cleaned)
    cleaned = normalize_timestamps(cleaned)
    cleaned = handle_missing(cleaned, {"temperature": "fill_median", "humidity": "fill_ffill"})
    validator = DataFrameValidator(
        [
            ValidationRule("not_null", "sensor_id"),
            ValidationRule("range", "temperature", {"min": -50.0, "max": 80.0}),
        ]
    )
    report = validator.validate(cleaned)
    if not report["passed"]:
        raise ValueError(f"Validation failed: {report['violations']}")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(SILVER_DIR / "sensor_logs_clean.csv", index=False)
    logger.info("[daily] dedup stats: %s", stats)
    return cleaned


def transform_stage(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Stage 3: feature engineering plus the daily gold rollup."""
    logger.info("[daily] Stage 3/4: transform")
    features = build_features(cleaned)
    features.to_csv(SILVER_DIR / "sensor_logs_features.csv", index=False)
    return aggregate_daily(
        cleaned,
        group_cols=["sensor_id"],
        metrics={"temperature": "mean", "humidity": "mean"},
        timestamp_col="recorded_at",
    )


def load_stage(rollup: pd.DataFrame) -> None:
    """Stage 4: persist the gold rollup under data/gold."""
    logger.info("[daily] Stage 4/4: load")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    rollup.to_csv(GOLD_DIR / "sensor_logs_daily.csv", index=False)


def run_daily(source: str = "sensor_logs") -> int:
    """Chain all stages; returns 0 on success, 1 on failure."""
    try:
        raw_df = ingest_stage(source)
        cleaned = clean_stage(raw_df)
        rollup = transform_stage(cleaned)
        load_stage(rollup)
        logger.info("[daily] job finished OK (%d gold rows).", len(rollup))
        return 0
    except Exception as exc:  # noqa: BLE001 - batch jobs exit non-zero instead of raising
        logger.error("[daily] job FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(run_daily())
