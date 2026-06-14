"""
Feature Engineering Module for Thunders BigData System.

Provides methods for creating, transforming, and selecting features
from raw data for machine learning and analytics workflows.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Creates and transforms features for analytics and machine learning.

    Provides methods for temporal, categorical, numerical, and interaction
    feature engineering with automatic metadata tracking.

    Attributes:
        feature_log: List of feature engineering operations performed.
    """

    def __init__(self) -> None:
        """Initialize the FeatureEngineer with an empty feature log."""
        self.feature_log: List[Dict[str, Any]] = []

    def _log_feature(self, operation: str, details: Dict[str, Any]) -> None:
        """Record a feature engineering operation.

        Args:
            operation: Name of the operation.
            details: Details about the operation.
        """
        self.feature_log.append({"operation": operation, **details})

    # -------------------------------------------------------------------------
    # Temporal Features
    # -------------------------------------------------------------------------

    def create_datetime_features(
        self,
        data: pd.DataFrame,
        column: str,
        features: Optional[List[str]] = None,
        prefix: Optional[str] = None,
    ) -> pd.DataFrame:
        """Extract features from a datetime column.

        Args:
            data: Input DataFrame.
            column: Name of the datetime column.
            features: List of features to extract. Available features:
                'year', 'month', 'day', 'hour', 'minute', 'second',
                'dayofweek', 'dayofyear', 'weekofyear', 'quarter',
                'is_weekend', 'is_month_start', 'is_month_end',
                'is_quarter_start', 'is_quarter_end'.
                If None, all features are extracted.
            prefix: Prefix for new feature columns (defaults to column name).

        Returns:
            DataFrame with new datetime feature columns.
        """
        df = data.copy()

        if not pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = pd.to_datetime(df[column], errors="coerce")

        pfx = prefix or column
        all_features = [
            "year", "month", "day", "hour", "minute", "second",
            "dayofweek", "dayofyear", "weekofyear", "quarter",
            "is_weekend", "is_month_start", "is_month_end",
            "is_quarter_start", "is_quarter_end",
        ]
        target_features = features or all_features

        feature_map = {
            "year": lambda s: s.dt.year,
            "month": lambda s: s.dt.month,
            "day": lambda s: s.dt.day,
            "hour": lambda s: s.dt.hour,
            "minute": lambda s: s.dt.minute,
            "second": lambda s: s.dt.second,
            "dayofweek": lambda s: s.dt.dayofweek,
            "dayofyear": lambda s: s.dt.dayofyear,
            "weekofyear": lambda s: s.dt.isocalendar().week.astype(int),
            "quarter": lambda s: s.dt.quarter,
            "is_weekend": lambda s: s.dt.dayofweek.isin([5, 6]).astype(int),
            "is_month_start": lambda s: s.dt.is_month_start.astype(int),
            "is_month_end": lambda s: s.dt.is_month_end.astype(int),
            "is_quarter_start": lambda s: s.dt.is_quarter_start.astype(int),
            "is_quarter_end": lambda s: s.dt.is_quarter_end.astype(int),
        }

        created = []
        for feat in target_features:
            if feat in feature_map:
                col_name = f"{pfx}_{feat}"
                df[col_name] = feature_map[feat](df[column])
                created.append(col_name)

        self._log_feature("create_datetime_features", {
            "source_column": column,
            "features_created": created,
        })
        logger.info("Created %d datetime features from '%s'", len(created), column)
        return df

    def create_lag_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        lags: List[int],
        group_by: Optional[str] = None,
    ) -> pd.DataFrame:
        """Create lag (shift) features for time-series data.

        Args:
            data: Input DataFrame.
            columns: Columns to create lag features for.
            lags: List of lag periods (e.g., [1, 7, 30]).
            group_by: Column to group by before shifting (e.g., 'customer_id').

        Returns:
            DataFrame with new lag feature columns.
        """
        df = data.copy()
        created = []

        for col in columns:
            for lag in lags:
                lag_col = f"{col}_lag_{lag}"
                if group_by:
                    df[lag_col] = df.groupby(group_by)[col].shift(lag)
                else:
                    df[lag_col] = df[col].shift(lag)
                created.append(lag_col)

        self._log_feature("create_lag_features", {
            "columns": columns,
            "lags": lags,
            "group_by": group_by,
            "features_created": created,
        })
        logger.info("Created %d lag features", len(created))
        return df

    def create_rolling_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        windows: List[int],
        functions: Optional[List[str]] = None,
        group_by: Optional[str] = None,
    ) -> pd.DataFrame:
        """Create rolling window features for time-series data.

        Args:
            data: Input DataFrame.
            columns: Columns to create rolling features for.
            windows: List of window sizes (e.g., [7, 14, 30]).
            functions: Aggregation functions ('mean', 'std', 'min', 'max', 'sum').
                Defaults to ['mean', 'std'].
            group_by: Column to group by before applying rolling windows.

        Returns:
            DataFrame with new rolling feature columns.
        """
        df = data.copy()
        functions = functions or ["mean", "std"]
        created = []

        for col in columns:
            for window in windows:
                for func in functions:
                    roll_col = f"{col}_rolling_{window}_{func}"
                    if group_by:
                        df[roll_col] = (
                            df.groupby(group_by)[col]
                            .transform(lambda s: s.rolling(window, min_periods=1).agg(func))
                        )
                    else:
                        df[roll_col] = df[col].rolling(window, min_periods=1).agg(func)
                    created.append(roll_col)

        self._log_feature("create_rolling_features", {
            "columns": columns,
            "windows": windows,
            "functions": functions,
            "features_created": created,
        })
        logger.info("Created %d rolling features", len(created))
        return df

    # -------------------------------------------------------------------------
    # Categorical Features
    # -------------------------------------------------------------------------

    def create_frequency_encoding(
        self,
        data: pd.DataFrame,
        columns: List[str],
    ) -> pd.DataFrame:
        """Replace categorical values with their frequency (count) in the dataset.

        Args:
            data: Input DataFrame.
            columns: Categorical columns to frequency-encode.

        Returns:
            DataFrame with new frequency-encoded columns.
        """
        df = data.copy()
        created = []

        for col in columns:
            freq_col = f"{col}_freq"
            freq_map = df[col].value_counts(normalize=True).to_dict()
            df[freq_col] = df[col].map(freq_map)
            created.append(freq_col)

        self._log_feature("create_frequency_encoding", {
            "columns": columns,
            "features_created": created,
        })
        logger.info("Created %d frequency-encoded features", len(created))
        return df

    def create_target_encoding(
        self,
        data: pd.DataFrame,
        categorical_columns: List[str],
        target_column: str,
        smoothing: float = 1.0,
    ) -> pd.DataFrame:
        """Encode categorical columns with the smoothed mean of the target variable.

        Args:
            data: Input DataFrame.
            categorical_columns: Categorical columns to target-encode.
            target_column: Numeric target column for computing means.
            smoothing: Smoothing parameter to balance category mean and global mean.

        Returns:
            DataFrame with new target-encoded columns.
        """
        df = data.copy()
        global_mean = df[target_column].mean()
        created = []

        for col in categorical_columns:
            te_col = f"{col}_target_enc"
            stats = df.groupby(col)[target_column].agg(["mean", "count"])
            smoothed = (stats["count"] * stats["mean"] + smoothing * global_mean) / (stats["count"] + smoothing)
            df[te_col] = df[col].map(smoothed.to_dict())
            created.append(te_col)

        self._log_feature("create_target_encoding", {
            "columns": categorical_columns,
            "target": target_column,
            "smoothing": smoothing,
            "features_created": created,
        })
        logger.info("Created %d target-encoded features", len(created))
        return df

    # -------------------------------------------------------------------------
    # Numerical Features
    # -------------------------------------------------------------------------

    def create_interaction_features(
        self,
        data: pd.DataFrame,
        column_pairs: List[Tuple[str, str]],
        operations: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Create interaction features between pairs of numeric columns.

        Args:
            data: Input DataFrame.
            column_pairs: List of (col_a, col_b) tuples.
            operations: Operations to apply ('multiply', 'divide', 'add', 'subtract').
                Defaults to ['multiply'].

        Returns:
            DataFrame with new interaction feature columns.
        """
        df = data.copy()
        operations = operations or ["multiply"]
        created = []

        op_map = {
            "multiply": lambda a, b: a * b,
            "divide": lambda a, b: a / (b + 1e-8),
            "add": lambda a, b: a + b,
            "subtract": lambda a, b: a - b,
        }

        for col_a, col_b in column_pairs:
            for op in operations:
                feat_name = f"{col_a}_{op}_{col_b}"
                df[feat_name] = op_map[op](df[col_a], df[col_b])
                created.append(feat_name)

        self._log_feature("create_interaction_features", {
            "pairs": column_pairs,
            "operations": operations,
            "features_created": created,
        })
        logger.info("Created %d interaction features", len(created))
        return df

    def create_binning_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        bins: int = 10,
        strategy: str = "equal_width",
        labels: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Create binned (discretized) features from numeric columns.

        Args:
            data: Input DataFrame.
            columns: Numeric columns to bin.
            bins: Number of bins.
            strategy: Binning strategy - 'equal_width' or 'quantile'.
            labels: Optional labels for bins.

        Returns:
            DataFrame with new binned feature columns.
        """
        df = data.copy()
        created = []

        for col in columns:
            bin_col = f"{col}_binned"
            if strategy == "equal_width":
                df[bin_col] = pd.cut(df[col], bins=bins, labels=labels)
            elif strategy == "quantile":
                df[bin_col] = pd.qcut(df[col], q=bins, labels=labels, duplicates="drop")
            else:
                raise ValueError(f"Unknown binning strategy: {strategy}")
            created.append(bin_col)

        self._log_feature("create_binning_features", {
            "columns": columns,
            "bins": bins,
            "strategy": strategy,
            "features_created": created,
        })
        logger.info("Created %d binned features", len(created))
        return df

    def create_ratio_features(
        self,
        data: pd.DataFrame,
        numerator: str,
        denominator: str,
        feature_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """Create a ratio feature from two numeric columns.

        Args:
            data: Input DataFrame.
            numerator: Column for the numerator.
            denominator: Column for the denominator.
            feature_name: Name for the new feature column.

        Returns:
            DataFrame with the new ratio feature column.
        """
        df = data.copy()
        name = feature_name or f"{numerator}_to_{denominator}_ratio"
        df[name] = df[numerator] / (df[denominator] + 1e-8)

        self._log_feature("create_ratio_features", {
            "numerator": numerator,
            "denominator": denominator,
            "feature_name": name,
        })
        logger.info("Created ratio feature: %s", name)
        return df

    # -------------------------------------------------------------------------
    # Text Features
    # -------------------------------------------------------------------------

    def create_text_features(
        self,
        data: pd.DataFrame,
        column: str,
        features: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Extract basic features from a text column.

        Args:
            data: Input DataFrame.
            column: Text column to process.
            features: List of features to extract. Available:
                'length', 'word_count', 'char_count', 'avg_word_length',
                'digit_count', 'upper_count'.
                Defaults to all features.

        Returns:
            DataFrame with new text feature columns.
        """
        df = data.copy()
        all_features = ["length", "word_count", "char_count", "avg_word_length", "digit_count", "upper_count"]
        target_features = features or all_features
        created = []

        feat_generators = {
            "length": lambda s: s.str.len(),
            "word_count": lambda s: s.str.split().str.len(),
            "char_count": lambda s: s.str.len(),
            "avg_word_length": lambda s: s.str.len() / (s.str.split().str.len() + 1e-8),
            "digit_count": lambda s: s.str.count(r"\d"),
            "upper_count": lambda s: s.str.count(r"[A-Z]"),
        }

        text_series = df[column].fillna("").astype(str)
        for feat in target_features:
            if feat in feat_generators:
                col_name = f"{column}_{feat}"
                df[col_name] = feat_generators[feat](text_series)
                created.append(col_name)

        self._log_feature("create_text_features", {
            "source_column": column,
            "features_created": created,
        })
        logger.info("Created %d text features from '%s'", len(created), column)
        return df

    def get_feature_log(self) -> List[Dict[str, Any]]:
        """Return the log of all feature engineering operations.

        Returns:
            List of operation records.
        """
        return self.feature_log

    def __repr__(self) -> str:
        return f"FeatureEngineer(operations={len(self.feature_log)})"
