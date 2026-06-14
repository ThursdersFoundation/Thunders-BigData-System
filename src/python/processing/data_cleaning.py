"""
Data Cleaning Module for Thunders BigData System.

Provides comprehensive data quality operations including missing value
imputation, duplicate removal, and outlier detection and treatment.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataCleaner:
    """Performs data cleaning operations on pandas DataFrames.

    Offers configurable strategies for handling missing values, duplicates,
    and outliers with detailed reporting of all changes made.

    Attributes:
        report: Dictionary tracking all cleaning operations performed.
    """

    def __init__(self) -> None:
        """Initialize the DataCleaner with an empty report."""
        self.report: Dict[str, Any] = {}

    def _record(self, operation: str, details: Dict[str, Any]) -> None:
        """Record a cleaning operation in the report.

        Args:
            operation: Name of the cleaning operation.
            details: Details about the operation.
        """
        if operation not in self.report:
            self.report[operation] = []
        self.report[operation].append(details)

    # -------------------------------------------------------------------------
    # Missing Value Handling
    # -------------------------------------------------------------------------

    def check_missing(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate a report of missing values per column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with columns: column, missing_count, missing_pct, dtype.
        """
        total_rows = len(data)
        missing = data.isnull().sum()
        pct = (missing / total_rows * 100).round(2)

        report_df = pd.DataFrame({
            "column": data.columns,
            "missing_count": missing.values,
            "missing_pct": pct.values,
            "dtype": data.dtypes.values,
        })

        logger.info(
            "Missing value check: %d columns, %d total missing values",
            len(data.columns),
            int(missing.sum()),
        )
        return report_df

    def fill_missing(
        self,
        data: pd.DataFrame,
        strategy: str = "mean",
        fill_value: Optional[Any] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Fill missing values using a specified strategy.

        Args:
            data: Input DataFrame.
            strategy: Imputation strategy. One of:
                - 'mean': Fill with column mean (numeric only).
                - 'median': Fill with column median (numeric only).
                - 'mode': Fill with column mode.
                - 'constant': Fill with fill_value.
                - 'ffill': Forward fill.
                - 'bfill': Backward fill.
                - 'drop': Drop rows with missing values.
            fill_value: Value to use when strategy is 'constant'.
            columns: List of columns to apply imputation to (None for all).

        Returns:
            DataFrame with missing values handled.

        Raises:
            ValueError: If an unknown strategy is specified.
        """
        df = data.copy()
        target_cols = columns or df.columns.tolist()
        target_cols = [c for c in target_cols if c in df.columns]

        total_missing_before = int(df[target_cols].isnull().sum().sum())

        if total_missing_before == 0:
            logger.info("No missing values found in target columns")
            return df

        for col in target_cols:
            missing_count = int(df[col].isnull().sum())
            if missing_count == 0:
                continue

            if strategy == "mean":
                df[col].fillna(df[col].mean(), inplace=True)
            elif strategy == "median":
                df[col].fillna(df[col].median(), inplace=True)
            elif strategy == "mode":
                mode_val = df[col].mode()
                if not mode_val.empty:
                    df[col].fillna(mode_val.iloc[0], inplace=True)
            elif strategy == "constant":
                df[col].fillna(fill_value, inplace=True)
            elif strategy == "ffill":
                df[col].ffill(inplace=True)
            elif strategy == "bfill":
                df[col].bfill(inplace=True)
            elif strategy == "drop":
                df.dropna(subset=[col], inplace=True)
            else:
                raise ValueError(f"Unknown imputation strategy: {strategy}")

            logger.debug("Column '%s': filled %d missing values using '%s'", col, missing_count, strategy)

        total_missing_after = int(df[target_cols].isnull().sum().sum())

        self._record("fill_missing", {
            "strategy": strategy,
            "columns": target_cols,
            "missing_before": total_missing_before,
            "missing_after": total_missing_after,
            "values_filled": total_missing_before - total_missing_after,
        })

        logger.info(
            "Filled %d missing values using '%s' strategy",
            total_missing_before - total_missing_after,
            strategy,
        )
        return df

    def drop_missing(
        self,
        data: pd.DataFrame,
        threshold: float = 0.5,
        axis: int = 1,
    ) -> pd.DataFrame:
        """Drop columns or rows with missing values above a threshold.

        Args:
            data: Input DataFrame.
            threshold: Fraction of missing values required to drop (0.0 to 1.0).
            axis: 0 to drop rows, 1 to drop columns.

        Returns:
            DataFrame with high-missing rows/columns removed.
        """
        df = data.copy()
        before_shape = df.shape

        if axis == 1:
            # Drop columns
            missing_pct = df.isnull().sum() / len(df)
            to_drop = missing_pct[missing_pct > threshold].index.tolist()
            df.drop(columns=to_drop, inplace=True)
        else:
            # Drop rows
            missing_pct = df.isnull().sum(axis=1) / len(df.columns)
            df = df[missing_pct <= threshold].reset_index(drop=True)

        after_shape = df.shape

        self._record("drop_missing", {
            "threshold": threshold,
            "axis": axis,
            "before_shape": before_shape,
            "after_shape": after_shape,
        })

        logger.info(
            "Dropped %s with >%.0f%% missing: shape %s -> %s",
            "columns" if axis == 1 else "rows",
            threshold * 100,
            before_shape,
            after_shape,
        )
        return df

    # -------------------------------------------------------------------------
    # Duplicate Handling
    # -------------------------------------------------------------------------

    def check_duplicates(
        self,
        data: pd.DataFrame,
        subset: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Analyze duplicates in the DataFrame.

        Args:
            data: Input DataFrame.
            subset: Columns to consider for duplicate detection.

        Returns:
            Dictionary with duplicate statistics.
        """
        duplicates = data.duplicated(subset=subset, keep=False)
        dup_count = int(duplicates.sum())
        unique_dup_count = int(data[duplicates].drop_duplicates(subset=subset).shape[0]) if dup_count > 0 else 0

        result = {
            "total_rows": len(data),
            "duplicate_rows": dup_count,
            "unique_duplicate_groups": unique_dup_count,
            "duplicate_pct": round(dup_count / len(data) * 100, 2) if len(data) > 0 else 0.0,
        }

        logger.info(
            "Duplicate check: %d/%d duplicate rows (%.1f%%)",
            dup_count,
            len(data),
            result["duplicate_pct"],
        )
        return result

    def remove_duplicates(
        self,
        data: pd.DataFrame,
        subset: Optional[List[str]] = None,
        keep: str = "first",
    ) -> pd.DataFrame:
        """Remove duplicate rows from the DataFrame.

        Args:
            data: Input DataFrame.
            subset: Columns to consider for duplicate detection.
            keep: Which duplicates to keep ('first', 'last', or False for none).

        Returns:
            DataFrame with duplicates removed.
        """
        df = data.copy()
        rows_before = len(df)

        df.drop_duplicates(subset=subset, keep=keep, inplace=True)
        df.reset_index(drop=True, inplace=True)

        rows_after = len(df)
        rows_removed = rows_before - rows_after

        self._record("remove_duplicates", {
            "subset": subset,
            "keep": keep,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_removed,
        })

        logger.info("Removed %d duplicate rows (%d -> %d)", rows_removed, rows_before, rows_after)
        return df

    # -------------------------------------------------------------------------
    # Outlier Handling
    # -------------------------------------------------------------------------

    def detect_outliers_iqr(
        self,
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        iqr_multiplier: float = 1.5,
    ) -> Dict[str, Dict[str, Any]]:
        """Detect outliers using the Interquartile Range (IQR) method.

        Args:
            data: Input DataFrame.
            columns: Numeric columns to check (None for all numeric).
            iqr_multiplier: Multiplier for IQR to define outlier bounds.

        Returns:
            Dictionary mapping column names to outlier statistics:
                - lower_bound, upper_bound, outlier_count, outlier_pct
        """
        if columns is None:
            columns = data.select_dtypes(include=[np.number]).columns.tolist()

        results: Dict[str, Dict[str, Any]] = {}

        for col in columns:
            if col not in data.columns:
                continue

            q1 = data[col].quantile(0.25)
            q3 = data[col].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - iqr_multiplier * iqr
            upper = q3 + iqr_multiplier * iqr

            outlier_mask = (data[col] < lower) | (data[col] > upper)
            outlier_count = int(outlier_mask.sum())

            results[col] = {
                "lower_bound": round(lower, 4),
                "upper_bound": round(upper, 4),
                "q1": round(q1, 4),
                "q3": round(q3, 4),
                "iqr": round(iqr, 4),
                "outlier_count": outlier_count,
                "outlier_pct": round(outlier_count / len(data) * 100, 2),
            }

        total_outliers = sum(r["outlier_count"] for r in results.values())
        logger.info("IQR outlier detection: %d total outliers across %d columns", total_outliers, len(results))
        return results

    def detect_outliers_zscore(
        self,
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        threshold: float = 3.0,
    ) -> Dict[str, Dict[str, Any]]:
        """Detect outliers using the Z-score method.

        Args:
            data: Input DataFrame.
            columns: Numeric columns to check (None for all numeric).
            threshold: Z-score threshold for outlier classification.

        Returns:
            Dictionary mapping column names to outlier statistics.
        """
        if columns is None:
            columns = data.select_dtypes(include=[np.number]).columns.tolist()

        results: Dict[str, Dict[str, Any]] = {}

        for col in columns:
            if col not in data.columns:
                continue

            mean = data[col].mean()
            std = data[col].std()

            if std == 0:
                results[col] = {"outlier_count": 0, "outlier_pct": 0.0, "mean": mean, "std": std}
                continue

            z_scores = np.abs((data[col] - mean) / std)
            outlier_mask = z_scores > threshold
            outlier_count = int(outlier_mask.sum())

            results[col] = {
                "mean": round(mean, 4),
                "std": round(std, 4),
                "threshold": threshold,
                "outlier_count": outlier_count,
                "outlier_pct": round(outlier_count / len(data) * 100, 2),
            }

        total_outliers = sum(r["outlier_count"] for r in results.values())
        logger.info("Z-score outlier detection: %d total outliers across %d columns", total_outliers, len(results))
        return results

    def handle_outliers(
        self,
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        method: str = "iqr",
        strategy: str = "clip",
        iqr_multiplier: float = 1.5,
        zscore_threshold: float = 3.0,
    ) -> pd.DataFrame:
        """Handle outliers using the specified detection method and treatment strategy.

        Args:
            data: Input DataFrame.
            columns: Columns to process (None for all numeric).
            method: Detection method ('iqr' or 'zscore').
            strategy: Treatment strategy. One of:
                - 'clip': Clip values to the boundary.
                - 'remove': Remove rows containing outliers.
                - 'replace_median': Replace outliers with the column median.
            iqr_multiplier: IQR multiplier (used when method='iqr').
            zscore_threshold: Z-score threshold (used when method='zscore').

        Returns:
            DataFrame with outliers handled.

        Raises:
            ValueError: If an unknown method or strategy is specified.
        """
        df = data.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()

        total_outliers_handled = 0

        for col in columns:
            if col not in df.columns or not np.issubdtype(df[col].dtype, np.number):
                continue

            if method == "iqr":
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - iqr_multiplier * iqr
                upper = q3 + iqr_multiplier * iqr
            elif method == "zscore":
                mean = df[col].mean()
                std = df[col].std()
                if std == 0:
                    continue
                lower = mean - zscore_threshold * std
                upper = mean + zscore_threshold * std
            else:
                raise ValueError(f"Unknown outlier detection method: {method}")

            outlier_mask = (df[col] < lower) | (df[col] > upper)
            outlier_count = int(outlier_mask.sum())

            if outlier_count == 0:
                continue

            if strategy == "clip":
                df[col] = df[col].clip(lower=lower, upper=upper)
            elif strategy == "remove":
                df = df[~outlier_mask].reset_index(drop=True)
            elif strategy == "replace_median":
                median_val = df[col].median()
                df.loc[outlier_mask, col] = median_val
            else:
                raise ValueError(f"Unknown outlier strategy: {strategy}")

            total_outliers_handled += outlier_count
            logger.debug("Column '%s': handled %d outliers using '%s'/'%s'", col, outlier_count, method, strategy)

        self._record("handle_outliers", {
            "method": method,
            "strategy": strategy,
            "columns": columns,
            "total_outliers_handled": total_outliers_handled,
        })

        logger.info("Handled %d total outliers using %s/%s", total_outliers_handled, method, strategy)
        return df

    # -------------------------------------------------------------------------
    # General Cleaning
    # -------------------------------------------------------------------------

    def clean_column_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names: lowercase, strip whitespace, replace spaces.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with cleaned column names.
        """
        df = data.copy()
        original = list(df.columns)
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^a-z0-9_]", "", regex=True)
        )
        renamed = dict(zip(original, list(df.columns)))
        self._record("clean_column_names", {"renamed": renamed})
        return df

    def remove_empty_rows(self, data: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
        """Remove rows where most values are missing.

        Args:
            data: Input DataFrame.
            threshold: Fraction of missing values required to drop the row.

        Returns:
            DataFrame with empty rows removed.
        """
        df = data.copy()
        rows_before = len(df)
        missing_pct = df.isnull().sum(axis=1) / len(df.columns)
        df = df[missing_pct < threshold].reset_index(drop=True)
        rows_removed = rows_before - len(df)
        self._record("remove_empty_rows", {"rows_removed": rows_removed})
        logger.info("Removed %d mostly-empty rows", rows_removed)
        return df

    def get_report(self) -> Dict[str, Any]:
        """Return the complete cleaning report.

        Returns:
            Dictionary of all recorded cleaning operations.
        """
        return self.report

    def __repr__(self) -> str:
        ops_count = sum(len(v) for v in self.report.values())
        return f"DataCleaner(operations_performed={ops_count})"
