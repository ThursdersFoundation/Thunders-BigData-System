"""
Data Transformation Module for Thunders BigData System.

Provides data transformation operations including normalization, encoding,
aggregation, and pivoting for analytics and machine learning workflows.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from sklearn.preprocessing import (
    LabelEncoder,
    MinMaxScaler,
    OneHotEncoder,
    StandardScaler,
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms data through normalization, encoding, aggregation, and reshaping.

    Maintains fitted transformers for consistent application across
    training and inference datasets.

    Attributes:
        scalers: Dictionary of fitted scalers keyed by column name.
        encoders: Dictionary of fitted encoders keyed by column name.
    """

    def __init__(self) -> None:
        """Initialize the DataTransformer."""
        self.scalers: Dict[str, Any] = {}
        self.encoders: Dict[str, Any] = {}
        self._transform_log: List[Dict[str, Any]] = []

    def _log(self, operation: str, details: Dict[str, Any]) -> None:
        """Record a transformation operation."""
        self._transform_log.append({"operation": operation, **details})

    # -------------------------------------------------------------------------
    # Normalization / Scaling
    # -------------------------------------------------------------------------

    def normalize(
        self,
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        method: str = "standard",
        fit: bool = True,
    ) -> pd.DataFrame:
        """Normalize numeric columns using the specified method.

        Args:
            data: Input DataFrame.
            columns: Columns to normalize (None for all numeric).
            method: Normalization method:
                - 'standard': Zero mean, unit variance (Z-score).
                - 'minmax': Scale to [0, 1] range.
                - 'robust': Scale using median and IQR.
                - 'log': Log1p transformation.
                - 'log1p': Log1p transformation (alias for 'log').
            fit: Whether to fit the scaler on this data (True for training,
                False for applying a pre-fitted scaler).

        Returns:
            DataFrame with normalized columns.

        Raises:
            ValueError: If an unknown method is specified.
        """
        df = data.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in columns:
            if col not in df.columns:
                continue

            if method in ("standard", "minmax", "robust"):
                scaler_key = f"{col}_{method}"
                if fit:
                    if method == "standard":
                        scaler = StandardScaler()
                    elif method == "minmax":
                        scaler = MinMaxScaler()
                    else:
                        from sklearn.preprocessing import RobustScaler
                        scaler = RobustScaler()

                    df[col] = scaler.fit_transform(df[[col]]).flatten()
                    self.scalers[scaler_key] = scaler
                else:
                    scaler = self.scalers.get(scaler_key)
                    if scaler is None:
                        raise ValueError(f"No fitted scaler found for '{scaler_key}'. Call with fit=True first.")
                    df[col] = scaler.transform(df[[col]]).flatten()

            elif method in ("log", "log1p"):
                df[col] = np.log1p(df[col].clip(lower=0))

            else:
                raise ValueError(f"Unknown normalization method: {method}")

        self._log("normalize", {"method": method, "columns": columns, "fit": fit})
        logger.info("Normalized %d columns using '%s' method", len(columns), method)
        return df

    def power_transform(
        self,
        data: pd.DataFrame,
        columns: List[str],
        method: str = "yeo-johnson",
    ) -> pd.DataFrame:
        """Apply a power transformation to make data more Gaussian-like.

        Args:
            data: Input DataFrame.
            columns: Columns to transform.
            method: Power transform method ('yeo-johnson' or 'box-cox').

        Returns:
            DataFrame with power-transformed columns.
        """
        from sklearn.preprocessing import PowerTransformer

        df = data.copy()
        pt = PowerTransformer(method=method)

        for col in columns:
            valid_mask = df[col].notna()
            if valid_mask.sum() == 0:
                continue
            df.loc[valid_mask, col] = pt.fit_transform(df.loc[valid_mask, [col]]).flatten()
            self.scalers[f"{col}_power_{method}"] = pt

        self._log("power_transform", {"method": method, "columns": columns})
        logger.info("Power-transformed %d columns using '%s'", len(columns), method)
        return df

    # -------------------------------------------------------------------------
    # Encoding
    # -------------------------------------------------------------------------

    def one_hot_encode(
        self,
        data: pd.DataFrame,
        columns: List[str],
        drop_first: bool = False,
        prefix: Optional[str] = None,
    ) -> pd.DataFrame:
        """One-hot encode categorical columns.

        Args:
            data: Input DataFrame.
            columns: Categorical columns to encode.
            drop_first: Whether to drop the first category to avoid collinearity.
            prefix: Prefix for the new dummy columns.

        Returns:
            DataFrame with one-hot encoded columns replacing the originals.
        """
        df = data.copy()
        encoded_dfs = []

        for col in columns:
            dummies = pd.get_dummies(df[col], prefix=prefix or col, drop_first=drop_first, dtype=int)
            encoded_dfs.append(dummies)

            encoder = OneHotEncoder(sparse_output=False, drop="first" if drop_first else None, handle_unknown="ignore")
            encoder.fit(df[[col]].astype(str))
            self.encoders[f"{col}_onehot"] = encoder

        df = df.drop(columns=columns)
        if encoded_dfs:
            df = pd.concat([df] + encoded_dfs, axis=1)

        self._log("one_hot_encode", {"columns": columns, "drop_first": drop_first})
        logger.info("One-hot encoded %d columns", len(columns))
        return df

    def label_encode(
        self,
        data: pd.DataFrame,
        columns: List[str],
    ) -> pd.DataFrame:
        """Label encode categorical columns to integer values.

        Args:
            data: Input DataFrame.
            columns: Categorical columns to encode.

        Returns:
            DataFrame with label-encoded columns.
        """
        df = data.copy()

        for col in columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            self.encoders[f"{col}_label"] = le

        self._log("label_encode", {"columns": columns})
        logger.info("Label-encoded %d columns", len(columns))
        return df

    def ordinal_encode(
        self,
        data: pd.DataFrame,
        columns: List[str],
        categories: Optional[Dict[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Ordinal encode categorical columns with a specified order.

        Args:
            data: Input DataFrame.
            columns: Categorical columns to encode.
            categories: Dict mapping column names to ordered category lists.
                If not provided, categories are inferred alphabetically.

        Returns:
            DataFrame with ordinally-encoded columns.
        """
        df = data.copy()

        for col in columns:
            cat_order = categories.get(col) if categories else None
            if cat_order is None:
                cat_order = sorted(df[col].dropna().unique().tolist())

            mapping = {val: idx for idx, val in enumerate(cat_order)}
            df[col] = df[col].map(mapping)
            self.encoders[f"{col}_ordinal"] = {"mapping": mapping, "categories": cat_order}

        self._log("ordinal_encode", {"columns": columns, "categories": categories})
        logger.info("Ordinal-encoded %d columns", len(columns))
        return df

    # -------------------------------------------------------------------------
    # Aggregation
    # -------------------------------------------------------------------------

    def aggregate(
        self,
        data: pd.DataFrame,
        group_by: Union[str, List[str]],
        aggregations: Dict[str, Union[str, List[str]]],
    ) -> pd.DataFrame:
        """Aggregate data by grouping columns with specified functions.

        Args:
            data: Input DataFrame.
            group_by: Column(s) to group by.
            aggregations: Dict mapping column names to aggregation functions
                or lists of functions. E.g., {'revenue': ['sum', 'mean'], 'quantity': 'sum'}.

        Returns:
            Aggregated DataFrame with MultiIndex columns flattened.

        Example:
            >>> result = transformer.aggregate(
            ...     df,
            ...     group_by="region",
            ...     aggregations={"revenue": ["sum", "mean"], "orders": "count"},
            ... )
        """
        grouped = data.groupby(group_by, as_index=False).agg(aggregations)

        # Flatten MultiIndex columns if present
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = [
                f"{col[0]}_{col[1]}" if col[1] else col[0]
                for col in grouped.columns
            ]

        self._log("aggregate", {"group_by": group_by, "aggregations": aggregations})
        logger.info("Aggregated data by %s", group_by)
        return grouped

    def pivot_table(
        self,
        data: pd.DataFrame,
        index: Union[str, List[str]],
        columns: Union[str, List[str]],
        values: str,
        aggfunc: str = "sum",
        fill_value: Any = 0,
    ) -> pd.DataFrame:
        """Create a pivot table from the data.

        Args:
            data: Input DataFrame.
            index: Column(s) for the pivot table rows.
            columns: Column(s) for the pivot table columns.
            values: Column to aggregate.
            aggfunc: Aggregation function.
            fill_value: Value to fill missing entries.

        Returns:
            Pivoted DataFrame.
        """
        result = data.pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=fill_value,
        ).reset_index()

        # Flatten column MultiIndex
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [
                "_".join(str(c) for c in col).strip("_") for col in result.columns
            ]

        self._log("pivot_table", {
            "index": index, "columns": columns, "values": values, "aggfunc": aggfunc,
        })
        logger.info("Created pivot table: index=%s, columns=%s", index, columns)
        return result

    def melt(
        self,
        data: pd.DataFrame,
        id_vars: List[str],
        value_vars: Optional[List[str]] = None,
        var_name: str = "variable",
        value_name: str = "value",
    ) -> pd.DataFrame:
        """Unpivot (melt) a DataFrame from wide to long format.

        Args:
            data: Input DataFrame.
            id_vars: Columns to keep as identifier variables.
            value_vars: Columns to unpivot (None for all non-id columns).
            var_name: Name for the variable column.
            value_name: Name for the value column.

        Returns:
            Melted (long-format) DataFrame.
        """
        result = data.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )

        self._log("melt", {"id_vars": id_vars, "var_name": var_name, "value_name": value_name})
        logger.info("Melted DataFrame: %d rows -> %d rows", len(data), len(result))
        return result

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def apply_custom(
        self,
        data: pd.DataFrame,
        column: str,
        func: Callable,
        output_column: Optional[str] = None,
    ) -> pd.DataFrame:
        """Apply a custom transformation function to a column.

        Args:
            data: Input DataFrame.
            column: Column to transform.
            func: Transformation function.
            output_column: Name for the transformed column (defaults to same column).

        Returns:
            DataFrame with the custom transformation applied.
        """
        df = data.copy()
        out_col = output_column or column
        df[out_col] = df[column].apply(func)

        self._log("apply_custom", {"column": column, "output_column": out_col})
        logger.info("Applied custom transformation to column '%s'", column)
        return df

    def get_transform_log(self) -> List[Dict[str, Any]]:
        """Return the log of all transformation operations."""
        return self._transform_log

    def __repr__(self) -> str:
        return (
            f"DataTransformer(scalers={len(self.scalers)}, "
            f"encoders={len(self.encoders)}, "
            f"operations={len(self._transform_log)})"
        )
