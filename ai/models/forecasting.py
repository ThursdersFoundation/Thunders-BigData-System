"""Thunders BigData System - AI - Forecasting."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Time-series forecasting."""

import numpy as np
from sklearn.linear_model import LinearRegression


class Forecaster:
    def __init__(self) -> None:
        self.model = LinearRegression()

    def fit(self, X: np.ndarray, y: np.ndarray) -> "Forecaster":
        self.model.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)
