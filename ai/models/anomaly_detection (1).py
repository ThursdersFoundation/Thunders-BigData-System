"""Thunders BigData System - AI - Anomaly detection."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Isolation Forest-based anomaly detection."""

import numpy as np
from sklearn.ensemble import IsolationForest


class AnomalyDetector:
    def __init__(self, contamination: float = 0.01) -> None:
        self.model = IsolationForest(contamination=contamination)

    def fit(self, X: np.ndarray) -> "AnomalyDetector":
        self.model.fit(X)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)
