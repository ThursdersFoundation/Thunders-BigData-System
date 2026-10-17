"""Thunders BigData System - AI - Classification."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Gradient-boosted classification."""

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier


class Classifier:
    def __init__(self) -> None:
        self.model = GradientBoostingClassifier()

    def fit(self, X: np.ndarray, y: np.ndarray) -> "Classifier":
        self.model.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)
