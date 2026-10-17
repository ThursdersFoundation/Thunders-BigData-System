"""Thunders BigData System - AI - Trainer."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Generic model trainer."""

import pickle
from sklearn.ensemble import GradientBoostingClassifier


class Trainer:
    def __init__(self) -> None:
        self.model = GradientBoostingClassifier()

    def fit(self, X, y) -> "Trainer":
        self.model.fit(X, y)
        return self

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump(self.model, f)
