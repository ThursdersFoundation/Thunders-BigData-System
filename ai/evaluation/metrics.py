"""Thunders BigData System - AI - Metrics."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Evaluation metrics."""

from sklearn.metrics import accuracy_score, f1_score, mean_squared_error


def classification_metrics(y_true, y_pred) -> dict:
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred, average="weighted"),
    }


def regression_metrics(y_true, y_pred) -> dict:
    return {"mse": mean_squared_error(y_true, y_pred)}
