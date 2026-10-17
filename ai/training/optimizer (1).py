"""Thunders BigData System - AI - Optimizer factory."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Hyperparameter optimizer using Optuna."""

import optuna


def optimize(objective, n_trials: int = 50) -> dict:
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials)
    return study.best_params
