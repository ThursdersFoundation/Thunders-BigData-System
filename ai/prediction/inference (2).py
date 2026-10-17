"""Thunders BigData System - AI - Inference helpers."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Inference helpers."""

import pickle


def load_model(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


def predict(model, input_path: str):
    import pandas as pd
    X = pd.read_parquet(input_path).values
    return model.predict(X).tolist()
