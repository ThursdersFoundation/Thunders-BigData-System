"""Thunders BigData System - AI - Dataset loader."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Dataset loader."""

import pandas as pd


def load_dataset(path: str):
    df = pd.read_parquet(path)
    X = df.drop(columns=["label"]).values
    y = df["label"].values
    return X, y
