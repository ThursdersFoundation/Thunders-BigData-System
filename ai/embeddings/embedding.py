"""Thunders BigData System - AI - Embeddings."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Embedding generation."""

import numpy as np


def embed(text: str) -> np.ndarray:
    # TODO: replace with a real embedding model
    return np.zeros(384, dtype="float32")
