"""Thunders BigData System - AI - Vector store."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Vector store wrapper (FAISS / pgvector / Pinecone)."""

import numpy as np


class VectorStore:
    def __init__(self) -> None:
        self.vectors: list[np.ndarray] = []
        self.metadata: list[dict] = []

    def add(self, vector: np.ndarray, meta: dict) -> None:
        self.vectors.append(vector)
        self.metadata.append(meta)

    def search(self, query: np.ndarray, top_k: int = 5) -> list[dict]:
        # TODO: replace brute-force with FAISS or pgvector
        return self.metadata[:top_k]
