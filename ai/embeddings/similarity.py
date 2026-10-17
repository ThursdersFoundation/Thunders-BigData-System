"""Thunders BigData System - AI - Similarity search."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Similarity search facade used by the RAG pipeline."""

from ai.embeddings.embedding import embed
from ai.embeddings.vector_store import VectorStore

_store = VectorStore()


def search(text: str, top_k: int = 5) -> list[dict]:
    return _store.search(embed(text), top_k=top_k)
