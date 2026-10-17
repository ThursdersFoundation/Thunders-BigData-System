"""Thunders BigData System - AI - Retrieval-augmented generation."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""RAG pipeline: retrieve relevant chunks and pass to the LLM."""

from ai.embeddings.similarity import search


def answer(question: str, top_k: int = 3) -> str:
    chunks = search(question, top_k=top_k)
    context = "\n".join(c["text"] for c in chunks)
    # TODO: call LLM with context + question
    return context or "no relevant context found"
