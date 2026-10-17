"""Thunders BigData System - AI - LLM chatbot."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""LLM chatbot facade."""

def chat(message: str, history: list[dict] | None = None) -> str:
    # TODO: integrate with an LLM provider
    return f"echo: {message}"
