"""Thunders BigData System - AI - Recommendation."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Collaborative-filtering recommendation."""

import numpy as np


class Recommender:
    def __init__(self) -> None:
        self.user_items: dict[str, set[str]] = {}

    def add_interaction(self, user: str, item: str) -> None:
        self.user_items.setdefault(user, set()).add(item)

    def recommend(self, user: str, top_k: int = 5) -> list[str]:
        # TODO: replace with a trained model
        return list(self.user_items.get(user, set()))[:top_k]
