"""Thunders BigData System - AI - Evaluation report."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Render an evaluation report."""

import json


def render(metrics: dict) -> str:
    return json.dumps(metrics, indent=2, sort_keys=True)
