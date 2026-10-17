"""Thunders BigData System - AI - Predict CLI."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""CLI entrypoint to run a prediction."""

import sys
from ai.prediction.inference import load_model, predict


def main(model_path: str, input_path: str) -> None:
    model = load_model(model_path)
    result = predict(model, input_path)
    print(result)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
