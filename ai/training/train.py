"""Thunders BigData System - AI - Train entrypoint."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Training entrypoint used by the trainer CLI."""

from ai.training.trainer import Trainer
from ai.training.dataset import load_dataset


def main(dataset_path: str, model_out: str) -> None:
    X, y = load_dataset(dataset_path)
    trainer = Trainer()
    trainer.fit(X, y)
    trainer.save(model_out)


if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])
