"""Thunders BigData System - AI - Model serving."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""FastAPI app that serves predictions."""

from fastapi import FastAPI

app = FastAPI()


@app.post("/predict")
def predict(payload: dict) -> dict:
    # TODO: load model and run inference
    return {"prediction": "TODO"}
