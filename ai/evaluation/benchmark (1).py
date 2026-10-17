"""Thunders BigData System - AI - Benchmark."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


"""Benchmark runner."""

import time


def benchmark(fn, *args, repeat: int = 10) -> dict:
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn(*args)
        times.append(time.perf_counter() - start)
    return {"avg_ms": sum(times) / repeat * 1000, "max_ms": max(times) * 1000}
