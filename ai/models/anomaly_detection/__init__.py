"""
Anomaly Detection Module for the Thunders BigDataSystem.

This package provides a scalable, high-throughput, and fault-tolerant framework
designed for real-time and batch anomaly detection over large-scale distributed
data streams. Built to integrate seamlessly with ecosystem components (e.g.,
Apache Spark, PySpark, Flink, and Ray), this module provides preprocessing,
statistical modeling, machine learning, and deep-learning-based anomaly detectors.

Key Components:
---------------
- `base`: Abstract Base Classes (ABCs) and contracts for all anomaly detection algorithms.
- `statistical`: Lightweight statistical and threshold-based anomaly detectors (Z-Score, IQR, MAD).
- `distance`: Distance- and density-based anomaly detectors (LOF, KNN, Mahalanobis).
- `isolation`: Tree-based ensemble anomaly detectors (Isolation Forest, Extended Isolation Forest).
- `deep_learning`: Neural-network-based detectors (Autoencoders, LSTMs, VAEs) for complex sequence/image telemetry.
- `pipeline`: Streaming and batch processing pipeline orchestrators with real-time scoring capabilities.
- `metrics`: Evaluation metrics optimized for imbalanced target distributions (PR-AUC, ROC-AUC, Precision@K).

Usage Example:
--------------
>>> from thunders.bigdatasystem.anomaly_detection import IsolationForestDetector, DetectionPipeline
>>> detector = IsolationForestDetector(contamination=0.01, n_estimators=100)
>>> pipeline = DetectionPipeline(detector=detector)
>>> pipeline.fit(spark_dataframe)
>>> anomaly_scores = pipeline.transform(spark_dataframe)
"""

import logging
import sys
from typing import List, Tuple, Final

# Module Versioning
__version__: Final[str] = "1.4.0"
__author__: Final[str] = "Thunders BigDataSystem Team"
__email__: Final[str] = "dev@thunders-bigdatasystem.org"

# Initialize package-level logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Lazy import mechanism / Safe explicit imports
try:
    from .base import BaseAnomalyDetector, DetectionResult
    from .statistical import ZScoreDetector, IQRDetector, MADDetector
    from .distance import LocalOutlierFactor, MahalanobisDetector
    from .isolation import IsolationForestDetector
    from .pipeline import DetectionPipeline
    from .metrics import AnomalyEvaluator
except ImportError as err:
    logger.warning(f"Failed to load some anomaly detection submodules: {err}")

# Explicit API Exports
__all__: Tuple[str, ...] = (
    "__version__",
    "__author__",
    "__email__",
    # Base Interfaces
    "BaseAnomalyDetector",
    "DetectionResult",
    # Statistical Methods
    "ZScoreDetector",
    "IQRDetector",
    "MADDetector",
    # Distance/Density Methods
    "LocalOutlierFactor",
    "MahalanobisDetector",
    # Ensemble Methods
    "IsolationForestDetector",
    # Pipeline & Utilities
    "DetectionPipeline",
    "AnomalyEvaluator",
)


def get_available_detectors() -> List[str]:
    """
    Retrieve the list of registered and active anomaly detection models in the system.

    Returns:
        List[str]: Names of currently available detector classes.
    """
    return [
        cls for cls in __all__ 
        if cls.endswith("Detector") or cls == "LocalOutlierFactor"
    ]


logger.debug(f"Initialized anomaly_detection package v{__version__} successfully.")
