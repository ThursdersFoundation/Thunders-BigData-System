"""
===============================================================================
Thunders BigDataSystem - Forecasting Engine Package (__init__.py)
===============================================================================

This package provides a comprehensive, scalable time-series forecasting 
framework tailored for high-throughput distributed data pipelines within the 
Thunders BigDataSystem ecosystem. 

Architecture Overview:
----------------------
1. Data Abstraction & Preprocessing (`base`, `transforms`)
   - Standardized time-series schema definitions (Spark/Polars/Pandas compatibility).
   - Windowing, feature extraction, and temporal decomposition tools.

2. Model Interfaces (`models`)
   - Statistical Models: ARIMA, AutoARIMA, Holt-Winters Exponential Smoothing.
   - Machine Learning: XGBoost, LightGBM, CatBoost with lag features.
   - Deep Learning: LSTM, Temporal Fusion Transformer (TFT), N-BEATS, N-HiTS.
   - Distributed Scale: Spark MLlib and Ray/Dask wrappers for multi-node fitting.

3. Pipelines & Ensembling (`pipelines`, `ensembles`)
   - End-to-end forecasting pipelines.
   - Weighted averaging, stacking, and dynamic model selection.

4. Evaluation & Metrics (`metrics`, `evaluation`)
   - Time-series cross-validation (rolling/expanding window).
   - Accuracy metrics: MAE, RMSE, MAPE, SMAPE, MASE, WAPE, Pinball Loss (Quantiles).

5. MLOps & Experiment Tracking (`tracking`, `registry`)
   - Seamless integration with MLflow, MLflow Model Registry, and S3 artifacts.

Author: Thunders BigDataSystem Core Engineering Team
License: Apache License 2.0
===============================================================================
"""

import logging
import sys
import importlib
from typing import List, Optional

# Set up logging for the forecasting package
logger = logging.getLogger("ThundersBigData.Forecasting")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [Thunders.Forecasting] - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# =============================================================================
# PACKAGE METADATA
# =============================================================================
__title__ = "thunders-bigdatasystem-forecasting"
__version__ = "1.4.0"
__author__ = "Thunders BigData System Engineering Team"
__license__ = "Apache-2.0"
__copyright__ = "Copyright 2026 Thunders BigData System"

# =============================================================================
# EXPLICIT IMPORTS (Core Functionality & Base Classes)
# =============================================================================
def _load_component(module_name: str, attribute: str):
    """Load a package component without requiring it during static analysis."""
    try:
        module = importlib.import_module(module_name, package=__name__)
        return getattr(module, attribute)
    except (ImportError, AttributeError) as err:
        logger.warning("Unable to load %s from %s: %s", attribute, module_name, err)
        return None


BaseForecaster = _load_component(".base", "BaseForecaster")
TimeSeriesDataset = _load_component(".base", "TimeSeriesDataset")
ForecastResult = _load_component(".base", "ForecastResult")
ModelConfig = _load_component(".base", "ModelConfig")
ForecastingPipeline = _load_component(".pipeline", "ForecastingPipeline")
CrossValidator = _load_component(".evaluator", "CrossValidator")
Evaluator = _load_component(".evaluator", "Evaluator")
ModelRegistry = _load_component(".registry", "ModelRegistry")

# =============================================================================
# OPTIONAL DEPENDENCIES AND CONDITIONAL EXPORTS
# =============================================================================

# 1. Statistical & Classical ML Models
_HAS_STATS = False
try:
    _stats_module = importlib.import_module(".models.statistical", package=__name__)
    AutoARIMA = _stats_module.AutoARIMA
    ProphetForecaster = _stats_module.ProphetForecaster
    ExponentialSmoothing = _stats_module.ExponentialSmoothing
    _HAS_STATS = True
except ImportError:
    logger.warning(
        "Statistical forecasting models (Prophet/Statsmodels) could not be loaded. "
        "Install missing dependencies via `pip install thunders-bigdatasystem[stats]`."
    )

# 2. Deep Learning Models (PyTorch / PyTorch Forecasting)
_HAS_TORCH = False
try:
    _torch_module = importlib.import_module(".models.deep_learning", package=__name__)
    LSTMForecaster = _torch_module.LSTMForecaster
    TemporalFusionTransformer = _torch_module.TemporalFusionTransformer
    NBEATSModel = _torch_module.NBEATSModel
    Training_scout = _torch_module.Training_scout
    _HAS_TORCH = True
except ImportError:
    logger.warning(
        "Deep Learning models (PyTorch) could not be loaded. "
        "Install PyTorch support via `pip install thunders-bigdatasystem[torch]`."
    )

# 3. Distributed Engine Integration (PySpark / Ray)
_HAS_DISTRIBUTED = False
try:
    _spark_module = importlib.import_module(
        ".distributed.spark_forecaster", package=__name__
    )
    _ray_module = importlib.import_module(
        ".distributed.ray_engine", package=__name__
    )
    SparkDistributedForecaster = _spark_module.SparkDistributedForecaster
    RayParallelTrainer = _ray_module.RayParallelTrainer
    _HAS_DISTRIBUTED = True
except ImportError:
    logger.debug(
        "Distributed engines (Spark/Ray) not initialized in local context."
    )

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_version() -> str:
    """Return the current version of the Thunders Forecasting engine."""
    return __version__


def list_available_models() -> List[str]:
    """
    Dynamically inspect and list all forecasting models currently 
    available in the active environment.
    """
    models = ["NaiveForecaster", "LinearRegressionForecaster", "XGBoostForecaster"]
    if _HAS_STATS:
        models.extend(["AutoARIMA", "ProphetForecaster", "ExponentialSmoothing"])
    if _HAS_TORCH:
        models.extend(["LSTMForecaster", "TemporalFusionTransformer", "NBEATSModel"])
    if _HAS_DISTRIBUTED:
        models.extend(["SparkDistributedForecaster", "RayParallelTrainer"])
    return models


def check_environment() -> dict:
    """
    Returns diagnostic information regarding backend frameworks and availability.
    """
    return {
        "package_version": __version__,
        "statistical_backends_available": _HAS_STATS,
        "pytorch_backends_available": _HAS_TORCH,
        "distributed_engine_available": _HAS_DISTRIBUTED,
        "python_version": sys.version,
    }

# =============================================================================
# ALL EXPORTS DEFINITION (__all__)
# =============================================================================
# Define public interface for clean `from thunders.forecasting import *` usage

__all__ = [
    # Package Metadata
    "__title__",
    "__version__",
    "__author__",
    "__license__",
    
    # Core Infrastructure
    "BaseForecaster",
    "TimeSeriesDataset",
    "ForecastResult",
    "ModelConfig",
    "ForecastingPipeline",
    "CrossValidator",
    "Evaluator",
    "ModelRegistry",
    
    # Environment Helpers
    "get_version",
    "list_available_models",
    "check_environment",
]

# Append conditional modules to __all__ if present
if _HAS_STATS:
    __all__.extend(["AutoARIMA", "ProphetForecaster", "ExponentialSmoothing"])

if _HAS_TORCH:
    __all__.extend(["LSTMForecaster", "TemporalFusionTransformer", "NBEATSModel"])

if _HAS_DISTRIBUTED:
    __all__.extend(["SparkDistributedForecaster", "RayParallelTrainer"])

# Clean up module namespace
del sys, Optional, importlib
