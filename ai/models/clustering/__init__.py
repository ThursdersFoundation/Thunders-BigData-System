"""
================================================================================
Thunders BigDataSystem - Clustering Engine Package
================================================================================

This package (`thunders.analytics.clustering`) provides high-performance,
distributed, and scalable clustering algorithms designed for massive-scale
datasets processed across the Thunders BigDataSystem ecosystem.

The module integrates seamlessly with PySpark DataFrames, Ray clusters, and
distributed NumPy/CuPy arrays. It includes native implementations of spatial,
density-based, hierarchical, and centroid-based clustering models optimized
for high-throughput vector processing.

Module Architecture:
--------------------
- `Centroid`    : K-Means, K-Medoids, K-Means++ Initialization
- `Density`     : Distributed DBSCAN, HDBSCAN*, OPTICS
- `Hierarchical`: Agglomerative Clustering with Ward / Complete / Average linkage
- `Evaluation`  : Silhouette Score, Davies-Bouldin Index, Calinski-Harabasz Index
- `Pipeline`    : End-to-end Feature Scaling + Dimensionality Reduction + Clustering Pipeline

Author: Thunders BigDataSystem Engineering Team
License: Apache License 2.0
================================================================================
"""

import logging
import importlib
from typing import List, Any, Optional, Union

# Module Versioning
__version__ = "2.4.0"
__author__ = "Thunders BigDataSystem Core Team"
__email__ = "core-eng@thunders-bigdata.org"

# Initialize package-level logger
logger = logging.getLogger("thunders.analytics.clustering")
logger.addHandler(logging.NullHandler())


# Check optional high-performance dependencies
_GPU_AVAILABLE = False
try:
    importlib.import_module("cupy")
    importlib.import_module("cuml")
    _GPU_AVAILABLE = True
    logger.info("Thunders Clustering: GPU acceleration enabled via RAPIDS cuML.")
except ImportError:
    logger.info("Thunders Clustering: Running in CPU-only mode.")


# ------------------------------------------------------------------------------
# Core Model Imports
# ------------------------------------------------------------------------------
try:
    _package = "thunders.analytics.clustering"

    _kmeans = importlib.import_module(f"{_package}.kmeans")
    KMeans = _kmeans.KMeans
    KMedoids = _kmeans.KMedoids
    MiniBatchKMeans = _kmeans.MiniBatchKMeans
    DistributedKMeans = _kmeans.DistributedKMeans

    _density = importlib.import_module(f"{_package}.density")
    DBSCAN = _density.DBSCAN
    HDBSCAN = _density.HDBSCAN
    OPTICS = _density.OPTICS

    _hierarchical = importlib.import_module(f"{_package}.hierarchical")
    AgglomerativeClustering = _hierarchical.AgglomerativeClustering
    HierarchicalTree = _hierarchical.HierarchicalTree

    _evaluation = importlib.import_module(f"{_package}.evaluation")
    SilhouetteEvaluator = _evaluation.SilhouetteEvaluator
    DaviesBouldinEvaluator = _evaluation.DaviesBouldinEvaluator
    CalinskiHarabaszEvaluator = _evaluation.CalinskiHarabaszEvaluator
    ClusteringMetrics = _evaluation.ClusteringMetrics

    _pipeline = importlib.import_module(f"{_package}.pipeline")
    ClusteringPipeline = _pipeline.ClusteringPipeline
    ClusteringPipelineBuilder = _pipeline.ClusteringPipelineBuilder

    _exceptions = importlib.import_module(f"{_package}.exceptions")
    ClusteringException = _exceptions.ClusteringException
    ConvergenceError = _exceptions.ConvergenceError
    InvalidDimensionError = _exceptions.InvalidDimensionError
except ImportError as err:
    logger.critical(
        f"Failed to import internal modules in Thunders Clustering package: {err}"
    )
    raise ImportError(
        "Error loading Thunders BigDataSystem clustering components. "
        "Ensure all submodules (kmeans, density, hierarchical, evaluation, pipeline) "
        "are installed properly."
    ) from err


# ------------------------------------------------------------------------------
# Convenience Factory Methods
# ------------------------------------------------------------------------------
def get_clustering_model(
    algorithm: str,
    n_clusters: Optional[int] = None,
    use_gpu: bool = False,
    **kwargs: Any
) -> Union[KMeans, DBSCAN, HDBSCAN, AgglomerativeClustering]:
    """
    Factory function to instantiate clustering algorithms by string name.

    Parameters
    ----------
    algorithm : str
        The algorithm name ('kmeans', 'minibatch_kmeans', 'dbscan', 'hdbscan', 'agglomerative').
    n_clusters : int, optional
        Target number of clusters (required for K-Means / Agglomerative).
    use_gpu : bool, default=False
        If True, attempts to use GPU-accelerated implementations.
    **kwargs : Any
        Additional hyperparameter arguments passed to the algorithm's constructor.

    Returns
    -------
    Union[KMeans, DBSCAN, HDBSCAN, AgglomerativeClustering]
        An initialized model instance ready for .fit() or .fit_predict().

    Raises
    ------
    ValueError
        If the algorithm name is unknown or missing required parameters.
    RuntimeError
        If GPU mode is requested but GPU libraries are unavailable.
    """
    algorithm_lower = algorithm.strip().lower()

    if use_gpu and not _GPU_AVAILABLE:
        raise RuntimeError(
            f"GPU execution requested for '{algorithm}', but RAPIDS/cuML "
            "is not installed or configured in this environment."
        )

    if algorithm_lower == "kmeans":
        if n_clusters is None:
            raise ValueError("Parameter 'n_clusters' is required for KMeans.")
        return KMeans(n_clusters=n_clusters, use_gpu=use_gpu, **kwargs)

    elif algorithm_lower in ["minibatch_kmeans", "minibatchkmeans"]:
        if n_clusters is None:
            raise ValueError("Parameter 'n_clusters' is required for MiniBatchKMeans.")
        return MiniBatchKMeans(n_clusters=n_clusters, **kwargs)

    elif algorithm_lower == "dbscan":
        return DBSCAN(use_gpu=use_gpu, **kwargs)

    elif algorithm_lower == "hdbscan":
        return HDBSCAN(use_gpu=use_gpu, **kwargs)

    elif algorithm_lower in ["agglomerative", "hierarchical"]:
        if n_clusters is None:
            raise ValueError("Parameter 'n_clusters' is required for AgglomerativeClustering.")
        return AgglomerativeClustering(n_clusters=n_clusters, **kwargs)

    else:
        supported = ["kmeans", "minibatch_kmeans", "dbscan", "hdbscan", "agglomerative"]
        raise ValueError(
            f"Unknown clustering algorithm '{algorithm}'. "
            f"Supported algorithms are: {', '.join(supported)}"
        )


def list_available_algorithms() -> List[str]:
    """
    Returns a list of all supported clustering algorithm identifiers in the package.
    """
    return [
        "KMeans",
        "MiniBatchKMeans",
        "DistributedKMeans",
        "KMedoids",
        "DBSCAN",
        "HDBSCAN",
        "OPTICS",
        "AgglomerativeClustering",
    ]


# ------------------------------------------------------------------------------
# Package Public Interface (__all__)
# ------------------------------------------------------------------------------
__all__: List[str] = [
    # Models
    "KMeans",
    "KMedoids",
    "MiniBatchKMeans",
    "DistributedKMeans",
    "DBSCAN",
    "HDBSCAN",
    "OPTICS",
    "AgglomerativeClustering",
    "HierarchicalTree",
    # Evaluation
    "SilhouetteEvaluator",
    "DaviesBouldinEvaluator",
    "CalinskiHarabaszEvaluator",
    "ClusteringMetrics",
    # Pipeline & Helpers
    "ClusteringPipeline",
    "ClusteringPipelineBuilder",
    "get_clustering_model",
    "list_available_algorithms",
    # Exceptions
    "ClusteringException",
    "ConvergenceError",
    "InvalidDimensionError",
    # Metadata
    "__version__",
    "__author__",
]
