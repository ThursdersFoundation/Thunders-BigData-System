"""
ETL Pipeline Module for Thunders BigData System.

Provides a flexible Extract-Transform-Load pipeline framework with
composable stages, parallel execution, and comprehensive error handling.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """Abstract base class for ETL pipeline stages.

    Each stage represents a discrete step in the ETL process and must
    implement the process method. Stages can be chained together to
    form a complete pipeline.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        """Initialize the pipeline stage.

        Args:
            name: Human-readable name for this stage.
        """
        self.name = name or self.__class__.__name__
        self._execution_time: float = 0.0
        self._input_rows: int = 0
        self._output_rows: int = 0

    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the input data and return transformed data.

        Args:
            data: Input DataFrame.

        Returns:
            Transformed DataFrame.
        """
        ...

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Execute the stage with timing and logging.

        Args:
            data: Input DataFrame.

        Returns:
            Transformed DataFrame.
        """
        self._input_rows = len(data)
        start_time = time.time()

        logger.info("Executing stage: %s (%d input rows)", self.name, self._input_rows)
        result = self.process(data)

        self._execution_time = time.time() - start_time
        self._output_rows = len(result)

        logger.info(
            "Stage '%s' complete: %d -> %d rows (%.2fs)",
            self.name,
            self._input_rows,
            self._output_rows,
            self._execution_time,
        )
        return result

    @property
    def stats(self) -> Dict[str, Any]:
        """Return execution statistics for this stage."""
        return {
            "name": self.name,
            "input_rows": self._input_rows,
            "output_rows": self._output_rows,
            "execution_time_sec": round(self._execution_time, 3),
            "row_change": self._output_rows - self._input_rows,
        }


class ExtractStage(PipelineStage):
    """Stage that extracts data from a source and loads it into a DataFrame."""

    def __init__(
        self,
        extractor: Callable[[], pd.DataFrame],
        name: str = "extract",
    ) -> None:
        """Initialize the extract stage.

        Args:
            extractor: Callable that returns a DataFrame.
            name: Stage name.
        """
        super().__init__(name=name)
        self._extractor = extractor

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data using the configured extractor.

        Args:
            data: Ignored for extract stages (pass empty DataFrame).

        Returns:
            Extracted DataFrame.
        """
        return self._extractor()


class TransformStage(PipelineStage):
    """Stage that applies a transformation function to the data."""

    def __init__(
        self,
        transformer: Callable[[pd.DataFrame], pd.DataFrame],
        name: str = "transform",
    ) -> None:
        """Initialize the transform stage.

        Args:
            transformer: Callable that transforms a DataFrame.
            name: Stage name.
        """
        super().__init__(name=name)
        self._transformer = transformer

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply the transformation function.

        Args:
            data: Input DataFrame.

        Returns:
            Transformed DataFrame.
        """
        return self._transformer(data)


class LoadStage(PipelineStage):
    """Stage that loads data into a destination (sink)."""

    def __init__(
        self,
        loader: Callable[[pd.DataFrame], None],
        name: str = "load",
    ) -> None:
        """Initialize the load stage.

        Args:
            loader: Callable that consumes a DataFrame.
            name: Stage name.
        """
        super().__init__(name=name)
        self._loader = loader

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Load data using the configured loader.

        Args:
            data: DataFrame to load.

        Returns:
            The same DataFrame (pass-through for pipeline chaining).
        """
        self._loader(data)
        return data


class FilterStage(PipelineStage):
    """Stage that filters rows based on a condition."""

    def __init__(
        self,
        condition: Callable[[pd.DataFrame], pd.Series],
        name: str = "filter",
    ) -> None:
        """Initialize the filter stage.

        Args:
            condition: Callable that returns a boolean Series for filtering.
            name: Stage name.
        """
        super().__init__(name=name)
        self._condition = condition

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Filter rows using the configured condition.

        Args:
            data: Input DataFrame.

        Returns:
            Filtered DataFrame.
        """
        mask = self._condition(data)
        return data[mask].reset_index(drop=True)


class ETLPipeline:
    """Orchestrates a sequence of ETL pipeline stages.

    Provides a fluent API for building pipelines, with comprehensive
    logging, error handling, and execution statistics.

    Example:
        >>> pipeline = (
        ...     ETLPipeline("sales_pipeline")
        ...     .extract(lambda: pd.read_csv("sales.csv"))
        ...     .transform(lambda df: df.dropna())
        ...     .transform(lambda df: df.assign(revenue=df["price"] * df["qty"]))
        ...     .load(lambda df: df.to_parquet("clean_sales.parquet"))
        ... )
        >>> result = pipeline.run()
    """

    def __init__(self, name: str = "etl_pipeline", fail_fast: bool = True) -> None:
        """Initialize the ETL pipeline.

        Args:
            name: Human-readable pipeline name.
            fail_fast: If True, stop on first error; if False, continue with warnings.
        """
        self.name = name
        self.fail_fast = fail_fast
        self._stages: List[PipelineStage] = []
        self._execution_stats: Dict[str, Any] = {}

    def add_stage(self, stage: PipelineStage) -> "ETLPipeline":
        """Add a pipeline stage.

        Args:
            stage: PipelineStage instance to add.

        Returns:
            Self for method chaining.
        """
        self._stages.append(stage)
        return self

    def extract(self, extractor: Callable[[], pd.DataFrame], name: str = "extract") -> "ETLPipeline":
        """Add an extraction stage to the pipeline.

        Args:
            extractor: Callable that returns a DataFrame.
            name: Stage name.

        Returns:
            Self for method chaining.
        """
        return self.add_stage(ExtractStage(extractor, name=name))

    def transform(
        self, transformer: Callable[[pd.DataFrame], pd.DataFrame], name: str = "transform"
    ) -> "ETLPipeline":
        """Add a transformation stage to the pipeline.

        Args:
            transformer: Callable that transforms a DataFrame.
            name: Stage name.

        Returns:
            Self for method chaining.
        """
        return self.add_stage(TransformStage(transformer, name=name))

    def filter(
        self, condition: Callable[[pd.DataFrame], pd.Series], name: str = "filter"
    ) -> "ETLPipeline":
        """Add a filter stage to the pipeline.

        Args:
            condition: Callable that returns a boolean Series for filtering.
            name: Stage name.

        Returns:
            Self for method chaining.
        """
        return self.add_stage(FilterStage(condition, name=name))

    def load(self, loader: Callable[[pd.DataFrame], None], name: str = "load") -> "ETLPipeline":
        """Add a load stage to the pipeline.

        Args:
            loader: Callable that consumes a DataFrame.
            name: Stage name.

        Returns:
            Self for method chaining.
        """
        return self.add_stage(LoadStage(loader, name=name))

    def run(self, initial_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Execute the pipeline sequentially through all stages.

        Args:
            initial_data: Starting DataFrame. If None, the first stage
                must be an ExtractStage that produces data.

        Returns:
            Final DataFrame after all stages have been applied.

        Raises:
            RuntimeError: If a stage fails and fail_fast is True.
        """
        if not self._stages:
            logger.warning("Pipeline '%s' has no stages", self.name)
            return initial_data if initial_data is not None else pd.DataFrame()

        pipeline_start = time.time()
        data = initial_data if initial_data is not None else pd.DataFrame()
        stage_stats: List[Dict[str, Any]] = []

        logger.info("Starting pipeline '%s' with %d stages", self.name, len(self._stages))

        for i, stage in enumerate(self._stages, 1):
            try:
                data = stage.execute(data)
                stage_stats.append(stage.stats)
            except Exception as exc:
                logger.error(
                    "Pipeline '%s' failed at stage %d/%d '%s': %s",
                    self.name,
                    i,
                    len(self._stages),
                    stage.name,
                    exc,
                )
                if self.fail_fast:
                    raise RuntimeError(
                        f"Pipeline '{self.name}' failed at stage '{stage.name}': {exc}"
                    ) from exc

        total_time = time.time() - pipeline_start
        self._execution_stats = {
            "pipeline_name": self.name,
            "total_stages": len(self._stages),
            "total_time_sec": round(total_time, 3),
            "stages": stage_stats,
            "final_rows": len(data),
            "final_columns": len(data.columns),
        }

        logger.info(
            "Pipeline '%s' completed: %d stages, %d final rows (%.2fs)",
            self.name,
            len(self._stages),
            len(data),
            total_time,
        )
        return data

    @property
    def stats(self) -> Dict[str, Any]:
        """Return execution statistics for the pipeline."""
        return self._execution_stats

    def __repr__(self) -> str:
        stage_names = [s.name for s in self._stages]
        return f"ETLPipeline(name='{self.name}', stages={stage_names})"
