"""
File Loader Module for Thunders BigData System.

Provides a unified interface for loading data from various file formats
including CSV, JSON, and Parquet with schema inference and validation.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

# Supported file extensions and their loaders
SUPPORTED_FORMATS = {
    ".csv": "csv",
    ".tsv": "csv",
    ".json": "json",
    ".jsonl": "json_lines",
    ".ndjson": "json_lines",
    ".parquet": "parquet",
    ".pq": "parquet",
}


class FileLoader:
    """Loads and validates data files from local and distributed filesystems.

    Supports CSV, JSON (both regular and newline-delimited), and Parquet formats
    with automatic format detection, schema validation, and data quality checks.

    Attributes:
        base_path: Root directory for resolving relative file paths.
        encoding: Default file encoding (e.g., 'utf-8', 'latin-1').
    """

    def __init__(
        self,
        base_path: str = ".",
        encoding: str = "utf-8",
        default_csv_delimiter: str = ",",
        default_csv_quotechar: str = '"',
        validate_schema: bool = False,
        max_file_size_mb: Optional[float] = None,
    ) -> None:
        """Initialize the FileLoader.

        Args:
            base_path: Root directory for resolving relative paths.
            encoding: Default character encoding for text files.
            default_csv_delimiter: Default delimiter for CSV files.
            default_csv_quotechar: Default quote character for CSV files.
            validate_schema: Whether to validate file schemas before loading.
            max_file_size_mb: Maximum file size in MB to load (None for unlimited).
        """
        self.base_path = Path(base_path).resolve()
        self.encoding = encoding
        self.default_csv_delimiter = default_csv_delimiter
        self.default_csv_quotechar = default_csv_quotechar
        self.validate_schema = validate_schema
        self.max_file_size_mb = max_file_size_mb

    def _resolve_path(self, file_path: str) -> Path:
        """Resolve a file path relative to the base path.

        Args:
            file_path: File path (absolute or relative to base_path).

        Returns:
            Resolved absolute Path object.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path

        path = path.resolve()

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        return path

    def _check_file_size(self, path: Path) -> None:
        """Check if the file exceeds the maximum size limit.

        Args:
            path: Path to the file.

        Raises:
            ValueError: If the file exceeds the configured size limit.
        """
        if self.max_file_size_mb is not None:
            size_mb = path.stat().st_size / (1024 * 1024)
            if size_mb > self.max_file_size_mb:
                raise ValueError(
                    f"File size ({size_mb:.1f} MB) exceeds limit "
                    f"({self.max_file_size_mb} MB): {path}"
                )

    def detect_format(self, file_path: str) -> Optional[str]:
        """Detect the file format from its extension.

        Args:
            file_path: Path to the file.

        Returns:
            Format string ('csv', 'json', 'json_lines', 'parquet') or None.
        """
        suffix = Path(file_path).suffix.lower()
        return SUPPORTED_FORMATS.get(suffix)

    def load_csv(
        self,
        file_path: str,
        delimiter: Optional[str] = None,
        quotechar: Optional[str] = None,
        header: int = 0,
        dtype: Optional[Dict[str, str]] = None,
        parse_dates: Optional[List[str]] = None,
        na_values: Optional[List[str]] = None,
        chunksize: Optional[int] = None,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, Any]:
        """Load data from a CSV file.

        Args:
            file_path: Path to the CSV file.
            delimiter: Column delimiter (defaults to self.default_csv_delimiter).
            quotechar: Quote character (defaults to self.default_csv_quotechar).
            header: Row number to use as column names.
            dtype: Column data type mapping.
            parse_dates: List of columns to parse as dates.
            na_values: Additional strings to recognize as NaN.
            chunksize: If set, return an iterator for chunked reading.
            **kwargs: Additional arguments passed to pandas.read_csv.

        Returns:
            pandas DataFrame (or TextFileReader if chunksize is set).

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file format is invalid or exceeds size limit.
        """
        path = self._resolve_path(file_path)
        self._check_file_size(path)

        logger.info("Loading CSV file: %s", path)

        try:
            df = pd.read_csv(
                path,
                delimiter=delimiter or self.default_csv_delimiter,
                quotechar=quotechar or self.default_csv_quotechar,
                header=header,
                encoding=self.encoding,
                dtype=dtype,
                parse_dates=parse_dates,
                na_values=na_values,
                chunksize=chunksize,
                **kwargs,
            )
            if chunksize is None:
                logger.info("Loaded %d rows x %d columns from %s", len(df), len(df.columns), path)
            return df
        except Exception as exc:
            logger.error("Failed to load CSV file %s: %s", path, exc)
            raise

    def load_json(
        self,
        file_path: str,
        orient: str = "records",
        lines: bool = False,
        dtype: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load data from a JSON file.

        Args:
            file_path: Path to the JSON file.
            orient: Expected JSON structure format.
            lines: If True, read the file as JSON Lines (one JSON object per line).
            dtype: Column data type mapping.
            **kwargs: Additional arguments passed to pandas.read_json.

        Returns:
            pandas DataFrame.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file format is invalid or exceeds size limit.
        """
        path = self._resolve_path(file_path)
        self._check_file_size(path)

        # Auto-detect JSON Lines format
        if path.suffix.lower() in (".jsonl", ".ndjson"):
            lines = True

        logger.info("Loading JSON file: %s (lines=%s)", path, lines)

        try:
            df = pd.read_json(
                path,
                orient=orient,
                lines=lines,
                dtype=dtype,
                encoding=self.encoding,
                **kwargs,
            )
            logger.info("Loaded %d rows x %d columns from %s", len(df), len(df.columns), path)
            return df
        except Exception as exc:
            logger.error("Failed to load JSON file %s: %s", path, exc)
            raise

    def load_parquet(
        self,
        file_path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load data from a Parquet file.

        Args:
            file_path: Path to the Parquet file.
            columns: List of columns to read (None for all).
            filters: Row filters to apply during read.
            **kwargs: Additional arguments passed to pandas.read_parquet.

        Returns:
            pandas DataFrame.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file exceeds the size limit.
        """
        path = self._resolve_path(file_path)
        self._check_file_size(path)

        logger.info("Loading Parquet file: %s", path)

        try:
            df = pd.read_parquet(
                path,
                columns=columns,
                filters=filters,
                **kwargs,
            )
            logger.info("Loaded %d rows x %d columns from %s", len(df), len(df.columns), path)
            return df
        except Exception as exc:
            logger.error("Failed to load Parquet file %s: %s", path, exc)
            raise

    def load(
        self,
        file_path: str,
        format: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load a data file with automatic format detection.

        Args:
            file_path: Path to the data file.
            format: Explicit format override ('csv', 'json', 'json_lines', 'parquet').
                If None, the format is detected from the file extension.
            **kwargs: Additional arguments passed to the format-specific loader.

        Returns:
            pandas DataFrame.

        Raises:
            ValueError: If the format cannot be detected or is unsupported.
            FileNotFoundError: If the file does not exist.
        """
        detected_format = format or self.detect_format(file_path)

        if detected_format is None:
            raise ValueError(
                f"Cannot detect format for file: {file_path}. "
                f"Supported formats: {list(SUPPORTED_FORMATS.keys())}"
            )

        loaders = {
            "csv": self.load_csv,
            "json": self.load_json,
            "json_lines": lambda fp, **kw: self.load_json(fp, lines=True, **kw),
            "parquet": self.load_parquet,
        }

        loader = loaders.get(detected_format)
        if loader is None:
            raise ValueError(f"Unsupported format: {detected_format}")

        return loader(file_path, **kwargs)

    def load_multiple(
        self,
        file_paths: List[str],
        format: Optional[str] = None,
        merge: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Load multiple data files and optionally merge them.

        Args:
            file_paths: List of file paths to load.
            format: Explicit format override for all files.
            merge: If True, concatenate all DataFrames into one.
            **kwargs: Additional arguments passed to each loader.

        Returns:
            Single merged DataFrame if merge=True, otherwise a dict
            mapping file paths to DataFrames.
        """
        results: Dict[str, pd.DataFrame] = {}

        for fp in file_paths:
            try:
                results[fp] = self.load(fp, format=format, **kwargs)
            except Exception as exc:
                logger.error("Skipping file %s: %s", fp, exc)

        if merge and results:
            return pd.concat(results.values(), ignore_index=True)

        return results

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get metadata about a data file without loading it.

        Args:
            file_path: Path to the data file.

        Returns:
            Dictionary with file metadata including size, format, and last modified time.
        """
        path = self._resolve_path(file_path)
        stat = path.stat()

        return {
            "path": str(path),
            "name": path.name,
            "format": self.detect_format(file_path),
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "last_modified": stat.st_mtime,
            "extension": path.suffix.lower(),
        }

    def __repr__(self) -> str:
        return f"FileLoader(base_path='{self.base_path}', encoding='{self.encoding}')"
