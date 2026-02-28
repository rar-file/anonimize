"""File format support base classes.

This module provides base classes and utilities for reading and writing
different file formats with streaming support.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class FormatConfig:
    """Configuration for file format handlers.

    Attributes:
        compression: Compression type (gzip, snappy, lz4, etc.).
        compression_level: Compression level (if applicable).
        buffer_size: Buffer size for streaming operations.
        encoding: Text encoding for text-based formats.
        batch_size: Number of rows to process per batch.
        extra: Additional format-specific options.
    """

    compression: Optional[str] = None
    compression_level: Optional[int] = None
    buffer_size: int = 8192
    encoding: str = "utf-8"
    batch_size: int = 10000
    extra: Dict[str, Any] = None

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}


@dataclass
class FileStats:
    """Statistics for file operations.

    Attributes:
        rows_read: Number of rows read.
        rows_written: Number of rows written.
        bytes_read: Number of bytes read.
        bytes_written: Number of bytes written.
        columns: List of column names.
        encoding: File encoding.
        compression: Compression type used.
    """

    rows_read: int = 0
    rows_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    columns: List[str] = None
    encoding: Optional[str] = None
    compression: Optional[str] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


class BaseFormatHandler(ABC):
    """Abstract base class for file format handlers.

    All file format handlers must implement this interface to provide
    a unified API for reading and writing data files.

    Attributes:
        config: Format configuration.
        supported_extensions: List of supported file extensions.

    Example:
        >>> class MyFormatHandler(BaseFormatHandler):
        ...     @property
        ...     def supported_extensions(self):
        ...         return [".myformat"]
        ...
        ...     def read(self, path):
        ...         # Implementation
        ...         pass
    """

    def __init__(self, config: Optional[FormatConfig] = None):
        """Initialize the format handler.

        Args:
            config: Format configuration.
        """
        self.config = config or FormatConfig()
        self._stats = FileStats()

    @property
    @abstractmethod
    def supported_extensions(self) -> List[str]:
        """Return list of supported file extensions."""
        pass

    @abstractmethod
    def read(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Read all data from a file.

        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read.
            **kwargs: Additional format-specific options.

        Returns:
            List of row dictionaries.
        """
        pass

    @abstractmethod
    def read_streaming(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Read data in batches (streaming).

        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read.
            batch_size: Number of rows per batch.
            **kwargs: Additional format-specific options.

        Yields:
            Batches of row dictionaries.
        """
        pass

    @abstractmethod
    def write(
        self,
        destination: Union[str, Path, BinaryIO],
        data: List[Dict[str, Any]],
        schema: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> FileStats:
        """Write data to a file.

        Args:
            destination: File path or file-like object.
            data: List of row dictionaries.
            schema: Optional schema dictionary {column: type}.
            **kwargs: Additional format-specific options.

        Returns:
            File statistics.
        """
        pass

    @abstractmethod
    def write_streaming(
        self,
        destination: Union[str, Path, BinaryIO],
        schema: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> "StreamingWriter":
        """Create a streaming writer.

        Args:
            destination: File path or file-like object.
            schema: Optional schema dictionary.
            **kwargs: Additional format-specific options.

        Returns:
            StreamingWriter instance.
        """
        pass

    @abstractmethod
    def get_schema(self, source: Union[str, Path, BinaryIO]) -> Dict[str, str]:
        """Get the schema of a file.

        Args:
            source: File path or file-like object.

        Returns:
            Dictionary mapping column names to types.
        """
        pass

    def can_handle(self, path: Union[str, Path]) -> bool:
        """Check if this handler can handle the given file.

        Args:
            path: File path to check.

        Returns:
            True if the file extension is supported.
        """
        path_str = str(path).lower()
        return any(path_str.endswith(ext.lower()) for ext in self.supported_extensions)

    def get_stats(self) -> FileStats:
        """Get file operation statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = FileStats()


class StreamingWriter(ABC):
    """Abstract base class for streaming file writers.

    This class provides a context manager for writing data in batches
    without loading everything into memory.

    Example:
        >>> with handler.write_streaming("output.parquet") as writer:
        ...     for batch in data_source:
        ...         writer.write_batch(batch)
    """

    def __init__(
        self,
        destination: Union[str, Path, BinaryIO],
        config: FormatConfig,
        schema: Optional[Dict[str, str]] = None,
    ):
        """Initialize the streaming writer.

        Args:
            destination: File path or file-like object.
            config: Format configuration.
            schema: Optional schema dictionary.
        """
        self.destination = destination
        self.config = config
        self.schema = schema
        self._rows_written = 0
        self._closed = False

    @abstractmethod
    def write_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Write a batch of rows.

        Args:
            batch: List of row dictionaries.

        Returns:
            Number of rows written.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the writer and finalize the file."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    @property
    def rows_written(self) -> int:
        """Number of rows written."""
        return self._rows_written

    def _check_closed(self) -> None:
        """Check if the writer is closed."""
        if self._closed:
            raise RuntimeError("Writer is already closed")


class FormatRegistry:
    """Registry for file format handlers.

    This registry provides a central place to register and retrieve
    format handlers by file extension.

    Example:
        >>> registry = FormatRegistry()
        >>> registry.register(ParquetHandler())
        >>> handler = registry.get_handler("data.parquet")
    """

    def __init__(self):
        """Initialize the registry."""
        self._handlers: Dict[str, BaseFormatHandler] = {}
        self._extensions: Dict[str, str] = {}

    def register(self, handler: BaseFormatHandler, override: bool = False) -> None:
        """Register a format handler.

        Args:
            handler: The handler to register.
            override: Whether to override existing handlers.

        Raises:
            ValueError: If extension already registered and override=False.
        """
        for ext in handler.supported_extensions:
            ext_lower = ext.lower()
            if ext_lower in self._extensions and not override:
                raise ValueError(f"Extension {ext} is already registered")

            handler_id = f"{handler.__class__.__name__}_{ext_lower}"
            self._handlers[handler_id] = handler
            self._extensions[ext_lower] = handler_id

    def get_handler(
        self, path: Union[str, Path], config: Optional[FormatConfig] = None
    ) -> BaseFormatHandler:
        """Get a handler for a file path.

        Args:
            path: File path to handle.
            config: Optional format configuration.

        Returns:
            Format handler for the file.

        Raises:
            ValueError: If no handler found for the file extension.
        """
        path_str = str(path).lower()

        for ext, handler_id in self._extensions.items():
            if path_str.endswith(ext):
                handler = self._handlers[handler_id]
                # Return a new instance with the given config
                return handler.__class__(config or handler.config)

        raise ValueError(f"No handler found for file: {path}")

    def get_supported_extensions(self) -> List[str]:
        """Get list of supported file extensions."""
        return list(self._extensions.keys())

    def is_supported(self, path: Union[str, Path]) -> bool:
        """Check if a file type is supported."""
        path_str = str(path).lower()
        return any(path_str.endswith(ext) for ext in self._extensions.keys())


# Global registry instance
_global_registry = FormatRegistry()


def register_handler(handler: BaseFormatHandler, override: bool = False) -> None:
    """Register a handler with the global registry.

    Args:
        handler: The handler to register.
        override: Whether to override existing handlers.
    """
    _global_registry.register(handler, override)


def get_handler(
    path: Union[str, Path], config: Optional[FormatConfig] = None
) -> BaseFormatHandler:
    """Get a handler from the global registry.

    Args:
        path: File path to handle.
        config: Optional format configuration.

    Returns:
        Format handler for the file.
    """
    return _global_registry.get_handler(path, config)


def is_supported(path: Union[str, Path]) -> bool:
    """Check if a file type is supported by the global registry."""
    return _global_registry.is_supported(path)
