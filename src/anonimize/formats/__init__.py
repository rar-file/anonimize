"""File format support for anonimize.

This package provides unified file format handling for multiple formats
with streaming support for large files.

Supported Formats:
    - Parquet (columnar, compressed)
    - Excel (.xlsx, .xls)
    - Avro (binary, schema-evolution)
    - CSV (via existing anonymizers)
    - JSON (via existing anonymizers)

Example:
    >>> from anonimize.formats import get_handler, FormatConfig
    >>> 
    >>> # Auto-detect format from extension
    >>> handler = get_handler("data.parquet")
    >>> 
    >>> # Stream read large file
    >>> for batch in handler.read_streaming("large.parquet", batch_size=5000):
    ...     anonymized_batch = anonymizer.anonymize(batch)
    ...     # Process batch...
    >>> 
    >>> # Stream write
    >>> config = FormatConfig(compression="zstd", batch_size=10000)
    >>> handler = get_handler("output.parquet", config)
    >>> with handler.write_streaming("output.parquet") as writer:
    ...     for batch in data_source:
    ...         writer.write_batch(batch)
"""

from typing import Optional

from anonimize.formats.base import (
    BaseFormatHandler,
    FormatConfig,
    FileStats,
    StreamingWriter,
    FormatRegistry,
    register_handler,
    get_handler,
    is_supported,
)

# Import format handlers with availability checks
try:
    from anonimize.formats.parquet import ParquetHandler
except ImportError:
    ParquetHandler = None  # type: ignore

try:
    from anonimize.formats.excel import ExcelHandler
except ImportError:
    ExcelHandler = None  # type: ignore

try:
    from anonimize.formats.avro import AvroHandler
except ImportError:
    AvroHandler = None  # type: ignore


def create_handler(
    format_name: str,
    config: Optional[FormatConfig] = None
) -> BaseFormatHandler:
    """Create a file format handler by name.
    
    Args:
        format_name: Format name ('parquet', 'excel', 'avro', etc.).
        config: Optional format configuration.
    
    Returns:
        Configured handler instance.
    
    Raises:
        ValueError: If format is not supported.
        ImportError: If required dependencies are not installed.
    
    Example:
        >>> handler = create_handler("parquet")
        >>> data = handler.read("data.parquet")
    """
    handlers = {
        "parquet": ParquetHandler,
        "pq": ParquetHandler,
        "excel": ExcelHandler,
        "xlsx": ExcelHandler,
        "xls": ExcelHandler,
        "avro": AvroHandler,
    }
    
    handler_class = handlers.get(format_name.lower())
    
    if handler_class is None:
        raise ValueError(
            f"Unsupported format: {format_name}. "
            f"Supported formats: {list(handlers.keys())}"
        )
    
    if handler_class is None:  # Import failed
        raise ImportError(
            f"Required dependencies for {format_name} are not installed."
        )
    
    return handler_class(config)


# Re-export everything
__all__ = [
    # Base classes
    "BaseFormatHandler",
    "FormatConfig",
    "FileStats",
    "StreamingWriter",
    "FormatRegistry",
    # Registry functions
    "register_handler",
    "get_handler",
    "is_supported",
    # Handlers
    "ParquetHandler",
    "ExcelHandler",
    "AvroHandler",
    # Factory
    "create_handler",
]
