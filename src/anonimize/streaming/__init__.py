"""Streaming module for anonimize.

This module provides streaming processing capabilities for large datasets
that don't fit in memory.

Example:
    >>> from anonimize.streaming import StreamingProcessor, StreamConfig
    >>> from anonimize import Anonymizer
    >>> 
    >>> config = StreamConfig(
    ...     batch_size=10000,
    ...     checkpoint_interval=10,
    ... )
    >>> processor = StreamingProcessor(Anonymizer(), config)
    >>> stats = processor.process_file(
    ...     "input.parquet",
    ...     "output.parquet",
    ...     anonymization_config={"name": {"strategy": "replace"}},
    ... )
    >>> print(f"Processed {stats.rows_processed} rows")
"""

from anonimize.streaming.processor import (
    StreamConfig,
    ProcessingStats,
    Checkpoint,
    StreamingProcessor,
    process_large_file,
)

__all__ = [
    "StreamConfig",
    "ProcessingStats",
    "Checkpoint",
    "StreamingProcessor",
    "process_large_file",
]
