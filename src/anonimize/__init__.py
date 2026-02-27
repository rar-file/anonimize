"""Anonimize: A data anonymization tool using Phoney.

This package provides tools for anonymizing PII (Personally Identifiable Information)
in databases and files while preserving data relationships.

Quick Start:
    >>> from anonimize import Anonymizer
    >>> anon = Anonymizer()
    >>> data = {"name": "John Doe", "email": "john@example.com"}
    >>> config = {
    ...     "name": {"strategy": "replace", "type": "name"},
    ...     "email": {"strategy": "hash", "type": "email"},
    ... }
    >>> result = anon.anonymize(data, config)
    >>> print(result)
    {'name': 'Jane Smith', 'email': 'a3f5c8e9...'}

Modules:
    - core: Main Anonymizer class
    - connectors: Database connectors (PostgreSQL, MySQL, SQLite, MongoDB)
    - formats: File format handlers (Parquet, Excel, Avro)
    - streaming: Streaming processor for large files
    - anonymizers: Format-specific anonymizers
    - detectors: PII detection utilities
"""

from anonimize.core import Anonymizer
from anonimize.__version__ import __version__, __version_info__

# Export connectors if available
try:
    from anonimize.connectors import (
        PostgreSQLConnector,
        MySQLConnector,
        SQLiteConnector,
        MongoDBConnector,
        ConnectionConfig,
        create_connector,
    )
except ImportError:
    PostgreSQLConnector = None  # type: ignore
    MySQLConnector = None  # type: ignore
    SQLiteConnector = None  # type: ignore
    MongoDBConnector = None  # type: ignore
    ConnectionConfig = None  # type: ignore
    create_connector = None  # type: ignore

# Export formats if available
try:
    from anonimize.formats import (
        ParquetHandler,
        ExcelHandler,
        AvroHandler,
        FormatConfig,
        get_handler,
    )
except ImportError:
    ParquetHandler = None  # type: ignore
    ExcelHandler = None  # type: ignore
    AvroHandler = None  # type: ignore
    FormatConfig = None  # type: ignore
    get_handler = None  # type: ignore

# Export streaming if available
try:
    from anonimize.streaming import (
        StreamingProcessor,
        StreamConfig,
        process_large_file,
    )
except ImportError:
    StreamingProcessor = None  # type: ignore
    StreamConfig = None  # type: ignore
    process_large_file = None  # type: ignore

__all__ = [
    # Core
    "Anonymizer",
    "__version__",
    "__version_info__",
    # Connectors
    "PostgreSQLConnector",
    "MySQLConnector",
    "SQLiteConnector",
    "MongoDBConnector",
    "ConnectionConfig",
    "create_connector",
    # Formats
    "ParquetHandler",
    "ExcelHandler",
    "AvroHandler",
    "FormatConfig",
    "get_handler",
    # Streaming
    "StreamingProcessor",
    "StreamConfig",
    "process_large_file",
]
