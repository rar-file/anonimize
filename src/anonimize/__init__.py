"""Anonimize: A data anonymization tool using Phoney.

This package provides tools for anonymizing PII (Personally Identifiable Information)
in databases and files while preserving data relationships.

QUICK START - The Simple Way (3 lines):
    >>> from anonimize import anonymize
    >>> anonymize("customers.csv", "customers_safe.csv")
    "customers_safe.csv"

Or even simpler - just pass data:
    >>> from anonimize import anonymize_data
    >>> data = {"name": "John", "email": "john@example.com"}
    >>> anonymize_data(data)
    {"name": "Sarah Smith", "email": "j***@example.com"}

ADVANCED USAGE - Full Control:
    >>> from anonimize import Anonymizer
    >>> anon = Anonymizer(locale="en_US", seed=42)
    >>> config = {
    ...     "name": {"strategy": "replace", "type": "name"},
    ...     "email": {"strategy": "hash", "type": "email"},
    ... }
    >>> result = anon.anonymize(data, config)

Modules:
    - simple: Dead-simple API (start here!)
    - core: Main Anonymizer class
    - connectors: Database connectors (PostgreSQL, MySQL, SQLite, MongoDB)
    - formats: File format handlers (Parquet, Excel, Avro)
    - streaming: Streaming processor for large files
    - anonymizers: Format-specific anonymizers
    - detectors: PII detection utilities
"""

# Simple API - Import these for quick usage
from anonimize.__version__ import __version__, __version_info__
from anonimize.core import Anonymizer
from anonimize.simple import anonymize, anonymize_data, detect_pii, preview

# Export connectors if available
try:
    from anonimize.connectors import (
        ConnectionConfig,
        MongoDBConnector,
        MySQLConnector,
        PostgreSQLConnector,
        SQLiteConnector,
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
        AvroHandler,
        ExcelHandler,
        FormatConfig,
        ParquetHandler,
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
        StreamConfig,
        StreamingProcessor,
        process_large_file,
    )
except ImportError:
    StreamingProcessor = None  # type: ignore
    StreamConfig = None  # type: ignore
    process_large_file = None  # type: ignore

__all__ = [
    # Simple API - Start here!
    "anonymize",
    "anonymize_data",
    "detect_pii",
    "preview",
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
