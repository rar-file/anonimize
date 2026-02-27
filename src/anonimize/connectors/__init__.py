"""Database connectors for various database systems."""

from anonimize.connectors.base import DatabaseConnector
from anonimize.connectors.sqlite import SQLiteConnector

__all__ = [
    "DatabaseConnector",
    "SQLiteConnector",
]

# Optional connectors (require additional dependencies)
try:
    from anonimize.connectors.postgres import PostgreSQLConnector
    __all__.append("PostgreSQLConnector")
except ImportError:
    pass
