"""Database connectors for various database systems."""

from anonimize.connectors.base import (
    DatabaseConnector,
    ConnectionConfig,
    ConnectionPool,
    ColumnInfo,
    TableInfo,
    QueryResult,
    BaseConnector,
    Transaction,
)
from anonimize.connectors.sqlite import SQLiteConnector

__all__ = [
    # Legacy connector
    "DatabaseConnector",
    # New base classes
    "ConnectionConfig",
    "ConnectionPool",
    "ColumnInfo",
    "TableInfo",
    "QueryResult",
    "BaseConnector",
    "Transaction",
    # Concrete implementations
    "SQLiteConnector",
    "create_connector",
]

# Optional connectors (require additional dependencies)
try:
    from anonimize.connectors.postgres import PostgreSQLConnector
    __all__.append("PostgreSQLConnector")
except ImportError:
    PostgreSQLConnector = None  # type: ignore

try:
    from anonimize.connectors.mysql import MySQLConnector
    __all__.append("MySQLConnector")
except ImportError:
    MySQLConnector = None  # type: ignore

try:
    from anonimize.connectors.mongodb import MongoDBConnector
    __all__.append("MongoDBConnector")
except ImportError:
    MongoDBConnector = None  # type: ignore


def create_connector(connection_string: str, **kwargs):
    """Create appropriate connector based on connection string.
    
    Args:
        connection_string: Database connection string
        **kwargs: Additional connection options
    
    Returns:
        Database connector instance
    
    Raises:
        ValueError: If connection string is not recognized
        ImportError: If required driver is not installed
    """
    if connection_string.startswith(("postgresql://", "postgres://")):
        if PostgreSQLConnector is None:
            raise ImportError(
                "PostgreSQL connector requires psycopg2-binary. "
                'Install with: pip install "anonimize[postgresql]"'
            )
        return PostgreSQLConnector(connection_string, **kwargs)
    
    elif connection_string.startswith(("mysql://", "mariadb://")):
        if MySQLConnector is None:
            raise ImportError(
                "MySQL connector requires pymysql. "
                'Install with: pip install "anonimize[mysql]"'
            )
        return MySQLConnector(connection_string, **kwargs)
    
    elif connection_string.startswith(("mongodb://", "mongodb+srv://")):
        if MongoDBConnector is None:
            raise ImportError(
                "MongoDB connector requires pymongo. "
                'Install with: pip install "anonimize[mongodb]"'
            )
        return MongoDBConnector(connection_string, **kwargs)
    
    elif connection_string.startswith("sqlite:///"):
        return SQLiteConnector(connection_string, **kwargs)
    
    else:
        raise ValueError(
            f"Unsupported connection string format: {connection_string[:20]}..."
        )
