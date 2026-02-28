"""Database connector base class and implementations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Callable
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Configuration for database connections."""
    host: str = "localhost"
    port: Optional[int] = None
    database: str = ""
    user: str = ""
    password: str = ""
    ssl_mode: Optional[str] = None
    connect_timeout: int = 30
    pool_size: int = 5
    pool_timeout: int = 30
    max_overflow: int = 10
    pool_recycle: int = 3600
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[Any] = None
    max_length: Optional[int] = None
    is_primary_key: bool = False
    is_unique: bool = False


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    schema: Optional[str] = None
    columns: List[ColumnInfo] = field(default_factory=list)
    primary_key: List[str] = field(default_factory=list)
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


@dataclass
class QueryResult:
    """Result of a database query."""
    rows: List[Dict[str, Any]] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    affected_rows: int = 0
    execution_time_ms: float = 0.0


class ConnectionPool:
    """Simple connection pool implementation."""
    
    def __init__(self, factory: Callable, config: ConnectionConfig):
        self._factory = factory
        self._config = config
        self._pool: List[Any] = []
        self._in_use: set = set()
        self._max_size = config.pool_size
        self._max_overflow = config.max_overflow
        self._overflow = 0
        self._total_created = 0
    
    def acquire(self, timeout: float = 30.0) -> Any:
        """Acquire a connection from the pool."""
        if self._pool:
            conn = self._pool.pop()
            self._in_use.add(id(conn))
            return conn
        
        if len(self._in_use) < self._max_size + self._max_overflow:
            conn = self._factory()
            self._total_created += 1
            self._in_use.add(id(conn))
            if len(self._in_use) > self._max_size:
                self._overflow += 1
            return conn
        
        raise RuntimeError("Connection pool exhausted")
    
    def release(self, connection: Any) -> None:
        """Release a connection back to the pool."""
        conn_id = id(connection)
        if conn_id in self._in_use:
            self._in_use.remove(conn_id)
            self._pool.append(connection)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            "pool_size": self._max_size,
            "available": len(self._pool),
            "in_use": len(self._in_use),
            "overflow": self._overflow,
            "total_created": self._total_created,
        }
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        self._pool.clear()
        self._in_use.clear()
        self._overflow = 0


class BaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    DB_TYPE: str = ""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._pool: Optional[ConnectionPool] = None
        self._closed = False
    
    @abstractmethod
    def connect(self):
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self, connection: Any) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def execute(self, query: str, parameters: Optional[tuple] = None, connection: Optional[Any] = None) -> QueryResult:
        """Execute a query."""
        pass
    
    @abstractmethod
    def executemany(self, query: str, parameters_list: List[tuple], connection: Optional[Any] = None) -> QueryResult:
        """Execute a query multiple times."""
        pass
    
    @abstractmethod
    def fetchiter(self, query: str, parameters: Optional[tuple] = None, batch_size: int = 1000, connection: Optional[Any] = None) -> Iterator[Dict[str, Any]]:
        """Fetch results as an iterator."""
        pass
    
    @abstractmethod
    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables."""
        pass
    
    @abstractmethod
    def get_columns(self, table_name: str, schema: Optional[str] = None) -> List[ColumnInfo]:
        """Get column information for a table."""
        pass
    
    @abstractmethod
    def get_primary_key(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """Get primary key columns for a table."""
        pass
    
    @abstractmethod
    def scan_table(self, table_name: str, columns: Optional[List[str]] = None, schema: Optional[str] = None, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Scan a table and yield rows."""
        pass
    
    @abstractmethod
    def update_rows(self, table_name: str, updates: List[Dict[str, Any]], schema: Optional[str] = None, batch_size: int = 1000) -> int:
        """Update rows in a table."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is working."""
        pass
    
    @abstractmethod
    def begin_transaction(self, connection: Any) -> None:
        """Begin a transaction."""
        pass
    
    @abstractmethod
    def commit_transaction(self, connection: Any) -> None:
        """Commit a transaction."""
        pass
    
    @abstractmethod
    def rollback_transaction(self, connection: Any) -> None:
        """Rollback a transaction."""
        pass
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        if self._pool is None:
            return {"status": "not_initialized"}
        return self._pool.get_stats()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._closed = True


class Transaction:
    """Database transaction context manager."""
    
    def __init__(self, connector: BaseConnector, connection: Any):
        self._connector = connector
        self._connection = connection
        self._active = False
        self._queries_executed = 0
        self._readonly = False
    
    def begin(self) -> None:
        """Begin the transaction."""
        self._connector.begin_transaction(self._connection)
        self._active = True
    
    def commit(self) -> None:
        """Commit the transaction."""
        self._connector.commit_transaction(self._connection)
        self._active = False
    
    def rollback(self) -> None:
        """Rollback the transaction."""
        self._connector.rollback_transaction(self._connection)
        self._active = False
    
    def execute(self, query: str, parameters: Optional[tuple] = None) -> QueryResult:
        """Execute a query within the transaction."""
        if not self._active:
            raise RuntimeError("Transaction is not active")
        result = self._connector.execute(query, parameters, self._connection)
        self._queries_executed += 1
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get transaction statistics."""
        return {
            "active": self._active,
            "queries_executed": self._queries_executed,
            "readonly": self._readonly,
        }


class DatabaseConnector(ABC):
    """Abstract base class for database connectors (legacy)."""
    
    def __init__(self, connection_string: str, **kwargs):
        self.connection_string = connection_string
        self._options = kwargs
        self._connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of all tables in database."""
        pass
    
    @abstractmethod
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table.
        
        Returns list of dicts with: name, type, nullable, default
        """
        pass
    
    @abstractmethod
    def read_table(self, table: str, batch_size: int = 1000) -> Iterator[List[Dict]]:
        """Read table data in batches.
        
        Yields:
            List of row dicts
        """
        pass
    
    @abstractmethod
    def write_table(self, table: str, data: List[Dict]) -> int:
        """Write data to table.
        
        Returns:
            Number of rows written
        """
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute raw SQL query."""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
