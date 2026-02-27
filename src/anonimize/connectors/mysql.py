"""MySQL database connector.

This module provides a production-grade MySQL connector with
support for table scanning and connection pooling.
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple
import logging

try:
    import pymysql
    from pymysql.cursors import DictCursor, SSCursor
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False
    pymysql = None
    DictCursor = None
    SSCursor = None

from anonimize.connectors.base import (
    BaseConnector,
    ConnectionConfig,
    ColumnInfo,
    TableInfo,
    QueryResult,
)

logger = logging.getLogger(__name__)


class MySQLConnector(BaseConnector):
    """MySQL database connector.
    
    This connector provides MySQL support with:
    - Connection pooling via PyMySQL
    - Server-side cursors for large result sets
    - Batch operations
    - SSL support
    
    Example:
        >>> config = ConnectionConfig(
        ...     host="localhost",
        ...     port=3306,
        ...     database="mydb",
        ...     user="root",
        ...     password="secret",
        ... )
        >>> connector = MySQLConnector(config)
        >>> with connector.transaction() as txn:
        ...     result = txn.query("SELECT * FROM users LIMIT 10")
    """
    
    DB_TYPE = "mysql"
    DEFAULT_PORT = 3306
    
    # MySQL isolation levels
    ISOLATION_LEVELS = {
        "read_uncommitted": "READ UNCOMMITTED",
        "read_committed": "READ COMMITTED",
        "repeatable_read": "REPEATABLE READ",
        "serializable": "SERIALIZABLE",
    }
    
    def __init__(self, config: ConnectionConfig):
        """Initialize the MySQL connector.
        
        Args:
            config: Connection configuration.
        
        Raises:
            ImportError: If pymysql is not installed.
        """
        if not PYMYSQL_AVAILABLE:
            raise ImportError(
                "pymysql is required for MySQL support. "
                "Install it with: pip install pymysql"
            )
        
        super().__init__(config)
        
        if config.port is None:
            config.port = self.DEFAULT_PORT
    
    def connect(self) -> Any:
        """Create a raw MySQL connection."""
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            charset='utf8mb4',
            cursorclass=DictCursor,
            connect_timeout=self.config.connect_timeout,
            autocommit=False,
            **self.config.extra
        )
        return conn
    
    def disconnect(self, connection: Any) -> None:
        """Close a MySQL connection."""
        if connection:
            connection.close()
    
    def execute(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        connection: Optional[Any] = None,
    ) -> QueryResult:
        """Execute a query."""
        import time
        
        start_time = time.time()
        should_close = False
        
        if connection is None:
            if self._pool:
                connection = self._pool.acquire()
            else:
                connection = self.connect()
            should_close = True
        
        try:
            with connection.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                # Fetch results if any
                rows = []
                columns = []
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    rows=rows,
                    columns=columns,
                    row_count=len(rows),
                    affected_rows=cursor.rowcount,
                    execution_time_ms=execution_time,
                )
        finally:
            if should_close:
                if self._pool:
                    self._pool.release(connection)
                else:
                    self.disconnect(connection)
    
    def executemany(
        self,
        query: str,
        parameters_list: List[Dict[str, Any]],
        connection: Optional[Any] = None,
    ) -> QueryResult:
        """Execute a query multiple times."""
        import time
        
        start_time = time.time()
        should_close = False
        
        if connection is None:
            if self._pool:
                connection = self._pool.acquire()
            else:
                connection = self.connect()
            should_close = True
        
        try:
            with connection.cursor() as cursor:
                cursor.executemany(query, parameters_list)
                
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    rows=[],
                    columns=[],
                    row_count=0,
                    affected_rows=cursor.rowcount,
                    execution_time_ms=execution_time,
                )
        finally:
            if should_close:
                if self._pool:
                    self._pool.release(connection)
                else:
                    self.disconnect(connection)
    
    def fetchiter(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        connection: Optional[Any] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch results as an iterator using server-side cursor."""
        # For MySQL, we create a new connection with SSCursor
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            charset='utf8mb4',
            cursorclass=SSCursor,
            connect_timeout=self.config.connect_timeout,
        )
        
        try:
            with conn.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                for row in cursor:
                    yield dict(zip(columns, row))
        finally:
            conn.close()
    
    def get_tables(self, schema: Optional[str] = None) -> List[TableInfo]:
        """Get list of tables."""
        database = schema or self.config.database
        
        query = """
            SELECT 
                TABLE_NAME as name,
                TABLE_SCHEMA as schema_name,
                TABLE_ROWS as row_count,
                DATA_LENGTH + INDEX_LENGTH as size_bytes
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        
        result = self.execute(query, {"database": database})
        
        tables = []
        for row in result.rows:
            table_info = TableInfo(
                name=row["name"],
                schema=row["schema_name"],
                row_count=row["row_count"],
                size_bytes=row["size_bytes"],
            )
            tables.append(table_info)
        
        # Get column info for each table
        for table in tables:
            table.columns = self.get_columns(table.name, table.schema)
            table.primary_key = self.get_primary_key(table.name, table.schema)
        
        return tables
    
    def get_columns(self, table_name: str, schema: Optional[str] = None) -> List[ColumnInfo]:
        """Get column information for a table."""
        database = schema or self.config.database
        
        query = """
            SELECT 
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as default_value,
                CHARACTER_MAXIMUM_LENGTH as max_length,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        
        result = self.execute(query, {"database": database, "table": table_name})
        
        columns = []
        for row in result.rows:
            column_info = ColumnInfo(
                name=row["name"],
                data_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default=row["default_value"],
                max_length=row["max_length"],
                is_primary_key=row["column_key"] == "PRI",
                is_unique=row["column_key"] in ("PRI", "UNI"),
            )
            columns.append(column_info)
        
        return columns
    
    def get_primary_key(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """Get primary key columns for a table."""
        database = schema or self.config.database
        
        query = """
            SELECT COLUMN_NAME as name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        
        result = self.execute(query, {"database": database, "table": table_name})
        return [row["name"] for row in result.rows]
    
    def scan_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Scan a table with server-side cursor."""
        database = schema or self.config.database
        
        # Build column list
        if columns:
            column_str = ", ".join(f'`{col}`' for col in columns)
        else:
            column_str = "*"
        
        query = f'SELECT {column_str} FROM `{database}`.`{table_name}`'
        
        return self.fetchiter(query, batch_size=batch_size)
    
    def update_rows(
        self,
        table_name: str,
        updates: List[Tuple[Dict[str, Any], Dict[str, Any]]],
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> int:
        """Update multiple rows efficiently."""
        database = schema or self.config.database
        total_updated = 0
        
        if self._pool is None:
            self.initialize_pool()
        
        connection = self._pool.acquire()
        
        try:
            with connection.cursor() as cursor:
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    
                    for where_conditions, set_values in batch:
                        # Build UPDATE statement
                        set_clause = ", ".join(f'`{k}` = %s' for k in set_values.keys())
                        where_clause = " AND ".join(f'`{k}` = %s' for k in where_conditions.keys())
                        
                        query = f'UPDATE `{database}`.`{table_name}` SET {set_clause} WHERE {where_clause}'
                        params = list(set_values.values()) + list(where_conditions.values())
                        
                        cursor.execute(query, params)
                        total_updated += cursor.rowcount
                    
                    connection.commit()
                    logger.debug(f"Batch update committed: {len(batch)} updates")
            
            return total_updated
        finally:
            self._pool.release(connection)
    
    def test_connection(self) -> bool:
        """Test if the database connection is working."""
        try:
            result = self.execute("SELECT 1 as test")
            return result.row_count == 1 and result.rows[0].get("test") == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def begin_transaction(self, connection: Any) -> None:
        """Begin a transaction."""
        # MySQL transactions are implicit
        pass
    
    def commit_transaction(self, connection: Any) -> None:
        """Commit a transaction."""
        connection.commit()
    
    def rollback_transaction(self, connection: Any) -> None:
        """Rollback a transaction."""
        connection.rollback()
