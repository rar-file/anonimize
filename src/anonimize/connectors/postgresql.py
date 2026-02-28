"""PostgreSQL database connector.

This module provides a production-grade PostgreSQL connector with
full support for table scanning, connection pooling, and transactions.
"""

import logging
import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2 import extras, sql
    from psycopg2.pool import ThreadedConnectionPool

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None
    sql = None
    extras = None
    ThreadedConnectionPool = None

from anonimize.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectionConfig,
    QueryResult,
    TableInfo,
)

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector.

    This connector provides optimized PostgreSQL support with:
    - Server-side cursors for large result sets
    - Efficient batch updates with COPY protocol support
    - Table scanning with progress tracking
    - Connection pooling

    Example:
        >>> config = ConnectionConfig(
        ...     host="localhost",
        ...     port=5432,
        ...     database="mydb",
        ...     user="postgres",
        ...     password="secret",
        ... )
        >>> connector = PostgreSQLConnector(config)
        >>> with connector.transaction() as txn:
        ...     result = txn.query("SELECT * FROM users LIMIT 10")
    """

    DB_TYPE = "postgresql"
    DEFAULT_PORT = 5432

    # PostgreSQL isolation levels
    ISOLATION_LEVELS = {
        "read_uncommitted": "READ UNCOMMITTED",
        "read_committed": "READ COMMITTED",
        "repeatable_read": "REPEATABLE READ",
        "serializable": "SERIALIZABLE",
    }

    def __init__(self, config: ConnectionConfig):
        """Initialize the PostgreSQL connector.

        Args:
            config: Connection configuration.

        Raises:
            ImportError: If psycopg2 is not installed.
        """
        if not PSYCOPG2_AVAILABLE:
            raise ImportError(
                "psycopg2-binary is required for PostgreSQL support. "
                "Install it with: pip install psycopg2-binary"
            )

        super().__init__(config)
        self._pg_pool = None
        self._lock = threading.RLock()

        if config.port is None:
            config.port = self.DEFAULT_PORT

    def _get_connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        conn_str = (
            f"host={self.config.host} "
            f"port={self.config.port} "
            f"dbname={self.config.database} "
            f"user={self.config.user} "
            f"password={self.config.password}"
        )

        if self.config.ssl_mode:
            conn_str += f" sslmode={self.config.ssl_mode}"

        if self.config.connect_timeout:
            conn_str += f" connect_timeout={self.config.connect_timeout}"

        return conn_str

    def connect(self) -> Any:
        """Create a raw PostgreSQL connection."""
        conn = psycopg2.connect(self._get_connection_string())
        conn.autocommit = False

        # Register UUID and other adapters
        extras.register_uuid(conn_or_curs=conn)

        return conn

    def disconnect(self, connection: Any) -> None:
        """Close a PostgreSQL connection."""
        if connection:
            connection.close()

    def initialize_pool(self) -> None:
        """Initialize psycopg2 threaded connection pool."""
        if self._pg_pool is None:
            with self._lock:
                if self._pg_pool is None:
                    self._pg_pool = ThreadedConnectionPool(
                        minconn=1,
                        maxconn=self.config.pool_size + self.config.max_overflow,
                        host=self.config.host,
                        port=self.config.port,
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password,
                        sslmode=self.config.ssl_mode or "prefer",
                        connect_timeout=self.config.connect_timeout,
                    )
                    logger.info("Initialized PostgreSQL connection pool")

    def _get_cursor(self, connection: Any, name: Optional[str] = None) -> Any:
        """Get a cursor, optionally named for server-side cursor.

        Args:
            connection: The database connection.
            name: Optional cursor name for server-side cursor.

        Returns:
            A database cursor.
        """
        if name:
            # Server-side cursor for large results
            return connection.cursor(name=name)
        return connection.cursor()

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
            self.initialize_pool()
            connection = self._pg_pool.getconn()
            should_close = True

        try:
            cursor = self._get_cursor(connection)

            try:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)

                # Fetch results if any
                rows = []
                columns = []

                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

                execution_time = (time.time() - start_time) * 1000

                return QueryResult(
                    rows=rows,
                    columns=columns,
                    row_count=len(rows),
                    affected_rows=cursor.rowcount,
                    execution_time_ms=execution_time,
                )
            finally:
                cursor.close()
        finally:
            if should_close and self._pg_pool:
                self._pg_pool.putconn(connection)

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
            self.initialize_pool()
            connection = self._pg_pool.getconn()
            should_close = True

        try:
            cursor = self._get_cursor(connection)

            try:
                cursor.executemany(query, parameters_list)

                execution_time = (time.time() - start_time) * 1000

                return QueryResult(
                    rows=[],
                    columns=[],
                    row_count=0,
                    affected_rows=cursor.rowcount * len(parameters_list),
                    execution_time_ms=execution_time,
                )
            finally:
                cursor.close()
        finally:
            if should_close and self._pg_pool:
                self._pg_pool.putconn(connection)

    def fetchiter(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        connection: Optional[Any] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch results as an iterator using server-side cursor."""
        import uuid

        should_close = False

        if connection is None:
            self.initialize_pool()
            connection = self._pg_pool.getconn()
            should_close = True

        cursor_name = f"cursor_{uuid.uuid4().hex[:16]}"

        try:
            cursor = self._get_cursor(connection, name=cursor_name)
            cursor.itersize = batch_size

            try:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)

                columns = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )

                for row in cursor:
                    yield dict(zip(columns, row))
            finally:
                cursor.close()
        finally:
            if should_close and self._pg_pool:
                self._pg_pool.putconn(connection)

    def get_tables(self, schema: Optional[str] = None) -> List[TableInfo]:
        """Get list of tables."""
        schema_filter = schema or "public"

        query = """
            SELECT 
                schemaname as schema,
                tablename as name,
                pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) as size_bytes,
                (SELECT reltuples::bigint 
                 FROM pg_class 
                 WHERE oid = (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass) as row_count
            FROM pg_tables
            WHERE schemaname = %s
            ORDER BY tablename
        """

        result = self.execute(query, {"schema": schema_filter})

        tables = []
        for row in result.rows:
            table_info = TableInfo(
                name=row["name"],
                schema=row["schema"],
                row_count=row["row_count"],
                size_bytes=row["size_bytes"],
            )
            tables.append(table_info)

        # Get column info for each table
        for table in tables:
            table.columns = self.get_columns(table.name, table.schema)
            table.primary_key = self.get_primary_key(table.name, table.schema)

        return tables

    def get_columns(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[ColumnInfo]:
        """Get column information for a table."""
        schema_name = schema or "public"

        query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END as is_primary_key,
                CASE WHEN u.column_name IS NOT NULL THEN TRUE ELSE FALSE END as is_unique
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name, ku.table_name, ku.table_schema
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku 
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name 
                AND c.table_name = pk.table_name 
                AND c.table_schema = pk.table_schema
            LEFT JOIN (
                SELECT ku.column_name, ku.table_name, ku.table_schema
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku 
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'UNIQUE'
            ) u ON c.column_name = u.column_name 
                AND c.table_name = u.table_name 
                AND c.table_schema = u.table_schema
            WHERE c.table_name = %s AND c.table_schema = %s
            ORDER BY c.ordinal_position
        """

        result = self.execute(query, {"table": table_name, "schema": schema_name})

        columns = []
        for row in result.rows:
            column_info = ColumnInfo(
                name=row["column_name"],
                data_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default=row["column_default"],
                max_length=row["character_maximum_length"],
                is_primary_key=row["is_primary_key"],
                is_unique=row["is_unique"],
            )
            columns.append(column_info)

        return columns

    def get_primary_key(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[str]:
        """Get primary key columns for a table."""
        schema_name = schema or "public"

        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = %s
                AND tc.table_schema = %s
            ORDER BY kcu.ordinal_position
        """

        result = self.execute(query, {"table": table_name, "schema": schema_name})
        return [row["column_name"] for row in result.rows]

    def scan_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Scan a table with server-side cursor for memory efficiency.

        This is optimized for large table scanning with proper memory management.
        """
        schema_name = schema or "public"

        # Build column list
        if columns:
            column_str = ", ".join(f'"{col}"' for col in columns)
        else:
            column_str = "*"

        query = f'SELECT {column_str} FROM "{schema_name}"."{table_name}"'

        return self.fetchiter(query, batch_size=batch_size)

    def update_rows(
        self,
        table_name: str,
        updates: List[Tuple[Dict[str, Any], Dict[str, Any]]],
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> int:
        """Update multiple rows efficiently using batch updates.

        Args:
            table_name: Name of the table.
            updates: List of (where_conditions, set_values) tuples.
            schema: Optional schema name.
            batch_size: Number of updates per batch.

        Returns:
            Number of rows updated.
        """
        schema_name = schema or "public"
        total_updated = 0

        self.initialize_pool()
        connection = self._pg_pool.getconn()

        try:
            cursor = connection.cursor()

            try:
                batch = []

                for where_conditions, set_values in updates:
                    if not batch:
                        connection.commit()  # Ensure clean state

                    # Build UPDATE statement
                    set_clause = ", ".join(f'"{k}" = %s' for k in set_values.keys())
                    where_clause = " AND ".join(
                        f'"{k}" = %s' for k in where_conditions.keys()
                    )

                    query = f'UPDATE "{schema_name}"."{table_name}" SET {set_clause} WHERE {where_clause}'
                    params = list(set_values.values()) + list(where_conditions.values())

                    cursor.execute(query, params)
                    total_updated += cursor.rowcount

                    # Commit in batches
                    if len(batch) >= batch_size:
                        connection.commit()
                        batch = []
                    else:
                        batch.append(1)

                # Final commit
                if batch:
                    connection.commit()

                return total_updated
            finally:
                cursor.close()
        finally:
            self._pg_pool.putconn(connection)

    def bulk_insert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> int:
        """Efficient bulk insert using COPY protocol.

        Args:
            table_name: Name of the table.
            data: List of row dictionaries.
            schema: Optional schema name.
            columns: Optional column list.

        Returns:
            Number of rows inserted.
        """
        if not data:
            return 0

        schema_name = schema or "public"

        if columns is None:
            columns = list(data[0].keys())

        self.initialize_pool()
        connection = self._pg_pool.getconn()

        try:
            # Use COPY for efficient bulk insert
            import io

            buffer = io.StringIO()
            for row in data:
                values = []
                for col in columns:
                    val = row.get(col)
                    if val is None:
                        values.append("\\N")
                    else:
                        # Escape special characters
                        val_str = (
                            str(val)
                            .replace("\\", "\\\\")
                            .replace("\t", "\\t")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r")
                        )
                        values.append(val_str)
                buffer.write("\t".join(values) + "\n")

            buffer.seek(0)

            column_str = ", ".join(f'"{col}"' for col in columns)

            with connection.cursor() as cursor:
                cursor.copy_from(
                    buffer,
                    f'"{schema_name}"."{table_name}"',
                    columns=columns,
                    sep="\t",
                    null="\\N",
                )
                connection.commit()
                return len(data)
        finally:
            self._pg_pool.putconn(connection)

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
        # psycopg2 handles transactions automatically
        pass

    def commit_transaction(self, connection: Any) -> None:
        """Commit a transaction."""
        connection.commit()

    def rollback_transaction(self, connection: Any) -> None:
        """Rollback a transaction."""
        connection.rollback()

    def analyze_table(self, table_name: str, schema: Optional[str] = None) -> None:
        """Run ANALYZE on a table for query optimization.

        Args:
            table_name: Name of the table.
            schema: Optional schema name.
        """
        schema_name = schema or "public"
        self.execute(f'ANALYZE "{schema_name}"."{table_name}"')

    def vacuum_table(
        self, table_name: str, schema: Optional[str] = None, analyze: bool = True
    ) -> None:
        """Run VACUUM on a table.

        Note: VACUUM cannot run inside a transaction block.

        Args:
            table_name: Name of the table.
            schema: Optional schema name.
            analyze: Whether to also run ANALYZE.
        """
        schema_name = schema or "public"
        vacuum_cmd = "VACUUM"
        if analyze:
            vacuum_cmd += " ANALYZE"
        vacuum_cmd += f' "{schema_name}"."{table_name}"'

        # VACUUM requires autocommit
        self.initialize_pool()
        connection = self._pg_pool.getconn()
        old_autocommit = connection.autocommit

        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(vacuum_cmd)
        finally:
            connection.autocommit = old_autocommit
            self._pg_pool.putconn(connection)

    def close(self) -> None:
        """Close the connector and release all resources."""
        if self._pg_pool:
            self._pg_pool.closeall()
            self._pg_pool = None
        super().close()
