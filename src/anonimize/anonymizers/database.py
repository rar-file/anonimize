"""Database anonymizer.

This module provides anonymization functionality for databases using SQLAlchemy.
Supports SQLite, PostgreSQL, MySQL, and other SQLAlchemy-compatible databases.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

try:
    from sqlalchemy import MetaData, Table, create_engine, inspect, select, update
    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.exc import SQLAlchemyError

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    Engine = Any
    Connection = Any

from anonimize.anonymizers.base import BaseAnonymizer
from anonimize.core import Anonymizer

logger = logging.getLogger(__name__)


class DatabaseAnonymizer(BaseAnonymizer):
    """Anonymizer for database tables.

    This class provides methods to anonymize PII in database tables
    using SQLAlchemy for database abstraction.

    Attributes:
        engine: SQLAlchemy engine instance.
        batch_size: Number of rows to process per batch.

    Example:
        >>> anon = DatabaseAnonymizer("postgresql://user:pass@localhost/db")
        >>> config = {
        ...     "users": {
        ...         "name": {"strategy": "replace", "type": "name"},
        ...         "email": {"strategy": "hash", "type": "email"},
        ...     }
        ... }
        >>> anon.anonymize(config)
    """

    def __init__(
        self,
        connection_string: str,
        batch_size: int = 1000,
        config: Optional[Dict[str, Any]] = None,
        **engine_kwargs,
    ):
        """Initialize the database anonymizer.

        Args:
            connection_string: Database connection string (SQLAlchemy format).
            batch_size: Number of rows to process per batch.
            config: Default configuration.
            **engine_kwargs: Additional arguments for create_engine.

        Raises:
            ImportError: If SQLAlchemy is not installed.
            ValueError: If connection string is invalid.

        Example:
            >>> # SQLite
            >>> anon = DatabaseAnonymizer("sqlite:///mydb.db")
            >>>
            >>> # PostgreSQL
            >>> anon = DatabaseAnonymizer("postgresql://user:pass@localhost/db")
            >>>
            >>> # MySQL
            >>> anon = DatabaseAnonymizer("mysql+pymysql://user:pass@localhost/db")
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for DatabaseAnonymizer. "
                "Install it with: pip install sqlalchemy"
            )

        super().__init__(config)
        self.batch_size = batch_size
        self.connection_string = connection_string

        try:
            self.engine = create_engine(connection_string, **engine_kwargs)
            self.metadata = MetaData()
            self._core_anonymizer = Anonymizer()
            logger.info(
                f"DatabaseAnonymizer initialized with: {self._safe_connection_string()}"
            )
        except Exception as e:
            raise ValueError(f"Failed to create database engine: {e}")

    def _safe_connection_string(self) -> str:
        """Return a safe version of the connection string (without password).

        Returns:
            Connection string with password masked.
        """
        # Simple masking - remove password from connection string for logging
        import re

        return re.sub(r":([^:@]+)@", ":***@", self.connection_string)

    @contextmanager
    def _get_connection(self) -> Generator[Connection, None, None]:
        """Get a database connection context manager.

        Yields:
            SQLAlchemy connection object.
        """
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def anonymize(
        self,
        config: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
        tables: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Anonymize database tables.

        Args:
            config: Table and column anonymization configuration.
                Format: {table_name: {column_name: {strategy, type}}}
            tables: List of tables to anonymize (if None, uses config keys).
            **kwargs: Additional arguments.

        Returns:
            Statistics about the anonymization process.

        Raises:
            ValueError: If configuration is invalid.
            SQLAlchemyError: If database operation fails.

        Example:
            >>> anon = DatabaseAnonymizer("sqlite:///test.db")
            >>> config = {
            ...     "users": {
            ...         "name": {"strategy": "replace", "type": "name"},
            ...         "email": {"strategy": "replace", "type": "email"},
            ...     }
            ... }
            >>> stats = anon.anonymize(config)
        """
        config = config or self.config

        if tables is None:
            tables = list(config.keys())

        if not tables:
            raise ValueError("No tables specified for anonymization")

        total_records = 0
        total_fields = 0

        for table_name in tables:
            if table_name not in config:
                logger.warning(f"No configuration for table '{table_name}', skipping")
                continue

            table_config = config[table_name]

            # Validate configuration
            errors = self.validate_config(table_config)
            if errors:
                raise ValueError(
                    f"Invalid configuration for table '{table_name}': {'; '.join(errors)}"
                )

            logger.info(f"Anonymizing table: {table_name}")

            try:
                records, fields = self._anonymize_table(table_name, table_config)
                total_records += records
                total_fields += fields
            except SQLAlchemyError as e:
                logger.error(f"Error anonymizing table '{table_name}': {e}")
                raise

        self._update_stats(records=total_records, fields=total_fields)

        stats = {
            "records_processed": total_records,
            "fields_anonymized": total_fields,
            "tables_processed": len(tables),
        }

        logger.info(f"Database anonymization complete: {stats}")

        return stats

    def _anonymize_table(
        self, table_name: str, config: Dict[str, Dict[str, Any]]
    ) -> tuple[int, int]:
        """Anonymize a single table.

        Args:
            table_name: Name of the table.
            config: Column configuration.

        Returns:
            Tuple of (records_processed, fields_anonymized).
        """
        with self._get_connection() as conn:
            # Reflect table
            table = Table(table_name, self.metadata, autoload_with=self.engine)

            # Get columns to anonymize
            columns_to_anonymize = list(config.keys())

            # Verify columns exist
            available_columns = {c.name for c in table.columns}
            for col in columns_to_anonymize:
                if col not in available_columns:
                    logger.warning(
                        f"Column '{col}' not found in table '{table_name}', skipping"
                    )

            columns_to_anonymize = [
                c for c in columns_to_anonymize if c in available_columns
            ]

            if not columns_to_anonymize:
                logger.warning(f"No valid columns to anonymize in '{table_name}'")
                return 0, 0

            # Get primary key for updates
            primary_key = self._get_primary_key(table)

            if not primary_key:
                logger.warning(f"No primary key found for table '{table_name}'")
                return self._anonymize_table_without_pk(
                    conn, table, config, columns_to_anonymize
                )

            return self._anonymize_table_with_pk(
                conn, table, config, columns_to_anonymize, primary_key
            )

    def _get_primary_key(self, table: "Table") -> Optional[str]:
        """Get the primary key column name of a table.

        Args:
            table: SQLAlchemy Table object.

        Returns:
            Primary key column name or None.
        """
        pk_columns = [c.name for c in table.columns if c.primary_key]

        if len(pk_columns) == 1:
            return pk_columns[0]
        elif len(pk_columns) > 1:
            # Composite key - use first one
            return pk_columns[0]

        return None

    def _anonymize_table_with_pk(
        self,
        conn: "Connection",
        table: "Table",
        config: Dict[str, Dict[str, Any]],
        columns: List[str],
        primary_key: str,
    ) -> tuple[int, int]:
        """Anonymize table using primary key for updates.

        Args:
            conn: Database connection.
            table: Table object.
            config: Column configuration.
            columns: Columns to anonymize.
            primary_key: Primary key column name.

        Returns:
            Tuple of (records_processed, fields_anonymized).
        """
        pk_column = table.c[primary_key]

        # Build select query
        select_columns = [pk_column] + [table.c[c] for c in columns]
        stmt = select(*select_columns)

        records_processed = 0
        fields_anonymized = 0

        # Process in batches
        result = conn.execution_options(stream_results=True).execute(stmt)

        batch_updates = []

        for row in result:
            row_dict = dict(row._mapping)
            pk_value = row_dict[primary_key]

            # Anonymize row
            anonymized = self._core_anonymizer.anonymize(row_dict, config)

            # Prepare update
            update_values = {
                col: anonymized[col]
                for col in columns
                if anonymized[col] != row_dict[col]
            }

            if update_values:
                batch_updates.append((pk_value, update_values))
                fields_anonymized += len(update_values)

            records_processed += 1

            # Execute batch updates
            if len(batch_updates) >= self.batch_size:
                self._execute_batch_updates(conn, table, pk_column, batch_updates)
                batch_updates = []
                logger.debug(f"Processed {records_processed} records...")

        # Execute remaining updates
        if batch_updates:
            self._execute_batch_updates(conn, table, pk_column, batch_updates)

        return records_processed, fields_anonymized

    def _execute_batch_updates(
        self,
        conn: "Connection",
        table: "Table",
        pk_column: Any,
        batch: List[tuple],
    ) -> None:
        """Execute a batch of updates.

        Args:
            conn: Database connection.
            table: Table object.
            pk_column: Primary key column.
            batch: List of (pk_value, update_values) tuples.
        """
        for pk_value, values in batch:
            stmt = update(table).where(pk_column == pk_value).values(**values)
            conn.execute(stmt)

        conn.commit()

    def _anonymize_table_without_pk(
        self,
        conn: "Connection",
        table: "Table",
        config: Dict[str, Dict[str, Any]],
        columns: List[str],
    ) -> tuple[int, int]:
        """Anonymize table without primary key.

        Note: This method is less efficient and may not work correctly
        if there are duplicate rows.

        Args:
            conn: Database connection.
            table: Table object.
            config: Column configuration.
            columns: Columns to anonymize.

        Returns:
            Tuple of (records_processed, fields_anonymized).
        """
        logger.warning(
            "Processing table without primary key. "
            "This may be slow and could cause issues with duplicate rows."
        )

        select_columns = [table.c[c] for c in columns]
        stmt = select(*select_columns)

        result = conn.execute(stmt)
        rows = result.fetchall()

        records_processed = 0
        fields_anonymized = 0

        for row in rows:
            row_dict = dict(row._mapping)
            anonymized = self._core_anonymizer.anonymize(row_dict, config)

            # Build WHERE clause based on original values
            where_clause = [table.c[col] == row_dict[col] for col in columns]

            update_values = {col: anonymized[col] for col in columns}

            # Limit to first matching row
            stmt = update(table).where(*where_clause).values(**update_values)

            conn.execute(stmt)
            fields_anonymized += len(columns)
            records_processed += 1

        conn.commit()

        return records_processed, fields_anonymized

    def get_tables(self) -> List[str]:
        """Get list of tables in the database.

        Returns:
            List of table names.
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table.

        Args:
            table_name: Name of the table.

        Returns:
            List of column information dictionaries.
        """
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)

    def preview(
        self,
        table_name: str,
        config: Dict[str, Dict[str, Any]],
        num_rows: int = 5,
    ) -> List[Dict[str, Any]]:
        """Preview anonymization on a few rows without saving.

        Args:
            table_name: Name of the table.
            config: Column configuration.
            num_rows: Number of rows to preview.

        Returns:
            List of anonymized rows.
        """
        with self._get_connection() as conn:
            table = Table(table_name, self.metadata, autoload_with=self.engine)

            # Get columns to anonymize
            columns = list(config.keys())

            # Build select query
            select_columns = [table.c[c] for c in columns]
            stmt = select(*select_columns).limit(num_rows)

            result = conn.execute(stmt)

            preview_rows = []
            for row in result:
                row_dict = dict(row._mapping)
                anonymized = self._core_anonymizer.anonymize(row_dict, config)
                preview_rows.append(
                    {
                        "original": row_dict,
                        "anonymized": anonymized,
                    }
                )

            return preview_rows

    def close(self) -> None:
        """Close the database connection and dispose of the engine."""
        if hasattr(self, "engine"):
            self.engine.dispose()
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
