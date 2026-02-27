"""SQLite database connector."""

import sqlite3
from typing import Any, Dict, Iterator, List, Optional

from anonimize.connectors.base import DatabaseConnector


class SQLiteConnector(DatabaseConnector):
    """Connector for SQLite databases.
    
    Supports both file-based and in-memory databases.
    
    Example:
        >>> with SQLiteConnector("data.db") as conn:
        ...     tables = conn.get_tables()
        ...     for batch in conn.read_table("users"):
        ...         process(batch)
    """
    
    def connect(self) -> None:
        """Connect to SQLite database."""
        # Handle :memory: and file paths
        db_path = self.connection_string.replace("sqlite://", "").replace("sqlite:///", "")
        self._connection = sqlite3.connect(db_path)
        self._connection.row_factory = sqlite3.Row
    
    def disconnect(self) -> None:
        """Close SQLite connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def get_tables(self) -> List[str]:
        """Get all table names."""
        cursor = self._connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column info for table."""
        cursor = self._connection.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "name": row[1],
                "type": row[2],
                "nullable": not row[3],
                "default": row[4],
                "primary_key": bool(row[5])
            })
        return columns
    
    def read_table(self, table: str, batch_size: int = 1000) -> Iterator[List[Dict]]:
        """Read table in batches."""
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        
        columns = [desc[0] for desc in cursor.description]
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]
    
    def write_table(self, table: str, data: List[Dict]) -> int:
        """Write data to table."""
        if not data:
            return 0
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        
        cursor = self._connection.cursor()
        rows = [(tuple(row.get(col) for col in columns) for row in data)]
        cursor.executemany(query, rows[0])
        self._connection.commit()
        
        return cursor.rowcount
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute raw SQL."""
        cursor = self._connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        self._connection.commit()
        return cursor.fetchall()
    
    def create_table_from_data(self, table: str, data: List[Dict]) -> None:
        """Create table schema from sample data."""
        if not data:
            return
        
        # Infer types from first row
        columns = []
        for key, value in data[0].items():
            if isinstance(value, int):
                col_type = "INTEGER"
            elif isinstance(value, float):
                col_type = "REAL"
            else:
                col_type = "TEXT"
            columns.append(f"{key} {col_type}")
        
        query = f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {', '.join(columns)})"
        self.execute(query)
