"""PostgreSQL database connector."""

from typing import Any, Dict, Iterator, List, Optional

from anonimize.connectors.base import DatabaseConnector


class PostgreSQLConnector(DatabaseConnector):
    """Connector for PostgreSQL databases.

    Requires psycopg2-binary to be installed.

    Example:
        >>> conn_str = "postgresql://user:pass@localhost/db"
        >>> with PostgreSQLConnector(conn_str) as conn:
        ...     tables = conn.get_tables()
    """

    def connect(self) -> None:
        """Connect to PostgreSQL."""
        try:
            import psycopg2

            self._connection = psycopg2.connect(self.connection_string)
        except ImportError:
            raise ImportError(
                "psycopg2-binary required. Install with: pip install psycopg2-binary"
            )

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def get_tables(self) -> List[str]:
        """Get all table names."""
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column info for table."""
        cursor = self._connection.cursor()
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
        """,
            (table,),
        )

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                    "primary_key": False,  # Would need separate query
                }
            )
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
        """Write data using COPY for efficiency."""
        if not data:
            return 0

        import csv
        import io

        columns = list(data[0].keys())
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns, delimiter="\t")
        writer.writerows(data)
        buffer.seek(0)

        cursor = self._connection.cursor()
        cursor.copy_from(buffer, table, columns=columns, null="")
        self._connection.commit()

        return len(data)

    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute raw SQL."""
        cursor = self._connection.cursor()
        cursor.execute(query, params or ())
        self._connection.commit()
        return cursor.fetchall()
