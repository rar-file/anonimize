"""Tests for database connectors base classes."""

import pytest
from unittest.mock import Mock, MagicMock

from anonimize.connectors.base import (
    ConnectionConfig,
    ConnectionPool,
    ColumnInfo,
    TableInfo,
    QueryResult,
    BaseConnector,
    Transaction,
)


class TestConnectionConfig:
    """Test ConnectionConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ConnectionConfig()

        assert config.host == "localhost"
        assert config.port is None
        assert config.database == ""
        assert config.user == ""
        assert config.password == ""
        assert config.ssl_mode is None
        assert config.connect_timeout == 30
        assert config.pool_size == 5
        assert config.pool_timeout == 30
        assert config.max_overflow == 10
        assert config.pool_recycle == 3600
        assert config.extra == {}

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ConnectionConfig(
            host="db.example.com",
            port=5432,
            database="mydb",
            user="admin",
            password="secret",
            pool_size=10,
        )

        assert config.host == "db.example.com"
        assert config.port == 5432
        assert config.database == "mydb"
        assert config.user == "admin"
        assert config.password == "secret"
        assert config.pool_size == 10


class TestConnectionPool:
    """Test ConnectionPool class."""

    def test_pool_initialization(self):
        """Test pool initialization."""
        mock_factory = Mock(return_value=Mock())
        config = ConnectionConfig(pool_size=5)

        pool = ConnectionPool(factory=mock_factory, config=config)

        assert pool._max_size == 5
        assert pool._max_overflow == 10
        assert pool._total_created >= 0

    def test_pool_acquire_release(self):
        """Test acquiring and releasing connections."""
        mock_conn = Mock()
        mock_factory = Mock(return_value=mock_conn)
        config = ConnectionConfig(pool_size=2)

        pool = ConnectionPool(factory=mock_factory, config=config)

        # Acquire connection
        conn = pool.acquire(timeout=1.0)
        assert conn is mock_conn
        assert len(pool._in_use) == 1

        # Release connection
        pool.release(conn)
        assert len(pool._in_use) == 0
        assert len(pool._pool) == 1

    def test_pool_stats(self):
        """Test pool statistics."""
        mock_conn = Mock()
        mock_factory = Mock(return_value=mock_conn)
        config = ConnectionConfig(pool_size=3)

        pool = ConnectionPool(factory=mock_factory, config=config)

        stats = pool.get_stats()

        assert "pool_size" in stats
        assert "available" in stats
        assert "in_use" in stats
        assert "overflow" in stats
        assert "total_created" in stats

    def test_pool_close_all(self):
        """Test closing all connections."""
        mock_conn = Mock()
        mock_factory = Mock(return_value=mock_conn)
        config = ConnectionConfig(pool_size=2)

        pool = ConnectionPool(factory=mock_factory, config=config)
        pool.close_all()

        assert len(pool._pool) == 0
        assert pool._overflow == 0


class TestColumnInfo:
    """Test ColumnInfo dataclass."""

    def test_basic_creation(self):
        """Test basic column info creation."""
        col = ColumnInfo(name="id", data_type="INTEGER")

        assert col.name == "id"
        assert col.data_type == "INTEGER"
        assert col.nullable is True
        assert col.default is None
        assert col.max_length is None
        assert col.is_primary_key is False
        assert col.is_unique is False

    def test_full_creation(self):
        """Test column info with all fields."""
        col = ColumnInfo(
            name="email",
            data_type="VARCHAR",
            nullable=False,
            default="",
            max_length=255,
            is_primary_key=False,
            is_unique=True,
        )

        assert col.nullable is False
        assert col.default == ""
        assert col.max_length == 255
        assert col.is_unique is True


class TestTableInfo:
    """Test TableInfo dataclass."""

    def test_basic_creation(self):
        """Test basic table info creation."""
        table = TableInfo(name="users")

        assert table.name == "users"
        assert table.schema is None
        assert table.columns == []
        assert table.primary_key == []
        assert table.row_count is None
        assert table.size_bytes is None

    def test_with_columns(self):
        """Test table info with columns."""
        columns = [
            ColumnInfo(name="id", data_type="INTEGER", is_primary_key=True),
            ColumnInfo(name="name", data_type="VARCHAR"),
        ]

        table = TableInfo(
            name="users",
            schema="public",
            columns=columns,
            primary_key=["id"],
            row_count=1000,
        )

        assert len(table.columns) == 2
        assert table.primary_key == ["id"]
        assert table.row_count == 1000


class TestQueryResult:
    """Test QueryResult dataclass."""

    def test_default_creation(self):
        """Test default query result."""
        result = QueryResult()

        assert result.rows == []
        assert result.columns == []
        assert result.row_count == 0
        assert result.affected_rows == 0
        assert result.execution_time_ms == 0.0

    def test_with_data(self):
        """Test query result with data."""
        result = QueryResult(
            rows=[{"id": 1, "name": "John"}],
            columns=["id", "name"],
            row_count=1,
            affected_rows=1,
            execution_time_ms=15.5,
        )

        assert result.row_count == 1
        assert result.execution_time_ms == 15.5


class MockConnector(BaseConnector):
    """Mock connector for testing."""

    DB_TYPE = "mock"

    def connect(self):
        return Mock()

    def disconnect(self, connection):
        pass

    def execute(self, query, parameters=None, connection=None):
        return QueryResult(rows=[{"test": 1}])

    def executemany(self, query, parameters_list, connection=None):
        return QueryResult(affected_rows=len(parameters_list))

    def fetchiter(self, query, parameters=None, batch_size=1000, connection=None):
        yield {"test": 1}

    def get_tables(self, schema=None):
        return []

    def get_columns(self, table_name, schema=None):
        return []

    def get_primary_key(self, table_name, schema=None):
        return []

    def scan_table(self, table_name, columns=None, schema=None, batch_size=1000):
        yield {}

    def update_rows(self, table_name, updates, schema=None, batch_size=1000):
        return len(updates)

    def test_connection(self):
        return True

    def begin_transaction(self, connection):
        pass

    def commit_transaction(self, connection):
        pass

    def rollback_transaction(self, connection):
        pass


class TestBaseConnector:
    """Test BaseConnector class."""

    def test_initialization(self):
        """Test connector initialization."""
        config = ConnectionConfig()
        connector = MockConnector(config)

        assert connector.config == config
        assert connector._pool is None
        assert connector._closed is False

    def test_context_manager(self):
        """Test context manager usage."""
        config = ConnectionConfig()

        with MockConnector(config) as connector:
            assert connector._closed is False

        assert connector._closed is True

    def test_get_pool_stats_not_initialized(self):
        """Test getting pool stats before initialization."""
        config = ConnectionConfig()
        connector = MockConnector(config)

        stats = connector.get_pool_stats()
        assert stats["status"] == "not_initialized"


class TestTransaction:
    """Test Transaction class."""

    def test_initialization(self):
        """Test transaction initialization."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)

        assert txn._connector == connector
        assert txn._connection == connection
        assert txn._active is False
        assert txn._queries_executed == 0

    def test_transaction_lifecycle(self):
        """Test transaction begin, commit."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)

        txn.begin()
        assert txn._active is True

        txn.commit()
        assert txn._active is False

    def test_transaction_rollback(self):
        """Test transaction rollback."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)

        txn.begin()
        txn.rollback()
        assert txn._active is False

    def test_execute_not_active(self):
        """Test executing on inactive transaction."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)

        with pytest.raises(RuntimeError, match="Transaction is not active"):
            txn.execute("SELECT 1")

    def test_execute_active(self):
        """Test executing on active transaction."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)
        txn.begin()

        result = txn.execute("SELECT 1")

        assert result is not None
        assert txn._queries_executed == 1

    def test_get_stats(self):
        """Test getting transaction stats."""
        connector = MockConnector(ConnectionConfig())
        connection = Mock()

        txn = Transaction(connector, connection)
        txn.begin()
        txn.execute("SELECT 1")

        stats = txn.get_stats()

        assert stats["active"] is True
        assert stats["queries_executed"] == 1
        assert stats["readonly"] is False
