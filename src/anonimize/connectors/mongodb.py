"""MongoDB database connector.

This module provides a MongoDB connector with support for
document scanning and batch operations.
"""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient
    from pymongo.collection import Collection
    from pymongo.database import Database
    from pymongo.errors import ConnectionFailure, PyMongoError

    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    MongoClient = None
    ASCENDING = None
    DESCENDING = None
    Collection = None
    Database = None
    PyMongoError = None
    ConnectionFailure = None

from anonimize.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectionConfig,
    QueryResult,
    TableInfo,
)

logger = logging.getLogger(__name__)


class MongoDBConnector(BaseConnector):
    """MongoDB database connector.

    This connector provides MongoDB support with:
    - Connection pooling via MongoClient
    - Batch document operations
    - Cursor-based iteration for large collections
    - Projection support for efficient scanning

    Example:
        >>> config = ConnectionConfig(
        ...     host="localhost",
        ...     port=27017,
        ...     database="mydb",
        ...     user="admin",
        ...     password="secret",
        ... )
        >>> connector = MongoDBConnector(config)
        >>> with connector.connection() as client:
        ...     db = client.mydb
        ...     docs = list(db.users.find().limit(10))
    """

    DB_TYPE = "mongodb"
    DEFAULT_PORT = 27017

    def __init__(self, config: ConnectionConfig, **client_options):
        """Initialize the MongoDB connector.

        Args:
            config: Connection configuration.
            **client_options: Additional MongoClient options.

        Raises:
            ImportError: If pymongo is not installed.
        """
        if not PYMONGO_AVAILABLE:
            raise ImportError(
                "pymongo is required for MongoDB support. "
                "Install it with: pip install pymongo"
            )

        super().__init__(config)

        if config.port is None:
            config.port = self.DEFAULT_PORT

        self._client_options = client_options
        self._client: Optional[MongoClient] = None

    def _build_connection_uri(self) -> str:
        """Build MongoDB connection URI."""
        if self.config.user and self.config.password:
            auth = f"{self.config.user}:{self.config.password}@"
        else:
            auth = ""

        uri = f"mongodb://{auth}{self.config.host}:{self.config.port}"

        if self.config.database:
            uri += f"/{self.config.database}"

        return uri

    def connect(self) -> MongoClient:
        """Create a MongoDB client connection."""
        uri = self._build_connection_uri()

        options = {
            "maxPoolSize": self.config.pool_size,
            "minPoolSize": 1,
            "maxIdleTimeMS": self.config.pool_recycle * 1000,
            "waitQueueTimeoutMS": self.config.pool_timeout * 1000,
            "serverSelectionTimeoutMS": self.config.connect_timeout * 1000,
            **self._client_options,
        }

        client = MongoClient(uri, **options)
        return client

    def disconnect(self, connection: MongoClient) -> None:
        """Close a MongoDB client connection."""
        if connection:
            connection.close()

    def initialize_pool(self) -> None:
        """Initialize the MongoDB client."""
        if self._client is None:
            self._client = self.connect()
            logger.info("Initialized MongoDB client")

    def _get_database(
        self, client: MongoClient, database_name: Optional[str] = None
    ) -> Database:
        """Get a database from the client."""
        db_name = database_name or self.config.database
        if not db_name:
            raise ValueError("Database name not specified")
        return client[db_name]

    def _get_collection(
        self,
        client: MongoClient,
        collection_name: str,
        database_name: Optional[str] = None,
    ) -> Collection:
        """Get a collection from the client."""
        db = self._get_database(client, database_name)
        return db[collection_name]

    def execute(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        connection: Optional[MongoClient] = None,
    ) -> QueryResult:
        """Execute a query (MongoDB aggregation pipeline).

        Note: The 'query' parameter should be a collection name,
        and 'parameters' should contain:
            - pipeline: The aggregation pipeline
            - database: Optional database name
        """
        import time

        start_time = time.time()
        should_close = False

        if connection is None:
            if self._client:
                connection = self._client
            else:
                connection = self.connect()
                should_close = True

        try:
            collection_name = query
            pipeline = parameters.get("pipeline", []) if parameters else []
            database_name = parameters.get("database") if parameters else None

            collection = self._get_collection(
                connection, collection_name, database_name
            )

            result = list(collection.aggregate(pipeline))

            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                rows=result,
                columns=list(result[0].keys()) if result else [],
                row_count=len(result),
                affected_rows=0,
                execution_time_ms=execution_time,
            )
        finally:
            if should_close:
                self.disconnect(connection)

    def executemany(
        self,
        query: str,
        parameters_list: List[Dict[str, Any]],
        connection: Optional[MongoClient] = None,
    ) -> QueryResult:
        """Execute bulk operations.

        Args:
            query: Collection name.
            parameters_list: List of operations (each with 'operation' and 'document').
            connection: Optional MongoDB client.
        """
        import time

        start_time = time.time()
        should_close = False

        if connection is None:
            if self._client:
                connection = self._client
            else:
                connection = self.connect()
                should_close = True

        try:
            collection_name = query
            database_name = (
                parameters_list[0].get("database") if parameters_list else None
            )

            collection = self._get_collection(
                connection, collection_name, database_name
            )

            # Build bulk operations
            operations = []
            for params in parameters_list:
                op = params.get("operation", "insert")
                doc = params.get("document", {})
                filter_doc = params.get("filter", {})

                if op == "insert":
                    operations.append(doc)
                elif op == "update":
                    collection.update_one(
                        filter_doc, {"$set": doc}, upsert=params.get("upsert", False)
                    )
                elif op == "delete":
                    collection.delete_one(filter_doc)

            # Bulk insert
            if operations:
                result = collection.insert_many(operations)
                affected_rows = len(result.inserted_ids)
            else:
                affected_rows = len(parameters_list)

            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                rows=[],
                columns=[],
                row_count=0,
                affected_rows=affected_rows,
                execution_time_ms=execution_time,
            )
        finally:
            if should_close:
                self.disconnect(connection)

    def fetchiter(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        connection: Optional[MongoClient] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch documents as an iterator."""
        should_close = False

        if connection is None:
            if self._client:
                connection = self._client
            else:
                connection = self.connect()
                should_close = True

        try:
            collection_name = query
            filter_doc = parameters.get("filter", {}) if parameters else {}
            projection = parameters.get("projection") if parameters else None
            database_name = parameters.get("database") if parameters else None

            collection = self._get_collection(
                connection, collection_name, database_name
            )

            cursor = collection.find(filter_doc, projection).batch_size(batch_size)

            for doc in cursor:
                # Convert ObjectId to string for JSON serialization
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                yield doc
        finally:
            if should_close:
                self.disconnect(connection)

    def get_tables(self, schema: Optional[str] = None) -> List[TableInfo]:
        """Get list of collections (treated as tables)."""
        client = self._client or self.connect()
        should_close = self._client is None

        try:
            db = self._get_database(client, schema)
            collection_names = db.list_collection_names()

            tables = []
            for name in collection_names:
                collection = db[name]

                # Get document count
                try:
                    row_count = collection.estimated_document_count()
                except Exception:
                    row_count = collection.count_documents({})

                # Get stats
                try:
                    stats = db.command("collStats", name)
                    size_bytes = stats.get("size", 0)
                except Exception:
                    size_bytes = None

                table_info = TableInfo(
                    name=name,
                    schema=schema or self.config.database,
                    row_count=row_count,
                    size_bytes=size_bytes,
                )
                tables.append(table_info)

            # Sample documents for schema inference
            for table in tables:
                table.columns = self.get_columns(table.name, schema)

            return tables
        finally:
            if should_close:
                self.disconnect(client)

    def get_columns(
        self, collection_name: str, schema: Optional[str] = None
    ) -> List[ColumnInfo]:
        """Infer schema by sampling documents."""
        client = self._client or self.connect()
        should_close = self._client is None

        try:
            collection = self._get_collection(client, collection_name, schema)

            # Sample documents to infer schema
            sample_docs = list(collection.find().limit(100))

            if not sample_docs:
                return []

            # Collect all unique fields and their types
            field_info: Dict[str, Dict[str, Any]] = {}

            for doc in sample_docs:
                for key, value in doc.items():
                    if key not in field_info:
                        field_info[key] = {
                            "types": set(),
                            "nullable": False,
                        }

                    value_type = type(value).__name__
                    field_info[key]["types"].add(value_type)

                    if value is None:
                        field_info[key]["nullable"] = True

            columns = []
            for name, info in field_info.items():
                # Determine data type (most common or mixed)
                types = info["types"]
                if len(types) == 1:
                    data_type = list(types)[0]
                else:
                    data_type = f"mixed({', '.join(sorted(types))})"

                column_info = ColumnInfo(
                    name=name,
                    data_type=data_type,
                    nullable=info["nullable"],
                    is_primary_key=(name == "_id"),
                )
                columns.append(column_info)

            return columns
        finally:
            if should_close:
                self.disconnect(client)

    def get_primary_key(
        self, collection_name: str, schema: Optional[str] = None
    ) -> List[str]:
        """Get primary key fields for a collection.

        MongoDB uses _id as the default primary key.
        """
        return ["_id"]

    def scan_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Scan a collection (table)."""
        # Build projection
        projection = None
        if columns:
            projection = {col: 1 for col in columns}
            projection["_id"] = 1  # Always include _id

        return self.fetchiter(
            table_name,
            {"projection": projection, "database": schema},
            batch_size=batch_size,
        )

    def update_rows(
        self,
        table_name: str,
        updates: List[Tuple[Dict[str, Any], Dict[str, Any]]],
        schema: Optional[str] = None,
        batch_size: int = 1000,
    ) -> int:
        """Update multiple documents.

        Args:
            table_name: Collection name.
            updates: List of (filter, update_values) tuples.
            schema: Optional database name.
            batch_size: Number of updates per batch.

        Returns:
            Number of documents updated.
        """
        client = self._client or self.connect()
        should_close = self._client is None

        try:
            collection = self._get_collection(client, table_name, schema)

            total_updated = 0

            for i in range(0, len(updates), batch_size):
                batch = updates[i : i + batch_size]

                for filter_doc, update_values in batch:
                    result = collection.update_one(
                        filter_doc, {"$set": update_values}, upsert=False
                    )
                    total_updated += result.modified_count

                logger.debug(f"Batch update: {len(batch)} documents")

            return total_updated
        finally:
            if should_close:
                self.disconnect(client)

    def bulk_update(
        self,
        collection_name: str,
        updates: List[Tuple[Dict[str, Any], Dict[str, Any]]],
        schema: Optional[str] = None,
    ) -> int:
        """Bulk update documents using bulk_write."""
        from pymongo import UpdateOne

        client = self._client or self.connect()
        should_close = self._client is None

        try:
            collection = self._get_collection(client, collection_name, schema)

            operations = [
                UpdateOne(filter_doc, {"$set": update_values})
                for filter_doc, update_values in updates
            ]

            result = collection.bulk_write(operations)
            return result.modified_count
        finally:
            if should_close:
                self.disconnect(client)

    def test_connection(self) -> bool:
        """Test if the MongoDB connection is working."""
        try:
            client = self.connect()
            # Ping the server
            client.admin.command("ping")
            self.disconnect(client)
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def begin_transaction(self, connection: MongoClient) -> None:
        """Begin a transaction."""
        # MongoDB 4.0+ supports multi-document transactions
        # This is handled at session level
        pass

    def commit_transaction(self, connection: MongoClient) -> None:
        """Commit a transaction."""
        # Transactions are handled via sessions in MongoDB
        pass

    def rollback_transaction(self, connection: MongoClient) -> None:
        """Rollback a transaction."""
        # Transactions are handled via sessions in MongoDB
        pass

    def create_index(
        self,
        collection_name: str,
        keys: List[Tuple[str, int]],
        schema: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Create an index on a collection.

        Args:
            collection_name: Name of the collection.
            keys: List of (field, direction) tuples.
            schema: Optional database name.
            **kwargs: Additional index options.

        Returns:
            Index name.
        """
        client = self._client or self.connect()
        should_close = self._client is None

        try:
            collection = self._get_collection(client, collection_name, schema)
            result = collection.create_index(keys, **kwargs)
            return result
        finally:
            if should_close:
                self.disconnect(client)

    def close(self) -> None:
        """Close the connector and release all resources."""
        if self._client:
            self._client.close()
            self._client = None
        super().close()
