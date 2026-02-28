"""Parquet file format handler.

This module provides support for reading and writing Parquet files
with streaming capabilities and compression support.
"""

import logging
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Union

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import Table

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pa = None
    pq = None
    Table = None

from anonimize.formats.base import (
    BaseFormatHandler,
    FileStats,
    FormatConfig,
    StreamingWriter,
    register_handler,
)

logger = logging.getLogger(__name__)


class ParquetStreamingWriter(StreamingWriter):
    """Streaming writer for Parquet files.

    Uses PyArrow's ParquetWriter to write batches incrementally.
    """

    def __init__(
        self,
        destination: Union[str, Path, BinaryIO],
        config: FormatConfig,
        schema: Optional[Dict[str, str]] = None,
    ):
        """Initialize the Parquet streaming writer."""
        super().__init__(destination, config, schema)

        self._writer = None
        self._schema = None
        self._first_batch = True

    def _infer_schema(self, batch: List[Dict[str, Any]]) -> pa.Schema:
        """Infer PyArrow schema from data batch."""
        if not batch:
            raise ValueError("Cannot infer schema from empty batch")

        fields = []

        for key, value in batch[0].items():
            if value is None:
                # Default to string for None values
                pa_type = pa.string()
            elif isinstance(value, bool):
                pa_type = pa.bool_()
            elif isinstance(value, int):
                pa_type = pa.int64()
            elif isinstance(value, float):
                pa_type = pa.float64()
            else:
                pa_type = pa.string()

            fields.append(pa.field(key, pa_type))

        return pa.schema(fields)

    def _dicts_to_table(self, batch: List[Dict[str, Any]]) -> Table:
        """Convert list of dicts to PyArrow Table."""
        if not batch:
            return pa.Table.from_pydict({})

        # Transpose list of dicts to dict of lists
        columns = {}
        keys = batch[0].keys()

        for key in keys:
            columns[key] = [row.get(key) for row in batch]

        return pa.Table.from_pydict(columns)

    def write_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Write a batch of rows to the Parquet file."""
        self._check_closed()

        if not batch:
            return 0

        table = self._dicts_to_table(batch)

        if self._first_batch:
            self._first_batch = False

            # Determine compression
            compression = self.config.compression or "snappy"

            # Create writer
            if isinstance(self.destination, (str, Path)):
                self._writer = pq.ParquetWriter(
                    self.destination,
                    table.schema,
                    compression=compression,
                    use_dictionary=True,
                    write_statistics=True,
                )
            else:
                self._writer = pq.ParquetWriter(
                    self.destination,
                    table.schema,
                    compression=compression,
                    use_dictionary=True,
                    write_statistics=True,
                )

        self._writer.write_table(table)
        self._rows_written += len(batch)

        return len(batch)

    def close(self) -> None:
        """Close the writer and finalize the Parquet file."""
        if self._closed:
            return

        if self._writer:
            self._writer.close()
            self._writer = None

        self._closed = True
        logger.debug(f"Parquet writer closed. Rows written: {self._rows_written}")


class ParquetHandler(BaseFormatHandler):
    """Handler for Parquet file format.

    This handler provides support for reading and writing Parquet files
    with the following features:
    - Column projection (read only specific columns)
    - Compression support (snappy, gzip, zstd, lz4, brotli)
    - Row group sizing for efficient reads
    - Streaming reads and writes

    Example:
        >>> from anonimize.formats.parquet import ParquetHandler
        >>> handler = ParquetHandler()
        >>> # Read entire file
        >>> data = handler.read("data.parquet")
        >>> # Read specific columns
        >>> data = handler.read("data.parquet", columns=["name", "email"])
        >>> # Stream read in batches
        >>> for batch in handler.read_streaming("data.parquet", batch_size=1000):
        ...     process(batch)
        >>> # Write data
        >>> handler.write("output.parquet", data, compression="zstd")
        >>> # Stream write
        >>> with handler.write_streaming("output.parquet") as writer:
        ...     for batch in data_source:
        ...         writer.write_batch(batch)
    """

    SUPPORTED_COMPRESSIONS = ["none", "snappy", "gzip", "zstd", "lz4", "brotli"]

    def __init__(self, config: Optional[FormatConfig] = None):
        """Initialize the Parquet handler.

        Args:
            config: Format configuration.

        Raises:
            ImportError: If pyarrow is not installed.
        """
        if not PYARROW_AVAILABLE:
            raise ImportError(
                "pyarrow is required for Parquet support. "
                "Install it with: pip install pyarrow"
            )

        super().__init__(config)

    @property
    def supported_extensions(self) -> List[str]:
        """Return supported file extensions."""
        return [".parquet", ".parq"]

    def read(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Read all data from a Parquet file.

        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read.
            **kwargs: Additional options:
                - use_threads: Use multiple threads for reading (default: True)
                - memory_map: Memory map the file (default: False)

        Returns:
            List of row dictionaries.
        """
        use_threads = kwargs.get("use_threads", True)
        memory_map = kwargs.get("memory_map", False)

        # Read table
        table = pq.read_table(
            source,
            columns=columns,
            use_threads=use_threads,
            memory_map=memory_map,
        )

        # Convert to list of dicts
        result = table.to_pydict()

        # Transpose to list of dicts
        if not result:
            return []

        num_rows = len(next(iter(result.values())))
        rows = []

        for i in range(num_rows):
            row = {key: result[key][i] for key in result.keys()}
            rows.append(row)

        self._stats.rows_read += len(rows)
        self._stats.columns = list(result.keys())

        return rows

    def read_streaming(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Read Parquet file in batches (streaming).

        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read.
            batch_size: Number of rows per batch (default: from config).
            **kwargs: Additional options:
                - use_threads: Use multiple threads (default: True)

        Yields:
            Batches of row dictionaries.
        """
        use_threads = kwargs.get("use_threads", True)
        batch_size = batch_size or self.config.batch_size

        parquet_file = pq.ParquetFile(source)

        self._stats.columns = [
            parquet_file.schema.column(i).name
            for i in range(parquet_file.schema.num_columns)
        ]

        for batch in parquet_file.iter_batches(
            batch_size=batch_size,
            columns=columns,
            use_threads=use_threads,
        ):
            # Convert batch to list of dicts
            batch_dict = batch.to_pydict()

            if not batch_dict:
                continue

            num_rows = len(next(iter(batch_dict.values())))
            rows = []

            for i in range(num_rows):
                row = {key: batch_dict[key][i] for key in batch_dict.keys()}
                rows.append(row)

            self._stats.rows_read += len(rows)

            yield rows

    def write(
        self,
        destination: Union[str, Path, BinaryIO],
        data: List[Dict[str, Any]],
        schema: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> FileStats:
        """Write data to a Parquet file.

        Args:
            destination: File path or file-like object.
            data: List of row dictionaries.
            schema: Optional schema dictionary {column: type}.
            **kwargs: Additional options:
                - compression: Compression type (default: from config or 'snappy')
                - row_group_size: Number of rows per row group
                - use_dictionary: Use dictionary encoding (default: True)

        Returns:
            File statistics.
        """
        if not data:
            return FileStats(rows_written=0)

        compression = kwargs.get("compression", self.config.compression or "snappy")
        row_group_size = kwargs.get("row_group_size", len(data))
        use_dictionary = kwargs.get("use_dictionary", True)

        # Convert to PyArrow Table
        columns = {}
        keys = data[0].keys()

        for key in keys:
            columns[key] = [row.get(key) for row in data]

        table = pa.Table.from_pydict(columns)

        # Write to file
        pq.write_table(
            table,
            destination,
            compression=compression,
            row_group_size=row_group_size,
            use_dictionary=use_dictionary,
            write_statistics=True,
        )

        self._stats.rows_written += len(data)
        self._stats.columns = list(keys)
        self._stats.compression = compression

        return self._stats

    def write_streaming(
        self,
        destination: Union[str, Path, BinaryIO],
        schema: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> ParquetStreamingWriter:
        """Create a streaming writer for Parquet files.

        Args:
            destination: File path or file-like object.
            schema: Optional schema dictionary.
            **kwargs: Additional options (passed to writer).

        Returns:
            ParquetStreamingWriter instance.
        """
        return ParquetStreamingWriter(destination, self.config, schema)

    def get_schema(self, source: Union[str, Path, BinaryIO]) -> Dict[str, str]:
        """Get the schema of a Parquet file.

        Args:
            source: File path or file-like object.

        Returns:
            Dictionary mapping column names to types.
        """
        parquet_file = pq.ParquetFile(source)
        schema_dict = {}

        for i in range(parquet_file.schema.num_columns):
            column = parquet_file.schema.column(i)
            schema_dict[column.name] = str(column.logical_type or column.physical_type)

        return schema_dict

    def get_metadata(self, source: Union[str, Path, BinaryIO]) -> Dict[str, Any]:
        """Get detailed metadata about a Parquet file.

        Args:
            source: File path or file-like object.

        Returns:
            Dictionary with file metadata.
        """
        parquet_file = pq.ParquetFile(source)
        metadata = parquet_file.metadata

        return {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "created_by": metadata.created_by,
            "column_names": [
                parquet_file.schema.column(i).name
                for i in range(parquet_file.schema.num_columns)
            ],
            "row_groups": [
                {
                    "num_rows": metadata.row_group(i).num_rows,
                    "total_byte_size": metadata.row_group(i).total_byte_size,
                }
                for i in range(metadata.num_row_groups)
            ],
        }

    def optimize_for_reading(
        self, source: Union[str, Path], destination: Union[str, Path]
    ) -> None:
        """Optimize a Parquet file for efficient reading.

        This rewrites the file with optimal settings for query performance:
        - Dictionary encoding for low-cardinality columns
        - Appropriate row group sizing
        - Sorted row groups

        Args:
            source: Source file path.
            destination: Destination file path.
        """
        table = pq.read_table(source)

        pq.write_table(
            table,
            destination,
            compression="snappy",
            row_group_size=100000,
            use_dictionary=True,
            write_statistics=True,
            flavor="spark",  # Ensure compatibility
        )

        logger.info(f"Optimized Parquet file written to {destination}")


# Register handler
if PYARROW_AVAILABLE:
    register_handler(ParquetHandler())
