"""Avro file format handler.

This module provides support for reading and writing Avro files
with schema evolution support and streaming capabilities.
"""

from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Union
import io
import logging

try:
    import fastavro
    from fastavro import reader, writer, parse_schema
    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False
    fastavro = None
    reader = None
    writer = None
    parse_schema = None

from anonimize.formats.base import (
    BaseFormatHandler,
    FormatConfig,
    FileStats,
    StreamingWriter,
    register_handler,
)

logger = logging.getLogger(__name__)


# Avro type mapping
AVRO_TYPES = {
    "string": "string",
    "str": "string",
    "integer": "int",
    "int": "long",
    "long": "long",
    "float": "double",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "bytes": "bytes",
    "null": "null",
    "none": "null",
}


def infer_avro_schema(data: List[Dict[str, Any]], name: str = "Record") -> Dict[str, Any]:
    """Infer Avro schema from data.
    
    Args:
        data: Sample data to infer schema from.
        name: Name for the record schema.
    
    Returns:
        Avro schema dictionary.
    """
    if not data:
        raise ValueError("Cannot infer schema from empty data")
    
    fields = []
    sample = data[0]
    
    for key, value in sample.items():
        field_type = _infer_avro_type(value)
        
        # Make fields nullable by default
        field_type = ["null", field_type]
        
        fields.append({
            "name": key,
            "type": field_type,
            "default": None,
        })
    
    return {
        "type": "record",
        "name": name,
        "fields": fields,
    }


def _infer_avro_type(value: Any) -> Union[str, Dict, List]:
    """Infer Avro type from Python value."""
    if value is None:
        return "null"
    
    if isinstance(value, bool):
        return "boolean"
    
    if isinstance(value, int):
        # Use long for integers to handle larger values
        return "long"
    
    if isinstance(value, float):
        return "double"
    
    if isinstance(value, str):
        return "string"
    
    if isinstance(value, bytes):
        return "bytes"
    
    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": "string"}
        
        # Infer type from first element
        item_type = _infer_avro_type(value[0])
        return {"type": "array", "items": item_type}
    
    if isinstance(value, dict):
        # Nested record
        return infer_avro_schema([value], "NestedRecord")
    
    # Default to string for unknown types
    return "string"


def convert_to_avro_compatible(data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convert data to be Avro-compatible.
    
    Args:
        data: Data to convert.
        schema: Avro schema.
    
    Returns:
        Converted data.
    """
    result = {}
    
    field_map = {f["name"]: f for f in schema.get("fields", [])}
    
    for key, value in data.items():
        if key not in field_map:
            continue
        
        field = field_map[key]
        field_type = field.get("type")
        
        # Handle union types
        if isinstance(field_type, list):
            # Find the non-null type
            non_null_types = [t for t in field_type if t != "null"]
            if non_null_types:
                field_type = non_null_types[0]
        
        # Convert value based on type
        if value is None:
            result[key] = None
        elif field_type == "string" and not isinstance(value, str):
            result[key] = str(value)
        elif field_type in ("long", "int") and isinstance(value, float):
            result[key] = int(value)
        elif field_type == "double" and isinstance(value, int):
            result[key] = float(value)
        else:
            result[key] = value
    
    return result


class AvroStreamingWriter(StreamingWriter):
    """Streaming writer for Avro files."""
    
    def __init__(
        self,
        destination: Union[str, Path, BinaryIO],
        config: FormatConfig,
        schema: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the Avro streaming writer."""
        super().__init__(destination, config, schema)
        
        self._parsed_schema = None
        self._file = None
        self._writer = None
        self._schema = schema
        self._buffer = []
        self._buffer_size = config.batch_size
    
    def _ensure_writer(self, sample_record: Dict[str, Any]) -> None:
        """Ensure the writer is initialized with schema."""
        if self._writer is not None:
            return
        
        # Infer or parse schema
        if self._schema is None:
            self._schema = infer_avro_schema([sample_record])
        
        self._parsed_schema = parse_schema(self._schema)
        
        # Open file if path provided
        if isinstance(self.destination, (str, Path)):
            self._file = open(self.destination, "wb")
        else:
            self._file = self.destination
        
        # Create writer
        self._writer = fastavro.write.writer(
            self._file,
            self._parsed_schema,
            codec=self.config.compression or "null",
        )
    
    def write_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Write a batch of rows to the Avro file."""
        self._check_closed()
        
        if not batch:
            return 0
        
        # Initialize writer if needed
        if self._writer is None:
            self._ensure_writer(batch[0])
        
        # Convert records to be Avro-compatible
        for record in batch:
            avro_record = convert_to_avro_compatible(record, self._schema)
            self._writer.write(avro_record)
        
        self._rows_written += len(batch)
        
        return len(batch)
    
    def close(self) -> None:
        """Close the writer and finalize the Avro file."""
        if self._closed:
            return
        
        if self._writer:
            # Avro writer flushes on close
            pass
        
        if self._file and isinstance(self.destination, (str, Path)):
            self._file.close()
        
        self._closed = True
        
        logger.debug(f"Avro writer closed. Rows written: {self._rows_written}")


class AvroHandler(BaseFormatHandler):
    """Handler for Avro file format.
    
    This handler provides support for reading and writing Avro files
    with the following features:
    - Schema inference and validation
    - Schema evolution support
    - Compression support (null, deflate, snappy, zstandard, lz4, xz)
    - Streaming reads and writes
    - Binary and JSON encoding
    
    Example:
        >>> from anonimize.formats.avro import AvroHandler
        >>> handler = AvroHandler()
        >>> # Read entire file
        >>> data = handler.read("data.avro")
        >>> # Stream read
        >>> for batch in handler.read_streaming("data.avro", batch_size=1000):
        ...     process(batch)
        >>> # Write with inferred schema
        >>> handler.write("output.avro", data, compression="snappy")
        >>> # Write with explicit schema
        >>> schema = {
        ...     "type": "record",
        ...     "name": "User",
        ...     "fields": [
        ...         {"name": "name", "type": "string"},
        ...         {"name": "age", "type": ["null", "int"], "default": None},
        ...     ]
        ... }
        >>> handler.write("output.avro", data, schema=schema)
    """
    
    SUPPORTED_CODECS = ["null", "deflate", "snappy", "zstandard", "lz4", "xz"]
    
    def __init__(self, config: Optional[FormatConfig] = None):
        """Initialize the Avro handler.
        
        Args:
            config: Format configuration.
        
        Raises:
            ImportError: If fastavro is not installed.
        """
        if not FASTAVRO_AVAILABLE:
            raise ImportError(
                "fastavro is required for Avro support. "
                "Install it with: pip install fastavro"
            )
        
        super().__init__(config)
    
    @property
    def supported_extensions(self) -> List[str]:
        """Return supported file extensions."""
        return [".avro"]
    
    def read(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Read all data from an Avro file.
        
        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read (filters after read).
            **kwargs: Additional options:
                - return_schema: Also return the schema (default: False)
        
        Returns:
            List of row dictionaries.
        """
        return_schema = kwargs.get("return_schema", False)
        
        # Open file
        if isinstance(source, (str, Path)):
            file_obj = open(source, "rb")
            should_close = True
        else:
            file_obj = source
            should_close = False
        
        try:
            # Read records
            avro_reader = reader(file_obj)
            records = list(avro_reader)
            
            # Filter columns if specified
            if columns:
                records = [
                    {k: v for k, v in record.items() if k in columns}
                    for record in records
                ]
            
            self._stats.rows_read += len(records)
            self._stats.columns = list(avro_reader.schema.get("fields", []))
            
            if return_schema:
                return records, avro_reader.schema
            
            return records
        finally:
            if should_close:
                file_obj.close()
    
    def read_streaming(
        self,
        source: Union[str, Path, BinaryIO],
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        **kwargs
    ) -> Iterator[List[Dict[str, Any]]]:
        """Read Avro file in batches (streaming).
        
        Args:
            source: File path or file-like object.
            columns: Optional list of columns to read.
            batch_size: Number of rows per batch.
            **kwargs: Additional options.
        
        Yields:
            Batches of row dictionaries.
        """
        batch_size = batch_size or self.config.batch_size
        
        # Open file
        if isinstance(source, (str, Path)):
            file_obj = open(source, "rb")
            should_close = True
        else:
            file_obj = source
            should_close = False
        
        try:
            avro_reader = reader(file_obj)
            
            self._stats.columns = [
                f["name"] for f in avro_reader.schema.get("fields", [])
            ]
            
            batch = []
            for record in avro_reader:
                # Filter columns if specified
                if columns:
                    record = {k: v for k, v in record.items() if k in columns}
                
                batch.append(record)
                
                if len(batch) >= batch_size:
                    self._stats.rows_read += len(batch)
                    yield batch
                    batch = []
            
            # Yield remaining records
            if batch:
                self._stats.rows_read += len(batch)
                yield batch
        finally:
            if should_close:
                file_obj.close()
    
    def write(
        self,
        destination: Union[str, Path, BinaryIO],
        data: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> FileStats:
        """Write data to an Avro file.
        
        Args:
            destination: File path or file-like object.
            data: List of row dictionaries.
            schema: Optional Avro schema. If not provided, will be inferred.
            **kwargs: Additional options:
                - compression: Compression codec
                - record_name: Name for inferred schema
        
        Returns:
            File statistics.
        """
        if not data:
            # Write empty file with minimal schema
            if schema is None:
                schema = {"type": "record", "name": "Empty", "fields": []}
            
            parsed_schema = parse_schema(schema)
            
            if isinstance(destination, (str, Path)):
                with open(destination, "wb") as f:
                    writer(f, parsed_schema, [])
            else:
                writer(destination, parsed_schema, [])
            
            return FileStats(rows_written=0)
        
        # Infer schema if not provided
        if schema is None:
            record_name = kwargs.get("record_name", "Record")
            schema = infer_avro_schema(data, record_name)
        
        parsed_schema = parse_schema(schema)
        
        # Get compression codec
        compression = kwargs.get("compression", self.config.compression or "null")
        
        # Convert records to be Avro-compatible
        avro_records = [convert_to_avro_compatible(record, schema) for record in data]
        
        # Write to file
        if isinstance(destination, (str, Path)):
            with open(destination, "wb") as f:
                writer(f, parsed_schema, avro_records, codec=compression)
        else:
            writer(destination, parsed_schema, avro_records, codec=compression)
        
        self._stats.rows_written += len(data)
        self._stats.columns = list(data[0].keys()) if data else []
        self._stats.compression = compression
        
        return self._stats
    
    def write_streaming(
        self,
        destination: Union[str, Path, BinaryIO],
        schema: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> AvroStreamingWriter:
        """Create a streaming writer for Avro files.
        
        Args:
            destination: File path or file-like object.
            schema: Optional Avro schema.
            **kwargs: Additional options.
        
        Returns:
            AvroStreamingWriter instance.
        """
        return AvroStreamingWriter(destination, self.config, schema)
    
    def get_schema(self, source: Union[str, Path, BinaryIO]) -> Dict[str, Any]:
        """Get the Avro schema from a file.
        
        Args:
            source: File path or file-like object.
        
        Returns:
            Avro schema dictionary.
        """
        if isinstance(source, (str, Path)):
            with open(source, "rb") as f:
                avro_reader = reader(f)
                return avro_reader.schema
        else:
            avro_reader = reader(source)
            return avro_reader.schema
    
    def validate_schema(self, schema: Dict[str, Any]) -> bool:
        """Validate an Avro schema.
        
        Args:
            schema: Avro schema dictionary.
        
        Returns:
            True if valid, raises exception otherwise.
        """
        try:
            parse_schema(schema)
            return True
        except Exception as e:
            raise ValueError(f"Invalid Avro schema: {e}")
    
    def convert_schema(
        self,
        source_format: str,
        source_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert schema from another format to Avro.
        
        Args:
            source_format: Source format name.
            source_schema: Source schema dictionary.
        
        Returns:
            Avro schema dictionary.
        """
        if source_format.lower() == "json":
            # Convert JSON schema-like to Avro
            fields = []
            
            for key, value_type in source_schema.items():
                avro_type = AVRO_TYPES.get(value_type.lower(), "string")
                fields.append({
                    "name": key,
                    "type": ["null", avro_type],
                    "default": None,
                })
            
            return {
                "type": "record",
                "name": "ConvertedRecord",
                "fields": fields,
            }
        
        raise ValueError(f"Unsupported source format: {source_format}")


# Register handler
if FASTAVRO_AVAILABLE:
    register_handler(AvroHandler())
