"""Streaming processor for large file anonymization.

This module provides efficient streaming processing for large files
that don't fit in memory, with progress tracking and checkpoint support.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from anonimize.connectors import BaseConnector
from anonimize.core import Anonymizer
from anonimize.formats import get_handler

logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for streaming operations.

    Attributes:
        batch_size: Number of rows to process per batch.
        checkpoint_interval: Save checkpoint every N batches.
        max_retries: Maximum retries for failed batches.
        continue_on_error: Whether to continue processing on batch errors.
        progress_interval: Log progress every N seconds.
        memory_limit_mb: Maximum memory usage in MB.
    """

    batch_size: int = 10000
    checkpoint_interval: int = 10
    max_retries: int = 3
    continue_on_error: bool = True
    progress_interval: int = 60
    memory_limit_mb: Optional[int] = None


@dataclass
class ProcessingStats:
    """Statistics for streaming processing.

    Attributes:
        total_rows: Total rows to process (if known).
        rows_processed: Number of rows processed.
        rows_anonymized: Number of rows anonymized.
        batches_processed: Number of batches processed.
        batches_failed: Number of batches that failed.
        start_time: Processing start timestamp.
        end_time: Processing end timestamp.
        errors: List of error messages.
        throughput_rps: Rows processed per second.
        estimated_remaining_seconds: Estimated time remaining.
    """

    total_rows: Optional[int] = None
    rows_processed: int = 0
    rows_anonymized: int = 0
    batches_processed: int = 0
    batches_failed: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    errors: List[str] = field(default_factory=list)

    @property
    def throughput_rps(self) -> float:
        """Calculate rows per second."""
        if self.start_time is None:
            return 0.0

        elapsed = (self.end_time or time.time()) - self.start_time
        if elapsed <= 0:
            return 0.0

        return self.rows_processed / elapsed

    @property
    def estimated_remaining_seconds(self) -> Optional[float]:
        """Estimate remaining time in seconds."""
        if self.total_rows is None or self.rows_processed == 0:
            return None

        throughput = self.throughput_rps
        if throughput <= 0:
            return None

        remaining = self.total_rows - self.rows_processed
        return remaining / throughput

    @property
    def progress_percent(self) -> Optional[float]:
        """Calculate progress percentage."""
        if self.total_rows is None or self.total_rows == 0:
            return None

        return (self.rows_processed / self.total_rows) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            "total_rows": self.total_rows,
            "rows_processed": self.rows_processed,
            "rows_anonymized": self.rows_anonymized,
            "batches_processed": self.batches_processed,
            "batches_failed": self.batches_failed,
            "throughput_rps": self.throughput_rps,
            "progress_percent": self.progress_percent,
            "estimated_remaining_seconds": self.estimated_remaining_seconds,
            "errors": self.errors[:10],  # Limit errors in output
        }


@dataclass
class Checkpoint:
    """Checkpoint for resumable processing.

    Attributes:
        input_path: Input file path.
        output_path: Output file path.
        rows_processed: Number of rows processed.
        batches_processed: Number of batches processed.
        timestamp: Checkpoint timestamp.
        metadata: Additional checkpoint metadata.
    """

    input_path: str
    output_path: str
    rows_processed: int
    batches_processed: int
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def save(self, path: Union[str, Path]) -> None:
        """Save checkpoint to file."""
        checkpoint_data = {
            "input_path": self.input_path,
            "output_path": self.output_path,
            "rows_processed": self.rows_processed,
            "batches_processed": self.batches_processed,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

        with open(path, "w") as f:
            json.dump(checkpoint_data, f, indent=2)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "Checkpoint":
        """Load checkpoint from file."""
        with open(path) as f:
            data = json.load(f)

        return cls(
            input_path=data["input_path"],
            output_path=data["output_path"],
            rows_processed=data["rows_processed"],
            batches_processed=data["batches_processed"],
            timestamp=data.get("timestamp", 0),
            metadata=data.get("metadata", {}),
        )


class StreamingProcessor:
    """Processor for streaming anonymization of large files.

    This processor handles large files that don't fit in memory by:
    - Reading data in batches
    - Processing each batch through the anonymizer
    - Writing results incrementally
    - Tracking progress and supporting checkpoints

    Example:
        >>> from anonimize import Anonymizer
        >>> from anonimize.streaming import StreamingProcessor, StreamConfig
        >>>
        >>> anonymizer = Anonymizer()
        >>> config = StreamConfig(batch_size=5000, checkpoint_interval=10)
        >>> processor = StreamingProcessor(anonymizer, config)
        >>>
        >>> stats = processor.process_file(
        ...     "input.parquet",
        ...     "output.parquet",
        ...     anonymization_config={"name": {"strategy": "replace"}},
        ... )
        >>> print(f"Processed {stats.rows_processed} rows at {stats.throughput_rps:.0f} RPS")
    """

    def __init__(
        self,
        anonymizer: Anonymizer,
        config: Optional[StreamConfig] = None,
    ):
        """Initialize the streaming processor.

        Args:
            anonymizer: The anonymizer to use for processing.
            config: Stream processing configuration.
        """
        self.anonymizer = anonymizer
        self.config = config or StreamConfig()
        self._stats = ProcessingStats()
        self._checkpoint_path: Optional[Path] = None
        self._last_progress_time = 0.0

    def process_file(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        anonymization_config: Dict[str, Any],
        checkpoint_path: Optional[Union[str, Path]] = None,
        resume: bool = False,
    ) -> ProcessingStats:
        """Process a file with streaming anonymization.

        Args:
            input_path: Input file path.
            output_path: Output file path.
            anonymization_config: Anonymization configuration.
            checkpoint_path: Optional path for checkpoint file.
            resume: Whether to resume from checkpoint.

        Returns:
            Processing statistics.
        """
        input_path = Path(input_path)
        output_path = Path(output_path)

        # Set up checkpoint
        if checkpoint_path:
            self._checkpoint_path = Path(checkpoint_path)

        # Try to resume from checkpoint
        rows_to_skip = 0
        if resume and self._checkpoint_path and self._checkpoint_path.exists():
            checkpoint = Checkpoint.load(self._checkpoint_path)
            rows_to_skip = checkpoint.rows_processed
            logger.info(
                f"Resuming from checkpoint: {rows_to_skip} rows already processed"
            )

        # Initialize stats
        self._stats = ProcessingStats(
            start_time=time.time(),
            total_rows=self._estimate_row_count(input_path),
        )

        # Get input handler
        input_handler = get_handler(input_path)
        output_handler = get_handler(output_path)

        logger.info(f"Starting streaming process: {input_path} -> {output_path}")
        logger.info(f"Batch size: {self.config.batch_size}")

        # Process in batches
        with output_handler.write_streaming(output_path) as writer:
            batch_iterator = input_handler.read_streaming(
                input_path,
                batch_size=self.config.batch_size,
            )

            for batch_num, batch in enumerate(batch_iterator, 1):
                # Skip if resuming
                if rows_to_skip > 0:
                    rows_in_batch = len(batch)
                    if rows_to_skip >= rows_in_batch:
                        rows_to_skip -= rows_in_batch
                        self._stats.rows_processed += rows_in_batch
                        continue
                    else:
                        batch = batch[rows_to_skip:]
                        rows_to_skip = 0

                # Process batch
                success = self._process_batch(
                    batch,
                    writer,
                    anonymization_config,
                    batch_num,
                )

                if not success and not self.config.continue_on_error:
                    logger.error("Stopping due to batch failure")
                    break

                # Save checkpoint
                if (
                    batch_num % self.config.checkpoint_interval == 0
                    and self._checkpoint_path
                ):
                    self._save_checkpoint(input_path, output_path)

                # Log progress
                self._maybe_log_progress()

        # Finalize
        self._stats.end_time = time.time()

        # Save final checkpoint
        if self._checkpoint_path:
            self._save_checkpoint(input_path, output_path)

        logger.info(f"Streaming process complete: {self._stats.to_dict()}")

        return self._stats

    def process_database(
        self,
        connector: BaseConnector,
        table_name: str,
        output_path: Union[str, Path],
        anonymization_config: Dict[str, Any],
        columns: Optional[List[str]] = None,
        schema: Optional[str] = None,
    ) -> ProcessingStats:
        """Process a database table with streaming anonymization.

        Args:
            connector: Database connector.
            table_name: Table to process.
            output_path: Output file path.
            anonymization_config: Anonymization configuration.
            columns: Optional columns to select.
            schema: Optional database schema.

        Returns:
            Processing statistics.
        """
        output_path = Path(output_path)

        # Initialize stats
        self._stats = ProcessingStats(
            start_time=time.time(),
            total_rows=self._get_table_row_count(connector, table_name, schema),
        )

        # Get output handler
        output_handler = get_handler(output_path)

        logger.info(
            f"Starting database streaming process: {table_name} -> {output_path}"
        )

        with output_handler.write_streaming(output_path) as writer:
            row_iterator = connector.scan_table(
                table_name,
                columns=columns,
                schema=schema,
                batch_size=self.config.batch_size,
            )

            # Process in batches
            batch = []
            batch_num = 0

            for row in row_iterator:
                batch.append(row)

                if len(batch) >= self.config.batch_size:
                    batch_num += 1
                    self._process_batch(
                        batch,
                        writer,
                        anonymization_config,
                        batch_num,
                    )
                    batch = []
                    self._maybe_log_progress()

            # Process remaining rows
            if batch:
                batch_num += 1
                self._process_batch(
                    batch,
                    writer,
                    anonymization_config,
                    batch_num,
                )

        self._stats.end_time = time.time()

        logger.info(f"Database streaming process complete: {self._stats.to_dict()}")

        return self._stats

    def _process_batch(
        self,
        batch: List[Dict[str, Any]],
        writer,
        anonymization_config: Dict[str, Any],
        batch_num: int,
    ) -> bool:
        """Process a single batch.

        Args:
            batch: List of rows to process.
            writer: Output writer.
            anonymization_config: Anonymization configuration.
            batch_num: Batch number for logging.

        Returns:
            True if successful, False otherwise.
        """
        for attempt in range(self.config.max_retries):
            try:
                # Anonymize batch
                anonymized = self.anonymizer.anonymize(batch, anonymization_config)

                # Write batch
                writer.write_batch(anonymized)

                # Update stats
                self._stats.rows_processed += len(batch)
                self._stats.rows_anonymized += len(batch)
                self._stats.batches_processed += 1

                return True

            except Exception as e:
                error_msg = f"Batch {batch_num} failed (attempt {attempt + 1}): {e}"
                logger.error(error_msg)

                if attempt == self.config.max_retries - 1:
                    self._stats.batches_failed += 1
                    self._stats.errors.append(error_msg)
                    return False

                time.sleep(0.1 * (attempt + 1))  # Exponential backoff

        return False

    def _estimate_row_count(self, path: Path) -> Optional[int]:
        """Estimate total rows in a file."""
        try:
            from anonimize.formats.parquet import ParquetHandler

            if path.suffix in (".parquet", ".parq"):
                handler = ParquetHandler()
                metadata = handler.get_metadata(path)
                return metadata.get("num_rows")
        except Exception:
            pass

        return None

    def _get_table_row_count(
        self,
        connector: BaseConnector,
        table_name: str,
        schema: Optional[str],
    ) -> Optional[int]:
        """Get row count from database table."""
        try:
            tables = connector.get_tables(schema)
            for table in tables:
                if table.name == table_name:
                    return table.row_count
        except Exception:
            pass

        return None

    def _save_checkpoint(
        self,
        input_path: Path,
        output_path: Path,
    ) -> None:
        """Save processing checkpoint."""
        if not self._checkpoint_path:
            return

        checkpoint = Checkpoint(
            input_path=str(input_path),
            output_path=str(output_path),
            rows_processed=self._stats.rows_processed,
            batches_processed=self._stats.batches_processed,
            metadata={
                "throughput_rps": self._stats.throughput_rps,
                "batches_failed": self._stats.batches_failed,
            },
        )

        checkpoint.save(self._checkpoint_path)
        logger.debug(f"Checkpoint saved: {self._stats.rows_processed} rows")

    def _maybe_log_progress(self) -> None:
        """Log progress if interval has passed."""
        now = time.time()

        if now - self._last_progress_time >= self.config.progress_interval:
            progress = self._stats.progress_percent
            progress_str = f"{progress:.1f}%" if progress else "unknown"

            remaining = self._stats.estimated_remaining_seconds
            remaining_str = f"{remaining:.0f}s remaining" if remaining else ""

            logger.info(
                f"Progress: {self._stats.rows_processed} rows "
                f"({progress_str}) | "
                f"{self._stats.throughput_rps:.0f} RPS | "
                f"{remaining_str}"
            )

            self._last_progress_time = now

    def get_stats(self) -> ProcessingStats:
        """Get current processing statistics."""
        return self._stats


def process_large_file(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    anonymization_config: Dict[str, Any],
    anonymizer: Optional[Anonymizer] = None,
    stream_config: Optional[StreamConfig] = None,
    **kwargs,
) -> ProcessingStats:
    """Convenience function for processing large files.

    Args:
        input_path: Input file path.
        output_path: Output file path.
        anonymization_config: Anonymization configuration.
        anonymizer: Optional custom anonymizer.
        stream_config: Optional stream configuration.
        **kwargs: Additional arguments passed to StreamingProcessor.process_file.

    Returns:
        Processing statistics.

    Example:
        >>> from anonimize.streaming import process_large_file
        >>>
        >>> stats = process_large_file(
        ...     "huge_dataset.parquet",
        ...     "anonymized.parquet",
        ...     anonymization_config={
        ...         "name": {"strategy": "replace", "type": "name"},
        ...         "email": {"strategy": "hash", "type": "email"},
        ...     },
        ... )
    """
    anonymizer = anonymizer or Anonymizer()
    processor = StreamingProcessor(anonymizer, stream_config)

    return processor.process_file(
        input_path, output_path, anonymization_config, **kwargs
    )
