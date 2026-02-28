"""CSV file anonymizer.

This module provides anonymization functionality for CSV files.
"""

import csv
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TextIO, Union

from anonimize.anonymizers.base import BaseAnonymizer
from anonimize.core import Anonymizer

logger = logging.getLogger(__name__)


class CSVAnonymizer(BaseAnonymizer):
    """Anonymizer for CSV files.
    
    This class provides methods to anonymize PII in CSV files while
    preserving the file structure.
    
    Attributes:
        chunk_size: Number of rows to process at once for large files.
        encoding: File encoding (default: utf-8).
        delimiter: CSV delimiter character.
    
    Example:
        >>> anon = CSVAnonymizer()
        >>> config = {"name": {"strategy": "replace", "type": "name"}}
        >>> anon.anonymize("input.csv", "output.csv", config)
    """

    def __init__(
        self,
        chunk_size: int = 10000,
        encoding: str = "utf-8",
        delimiter: str = ",",
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the CSV anonymizer.
        
        Args:
            chunk_size: Rows to process per batch for large files.
            encoding: File encoding.
            delimiter: CSV field delimiter.
            config: Default configuration.
        """
        super().__init__(config)
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.delimiter = delimiter
        self._core_anonymizer = Anonymizer()

    def anonymize(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        config: Optional[Dict[str, Dict[str, Any]]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        show_progress: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """Anonymize a CSV file.
        
        Args:
            input_path: Path to the input CSV file.
            output_path: Path for the anonymized output file.
            config: Column anonymization configuration.
            column_mapping: Map CSV columns to different field names in config.
            show_progress: Show progress bar for large files (requires tqdm).
            **kwargs: Additional arguments passed to csv.reader/writer.
        
        Returns:
            Statistics about the anonymization process.
        
        Raises:
            FileNotFoundError: If input file doesn't exist.
            ValueError: If configuration is invalid.
        
        Example:
            >>> anon = CSVAnonymizer()
            >>> config = {
            ...     "name": {"strategy": "replace", "type": "name"},
            ...     "email": {"strategy": "mask", "type": "email"},
            ... }
            >>> stats = anon.anonymize("input.csv", "output.csv", config)
        """
        input_path = Path(input_path)
        output_path = Path(output_path)
        config = config or self.config
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Validate configuration
        errors = self.validate_config(config)
        if errors:
            raise ValueError(f"Invalid configuration: {'; '.join(errors)}")
        
        logger.info(f"Anonymizing CSV: {input_path} -> {output_path}")
        
        # Get CSV dialect and headers
        with open(input_path, "r", encoding=self.encoding, newline="") as f:
            sample = f.read(8192)
            f.seek(0)
            
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample, delimiters=self.delimiter)
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = self.delimiter
            
            reader = csv.reader(f, dialect=dialect, **kwargs)
            headers = next(reader)
        
        # Count total rows for progress bar
        total_rows = 0
        if show_progress:
            try:
                with open(input_path, "r", encoding=self.encoding, newline="") as f:
                    total_rows = sum(1 for _ in f) - 1  # Subtract header
            except Exception:
                show_progress = False  # Fall back to no progress on error
        
        # Process the file
        records_processed = 0
        fields_anonymized = 0
        
        # Setup progress bar if requested
        progress_bar = None
        if show_progress:
            try:
                from tqdm import tqdm
                progress_bar = tqdm(
                    total=total_rows,
                    desc="Anonymizing",
                    unit="rows",
                    ncols=80,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
                )
            except ImportError:
                logger.warning("tqdm not installed, progress bar disabled. Install with: pip install tqdm")
                show_progress = False
        
        with open(input_path, "r", encoding=self.encoding, newline="") as infile, \
             open(output_path, "w", encoding=self.encoding, newline="") as outfile:
            
            reader = csv.reader(infile, dialect=dialect, **kwargs)
            writer = csv.writer(outfile, dialect=dialect, **kwargs)
            
            # Write headers
            headers = next(reader)
            writer.writerow(headers)
            
            # Process rows
            for row in reader:
                if not row:  # Skip empty rows
                    continue
                
                # Convert row to dict
                row_dict = dict(zip(headers, row))
                
                # Apply column mapping if provided
                if column_mapping:
                    for csv_col, config_key in column_mapping.items():
                        if csv_col in row_dict and config_key in config:
                            temp_config = {csv_col: config[config_key]}
                            row_dict = self._core_anonymizer.anonymize(row_dict, temp_config)
                            fields_anonymized += 1
                else:
                    # Anonymize based on matching column names
                    row_config = {k: v for k, v in config.items() if k in headers}
                    if row_config:
                        row_dict = self._core_anonymizer.anonymize(row_dict, row_config)
                        fields_anonymized += len(row_config)
                
                # Convert back to row
                new_row = [row_dict.get(h, "") for h in headers]
                writer.writerow(new_row)
                
                records_processed += 1
                
                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
                
                if records_processed % self.chunk_size == 0:
                    logger.debug(f"Processed {records_processed} records...")
        
        # Close progress bar
        if progress_bar:
            progress_bar.close()
        
        self._update_stats(records=records_processed, fields=fields_anonymized)
        
        logger.info(
            f"Anonymization complete: {records_processed} records, "
            f"{fields_anonymized} fields anonymized"
        )
        
        return {
            "records_processed": records_processed,
            "fields_anonymized": fields_anonymized,
            "output_path": str(output_path),
        }

    def anonymize_in_place(
        self,
        file_path: Union[str, Path],
        config: Optional[Dict[str, Dict[str, Any]]] = None,
        suffix: str = "_anonymized",
        **kwargs
    ) -> Dict[str, Any]:
        """Anonymize a CSV file in place.
        
        Creates a backup and overwrites the original file with anonymized data.
        
        Args:
            file_path: Path to the CSV file.
            config: Column anonymization configuration.
            suffix: Suffix for the backup file.
            **kwargs: Additional arguments.
        
        Returns:
            Statistics about the anonymization process.
        """
        file_path = Path(file_path)
        backup_path = file_path.with_suffix(f"{file_path.suffix}.{suffix}.bak")
        temp_path = file_path.with_suffix(f".tmp{file_path.suffix}")
        
        # Create backup
        import shutil
        shutil.copy2(file_path, backup_path)
        
        try:
            # Anonymize to temp file
            result = self.anonymize(file_path, temp_path, config, **kwargs)
            
            # Replace original with anonymized
            shutil.move(temp_path, file_path)
            
            return result
        except Exception as e:
            # Restore from backup on error
            shutil.copy2(backup_path, file_path)
            raise e
        finally:
            # Clean up temp file if it exists
            if temp_path.exists():
                temp_path.unlink()

    def detect_columns(
        self,
        input_path: Union[str, Path],
        sample_size: int = 100,
    ) -> Dict[str, str]:
        """Detect potential PII columns in a CSV file.
        
        Args:
            input_path: Path to the CSV file.
            sample_size: Number of rows to sample for detection.
        
        Returns:
            Dictionary mapping column names to detected PII types.
        """
        input_path = Path(input_path)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        with open(input_path, "r", encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            
            # Collect sample data
            samples: Dict[str, List[str]] = {}
            for i, row in enumerate(reader):
                if i >= sample_size:
                    break
                for col, value in row.items():
                    if col not in samples:
                        samples[col] = []
                    samples[col].append(value)
        
        # Detect PII in each column
        from anonimize.detectors.regex import RegexDetector
        detector = RegexDetector()
        
        detected = {}
        for col, values in samples.items():
            # Use first non-empty value for detection
            for value in values:
                if value:
                    result = detector.detect({col: value})
                    if col in result:
                        detected[col] = result[col].get("type", "unknown")
                        break
        
        return detected

    def preview(
        self,
        input_path: Union[str, Path],
        config: Dict[str, Dict[str, Any]],
        num_rows: int = 5,
    ) -> List[Dict[str, Any]]:
        """Preview anonymization on a few rows without writing output.
        
        Args:
            input_path: Path to the CSV file.
            config: Column anonymization configuration.
            num_rows: Number of rows to preview.
        
        Returns:
            List of anonymized rows.
        """
        input_path = Path(input_path)
        
        with open(input_path, "r", encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            
            results = []
            for i, row in enumerate(reader):
                if i >= num_rows:
                    break
                
                row_config = {k: v for k, v in config.items() if k in row}
                if row_config:
                    row = self._core_anonymizer.anonymize(row, row_config)
                
                results.append(row)
        
        return results
