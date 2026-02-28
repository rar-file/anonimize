"""JSON file anonymizer.

This module provides anonymization functionality for JSON files,
including support for nested structures and JSON Lines format.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from anonimize.anonymizers.base import BaseAnonymizer
from anonimize.core import Anonymizer
from anonimize.utils import get_nested_value, set_nested_value

logger = logging.getLogger(__name__)


class JSONAnonymizer(BaseAnonymizer):
    """Anonymizer for JSON files.
    
    This class provides methods to anonymize PII in JSON files with
    support for nested structures and JSON Lines format.
    
    Attributes:
        encoding: File encoding (default: utf-8).
        indent: Indentation for pretty-printing JSON output.
    
    Example:
        >>> anon = JSONAnonymizer()
        >>> config = {"user.name": {"strategy": "replace", "type": "name"}}
        >>> anon.anonymize("input.json", "output.json", config)
    """

    def __init__(
        self,
        encoding: str = "utf-8",
        indent: Optional[int] = 2,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the JSON anonymizer.
        
        Args:
            encoding: File encoding.
            indent: Indentation for output (None for compact output).
            config: Default configuration.
        """
        super().__init__(config)
        self.encoding = encoding
        self.indent = indent
        self._core_anonymizer = Anonymizer()

    def anonymize(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        config: Optional[Dict[str, Dict[str, Any]]] = None,
        json_path: Optional[str] = None,
        is_jsonlines: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """Anonymize a JSON file.
        
        Args:
            input_path: Path to the input JSON file.
            output_path: Path for the anonymized output file.
            config: JSON path anonymization configuration.
            json_path: JSONPath expression to select specific elements.
            is_jsonlines: Whether the file is in JSON Lines format.
            **kwargs: Additional arguments passed to json.load/dump.
        
        Returns:
            Statistics about the anonymization process.
        
        Raises:
            FileNotFoundError: If input file doesn't exist.
            ValueError: If configuration is invalid or JSON is malformed.
        
        Example:
            >>> anon = JSONAnonymizer()
            >>> config = {
            ...     "users.*.name": {"strategy": "replace", "type": "name"},
            ...     "users.*.email": {"strategy": "hash", "type": "email"},
            ... }
            >>> stats = anon.anonymize("input.json", "output.json", config)
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
        
        logger.info(f"Anonymizing JSON: {input_path} -> {output_path}")
        
        if is_jsonlines:
            return self._anonymize_jsonlines(
                input_path, output_path, config, **kwargs
            )
        
        # Load JSON
        with open(input_path, "r", encoding=self.encoding) as f:
            data = json.load(f, **kwargs)
        
        # Anonymize
        anonymized_data = self._anonymize_data(data, config)
        
        # Save
        with open(output_path, "w", encoding=self.encoding) as f:
            json.dump(anonymized_data, f, indent=self.indent, **kwargs)
        
        stats = self.get_stats()
        stats["output_path"] = str(output_path)
        
        logger.info(f"Anonymization complete: {stats}")
        
        return stats

    def _anonymize_data(
        self,
        data: Any,
        config: Dict[str, Dict[str, Any]],
        current_path: str = "",
    ) -> Any:
        """Recursively anonymize JSON data.
        
        Args:
            data: JSON data to anonymize.
            config: Configuration with JSON paths.
            current_path: Current path in the JSON structure.
        
        Returns:
            Anonymized data.
        """
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                new_path = f"{current_path}.{key}" if current_path else key
                
                # Check if this exact path is in config
                if new_path in config:
                    result[key] = self._apply_strategy(value, config[new_path])
                    self._update_stats(fields=1)
                # Check for wildcard patterns
                elif self._matches_wildcard(new_path, config):
                    matching_config = self._get_wildcard_config(new_path, config)
                    result[key] = self._apply_strategy(value, matching_config)
                    self._update_stats(fields=1)
                elif isinstance(value, (dict, list)):
                    result[key] = self._anonymize_data(value, config, new_path)
                else:
                    result[key] = value
            
            return result
        
        elif isinstance(data, list):
            return [
                self._anonymize_data(item, config, f"{current_path}[]")
                for item in data
            ]
        
        else:
            return data

    def _matches_wildcard(self, path: str, config: Dict[str, Any]) -> bool:
        """Check if a path matches any wildcard pattern in config.
        
        Args:
            path: The current JSON path.
            config: Configuration dictionary.
        
        Returns:
            True if path matches a wildcard pattern.
        """
        for pattern in config.keys():
            if "*" in pattern:
                # Convert [] to .* for matching since config uses * for array indices
                normalized_path = path.replace("[]", ".*")
                # Simple wildcard matching
                import fnmatch
                if fnmatch.fnmatch(normalized_path, pattern):
                    return True
        return False

    def _get_wildcard_config(
        self,
        path: str,
        config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Get configuration for a wildcard-matched path.
        
        Args:
            path: The current JSON path.
            config: Configuration dictionary.
        
        Returns:
            Matching configuration.
        """
        import fnmatch
        
        for pattern, settings in config.items():
            if "*" in pattern:
                # Convert [] to .* for matching since config uses * for array indices
                normalized_path = path.replace("[]", ".*")
                if fnmatch.fnmatch(normalized_path, pattern):
                    return settings
        
        return {}

    def _apply_strategy(
        self,
        value: Any,
        settings: Dict[str, Any]
    ) -> Any:
        """Apply anonymization strategy to a value.
        
        Args:
            value: Value to anonymize.
            settings: Anonymization settings.
        
        Returns:
            Anonymized value.
        """
        if value is None:
            return None
        
        strategy = settings.get("strategy", "replace")
        pii_type = settings.get("type", "string")
        
        # Create a temporary record for the core anonymizer
        temp_record = {"value": value}
        temp_config = {"value": settings}
        
        result = self._core_anonymizer.anonymize(temp_record, temp_config)
        return result["value"]

    def _anonymize_jsonlines(
        self,
        input_path: Path,
        output_path: Path,
        config: Dict[str, Dict[str, Any]],
        **kwargs
    ) -> Dict[str, Any]:
        """Anonymize a JSON Lines file.
        
        Args:
            input_path: Path to input file.
            output_path: Path to output file.
            config: Anonymization configuration.
            **kwargs: Additional arguments.
        
        Returns:
            Statistics.
        """
        records_processed = 0
        
        with open(input_path, "r", encoding=self.encoding) as infile, \
             open(output_path, "w", encoding=self.encoding) as outfile:
            
            for line in infile:
                line = line.strip()
                if not line:
                    continue
                
                data = json.loads(line)
                anonymized = self._anonymize_data(data, config)
                outfile.write(json.dumps(anonymized, **kwargs) + "\n")
                
                records_processed += 1
                self._update_stats(records=1)
        
        stats = self.get_stats()
        stats["output_path"] = str(output_path)
        
        return stats

    def anonymize_in_place(
        self,
        file_path: Union[str, Path],
        config: Optional[Dict[str, Dict[str, Any]]] = None,
        suffix: str = "_anonymized",
        **kwargs
    ) -> Dict[str, Any]:
        """Anonymize a JSON file in place.
        
        Args:
            file_path: Path to the JSON file.
            config: JSON path anonymization configuration.
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

    def detect_fields(
        self,
        input_path: Union[str, Path],
        sample_size: int = 100,
        is_jsonlines: bool = False,
    ) -> Dict[str, str]:
        """Detect potential PII fields in a JSON file.
        
        Args:
            input_path: Path to the JSON file.
            sample_size: Number of records to sample.
            is_jsonlines: Whether the file is in JSON Lines format.
        
        Returns:
            Dictionary mapping field paths to detected PII types.
        """
        input_path = Path(input_path)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        from anonimize.detectors.regex import RegexDetector
        detector = RegexDetector()
        
        detected = {}
        
        def scan_data(data: Any, path: str = "") -> None:
            """Recursively scan data for PII."""
            if isinstance(data, dict):
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    
                    if isinstance(value, str):
                        result = detector.detect({key: value})
                        if key in result:
                            detected[new_path] = result[key].get("type", "unknown")
                    elif isinstance(value, (dict, list)):
                        scan_data(value, new_path)
            
            elif isinstance(data, list):
                for i, item in enumerate(data[:5]):  # Sample first 5 items
                    scan_data(item, f"{path}[]")
        
        if is_jsonlines:
            with open(input_path, "r", encoding=self.encoding) as f:
                for i, line in enumerate(f):
                    if i >= sample_size:
                        break
                    if line.strip():
                        data = json.loads(line)
                        scan_data(data)
        else:
            with open(input_path, "r", encoding=self.encoding) as f:
                data = json.load(f)
                scan_data(data)
        
        return detected

    def preview(
        self,
        input_path: Union[str, Path],
        config: Dict[str, Dict[str, Any]],
        num_records: int = 3,
        is_jsonlines: bool = False,
    ) -> List[Any]:
        """Preview anonymization on a few records without writing output.
        
        Args:
            input_path: Path to the JSON file.
            config: JSON path anonymization configuration.
            num_records: Number of records to preview.
            is_jsonlines: Whether the file is in JSON Lines format.
        
        Returns:
            List of anonymized records.
        """
        input_path = Path(input_path)
        
        results = []
        
        if is_jsonlines:
            with open(input_path, "r", encoding=self.encoding) as f:
                for i, line in enumerate(f):
                    if i >= num_records:
                        break
                    if line.strip():
                        data = json.loads(line)
                        results.append(self._anonymize_data(data, config))
        else:
            with open(input_path, "r", encoding=self.encoding) as f:
                data = json.load(f)
                
                if isinstance(data, list):
                    for item in data[:num_records]:
                        results.append(self._anonymize_data(item, config))
                else:
                    results.append(self._anonymize_data(data, config))
        
        return results
