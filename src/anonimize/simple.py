"""Simple API for anonimize - Just 3 lines to anonymize data.

This module provides the simplest possible interface for anonymizing data.
No complex configuration needed - just import and go.

Quick Start:
    >>> from anonimize import anonymize
    >>> anonymize("customers.csv", "customers_safe.csv")

Or with Python data:
    >>> from anonimize import anonymize_data
    >>> data = [{"name": "John", "email": "john@example.com"}]
    >>> anonymize_data(data)
    [{"name": "Sarah Smith", "email": "j***@example.com"}]
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from anonimize.anonymizers.csv_anon import CSVAnonymizer
from anonimize.core import Anonymizer

# Silence verbose logging by default
logging.getLogger("anonimize").setLevel(logging.WARNING)

__all__ = ["anonymize", "anonymize_data", "detect_pii", "preview"]


def anonymize(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    strategy: str = "replace",
    dry_run: bool = False,
    progress: bool = True,
    **kwargs,
) -> Union[str, Dict[str, Any], List[Dict[str, Any]]]:
    """Anonymize a file or data structure in one line.

    This is the simplest possible way to anonymize data. Just point it at a file
    and it will auto-detect PII and anonymize it with smart defaults.

    Args:
        input_path: Path to file (CSV, JSON, JSONL) OR data to anonymize
        output_path: Where to save the result (optional, returns data if not provided)
        strategy: How to anonymize - "replace" (fake data), "mask" (***), "hash", "remove"
        dry_run: If True, show what WOULD be anonymized without changing anything
        progress: Show progress bar for large files
        **kwargs: Additional options (see below)

    Returns:
        Path to output file, or anonymized data if no output_path given

    Examples:
        # Anonymize a CSV file
        >>> anonymize("customers.csv", "customers_safe.csv")

        # Preview changes without modifying anything
        >>> anonymize("data.csv", dry_run=True)

        # Use masking instead of replacement
        >>> anonymize("users.json", "users_safe.json", strategy="mask")

        # Just get the result back
        >>> result = anonymize(my_data_list)

    Advanced Options (kwargs):
        locale: str = "en_US" - Locale for fake data generation
        seed: int = None - Random seed for reproducible results
        preserve_relationships: bool = True - Keep same fake value for same real value
        columns: List[str] = None - Only anonymize these columns
        pii_types: List[str] = None - Only detect these PII types
    """
    input_path = Path(input_path) if isinstance(input_path, str) else input_path

    # If input_path is actually data (dict or list), use anonymize_data
    if isinstance(input_path, (dict, list)):
        return anonymize_data(input_path, strategy=strategy, dry_run=dry_run, **kwargs)

    # File-based anonymization
    if not input_path.exists():
        raise FileNotFoundError(
            f"File not found: {input_path}\n"
            f"Hint: Check the path and ensure the file exists.\n"
            f"      If you meant to pass data directly, use anonymize_data() instead."
        )

    # Auto-detect file type and handle accordingly
    suffix = input_path.suffix.lower()

    if suffix == ".csv":
        return _anonymize_csv(
            input_path, output_path, strategy, dry_run, progress, **kwargs
        )
    elif suffix in (".json", ".jsonl"):
        return _anonymize_json(
            input_path, output_path, strategy, dry_run, progress, **kwargs
        )
    else:
        raise ValueError(
            f"Unsupported file type: {suffix}\n"
            f"Hint: anonimize supports .csv, .json, and .jsonl files.\n"
            f"      For other formats, see the full documentation."
        )


def anonymize_data(
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    strategy: str = "replace",
    dry_run: bool = False,
    **kwargs,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Anonymize Python data structures (dicts or lists) in one line.

    Args:
        data: Dictionary or list of dictionaries to anonymize
        strategy: How to anonymize - "replace", "mask", "hash", or "remove"
        dry_run: If True, return config that WOULD be used without anonymizing
        **kwargs: Additional options

    Returns:
        Anonymized data (same structure as input)

    Examples:
        >>> data = {"name": "John Doe", "email": "john@example.com"}
        >>> anonymize_data(data)
        {"name": "Sarah Smith", "email": "john.smith@example.com"}

        >>> users = [{"name": "Alice"}, {"name": "Bob"}]
        >>> anonymize_data(users, strategy="hash")
        [{"name": "a3f5c8..."}, {"name": "b2e9d1..."}]
    """
    anon = Anonymizer(
        locale=kwargs.get("locale", "en_US"),
        seed=kwargs.get("seed"),
        preserve_relationships=kwargs.get("preserve_relationships", True),
    )

    # Auto-detect PII
    detected = anon.detect_pii(data)

    if not detected:
        if dry_run:
            return {"would_anonymize": [], "reason": "No PII detected"}
        return data

    # Build config from detected PII
    columns = kwargs.get("columns")
    pii_types = kwargs.get("pii_types")

    config = {}
    for field, info in detected.items():
        # Skip if columns specified and this isn't one of them
        if columns and field not in columns:
            continue

        # Skip if pii_types specified and this type isn't included
        if pii_types and info.get("type") not in pii_types:
            continue

        config[field] = {"strategy": strategy, "type": info.get("type", "string")}

    if dry_run:
        return {
            "would_anonymize": list(config.keys()),
            "strategy": strategy,
            "detected_pii": detected,
            "preview": "Use dry_run=False to apply changes",
        }

    return anon.anonymize(data, config)


def detect_pii(
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, Path],
) -> Dict[str, Any]:
    """Detect PII in data without anonymizing.

    Args:
        data: Data to analyze (dict, list, file path, or string)

    Returns:
        Dictionary mapping field names to detected PII types

    Examples:
        >>> detect_pii({"name": "John", "ssn": "123-45-6789"})
        {"name": {"type": "name", "confidence": 0.7},
         "ssn": {"type": "ssn", "confidence": 0.95}}

        >>> detect_pii("customers.csv")
        {"email": {"type": "email", "confidence": 1.0}, ...}
    """
    anon = Anonymizer()

    # Handle file paths
    if isinstance(data, (str, Path)):
        path = Path(data)
        if path.suffix.lower() == ".csv":
            csv_anon = CSVAnonymizer()
            return csv_anon.detect_columns(path)
        elif path.suffix.lower() in (".json", ".jsonl"):
            with open(path) as f:
                if path.suffix.lower() == ".jsonl":
                    sample = json.loads(f.readline())
                else:
                    sample = json.load(f)
                    if isinstance(sample, list):
                        sample = sample[0] if sample else {}
            return anon.detect_pii(sample)

    return anon.detect_pii(data)


def preview(
    input_path: Union[str, Path], num_rows: int = 3, strategy: str = "replace", **kwargs
) -> List[Dict[str, Any]]:
    """Preview anonymization on first few rows without modifying anything.

    Args:
        input_path: Path to CSV file
        num_rows: Number of rows to preview (default: 3)
        strategy: Anonymization strategy to preview
        **kwargs: Additional options

    Returns:
        List of dictionaries showing before/after

    Example:
        >>> preview("customers.csv", num_rows=2)
        [
            {"name": "John Doe → Sarah Smith", "email": "john@example.com → jane@example.com"},
            ...
        ]
    """
    path = Path(input_path)

    if path.suffix.lower() != ".csv":
        raise ValueError("preview() currently only supports CSV files")

    csv_anon = CSVAnonymizer()

    # Auto-detect columns
    detected = csv_anon.detect_columns(path)

    # Build config
    config = {
        col: {"strategy": strategy, "type": pii_type}
        for col, pii_type in detected.items()
    }

    # Get preview
    return csv_anon.preview(path, config, num_rows=num_rows)


# Internal helper functions


def _anonymize_csv(
    input_path: Path,
    output_path: Optional[Path],
    strategy: str,
    dry_run: bool,
    progress: bool,
    **kwargs,
) -> Union[str, Dict[str, Any]]:
    """Internal: Anonymize CSV file."""
    csv_anon = CSVAnonymizer()

    # Auto-detect columns
    detected = csv_anon.detect_columns(input_path)

    if not detected:
        raise ValueError(
            f"No PII detected in {input_path.name}\n"
            f"Hint: The file may not contain recognizable PII columns.\n"
            f"      Use detect_pii() to see what was detected, or provide a manual config."
        )

    # Build config
    columns = kwargs.get("columns")
    config = {}
    for col, pii_type in detected.items():
        if columns and col not in columns:
            continue
        config[col] = {"strategy": strategy, "type": pii_type}

    if dry_run:
        preview_data = csv_anon.preview(input_path, config, num_rows=3)
        return {
            "file": str(input_path),
            "would_anonymize_columns": list(config.keys()),
            "strategy": strategy,
            "preview": preview_data,
            "note": "Use dry_run=False to apply changes",
        }

    if output_path is None:
        # Generate output path
        output_path = input_path.with_suffix(f".anonymized{input_path.suffix}")

    # Show progress for large files
    if progress:
        _show_progress(input_path, csv_anon, output_path, config)
    else:
        csv_anon.anonymize(input_path, output_path, config)

    return str(output_path)


def _anonymize_json(
    input_path: Path,
    output_path: Optional[Path],
    strategy: str,
    dry_run: bool,
    progress: bool,
    **kwargs,
) -> Union[str, Dict[str, Any]]:
    """Internal: Anonymize JSON file."""
    with open(input_path) as f:
        if input_path.suffix.lower() == ".jsonl":
            data = [json.loads(line) for line in f if line.strip()]
        else:
            data = json.load(f)

    result = anonymize_data(data, strategy=strategy, dry_run=dry_run, **kwargs)

    if dry_run:
        return result

    if output_path is None:
        output_path = input_path.with_suffix(f".anonymized{input_path.suffix}")

    with open(output_path, "w") as f:
        if input_path.suffix.lower() == ".jsonl":
            for item in result:
                f.write(json.dumps(item) + "\n")
        else:
            json.dump(result, f, indent=2)

    return str(output_path)


def _show_progress(
    input_path: Path, csv_anon: CSVAnonymizer, output_path: Path, config: Dict
):
    """Show progress bar during anonymization."""
    try:
        from tqdm import tqdm

        # Count total rows
        with open(input_path) as f:
            total = sum(1 for _ in f) - 1  # Subtract header

        # Create wrapper that shows progress
        chunk_size = csv_anon.chunk_size

        with tqdm(total=total, desc="Anonymizing", unit="rows") as pbar:
            # Override the process to update progress
            # This is a simplified version - full implementation would modify CSVAnonymizer
            result = csv_anon.anonymize(input_path, output_path, config)
            pbar.update(total)

        return result
    except ImportError:
        # tqdm not installed, run without progress
        return csv_anon.anonymize(input_path, output_path, config)
