"""Utility functions for anonimize.

This module provides helper functions used across the anonimize package
for data processing, validation, and transformation.
"""

import hashlib
import re
from typing import Any, Dict, List, Optional


def hash_value(
    value: str, salt: Optional[str] = None, algorithm: str = "sha256"
) -> str:
    """Hash a value using the specified algorithm.

    Args:
        value: The string value to hash.
        salt: Optional salt to add to the value before hashing.
        algorithm: Hash algorithm to use (sha256, sha512, md5).

    Returns:
        The hashed value as a hexadecimal string.

    Example:
        >>> hash_value("sensitive@email.com")
        'a3f5c2...'
    """
    if salt:
        value = f"{salt}:{value}"

    hasher = hashlib.new(algorithm)
    hasher.update(value.encode("utf-8"))
    return hasher.hexdigest()


def mask_value(value: str, mask_char: str = "*", preserve_last: int = 4) -> str:
    """Mask a value, preserving only the last N characters.

    Args:
        value: The string value to mask.
        mask_char: Character to use for masking (default: '*').
        preserve_last: Number of characters to preserve at the end.

    Returns:
        The masked value.

    Example:
        >>> mask_value("123-45-6789")
        '******6789'
    """
    if not value:
        return value

    if len(value) <= preserve_last:
        return mask_char * len(value)

    masked_length = len(value) - preserve_last
    return mask_char * masked_length + value[-preserve_last:]


def mask_email(email: str, mask_char: str = "*") -> str:
    """Mask an email address, preserving domain and partial local part.

    Args:
        email: The email address to mask.
        mask_char: Character to use for masking.

    Returns:
        The masked email address.

    Example:
        >>> mask_email("john.doe@example.com")
        'j***e@example.com'
    """
    if "@" not in email:
        return mask_value(email, mask_char, preserve_last=3)

    local, domain = email.rsplit("@", 1)

    if len(local) <= 2:
        masked_local = mask_char * len(local)
    else:
        masked_local = local[0] + mask_char * (len(local) - 2) + local[-1]

    return f"{masked_local}@{domain}"


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate an anonymization configuration.

    Args:
        config: Configuration dictionary to validate.

    Returns:
        List of validation error messages. Empty list if valid.
    """
    errors = []
    valid_strategies = {"replace", "hash", "mask", "remove"}

    for field, settings in config.items():
        if not isinstance(settings, dict):
            errors.append(f"Field '{field}': settings must be a dictionary")
            continue

        strategy = settings.get("strategy")
        if not strategy:
            errors.append(f"Field '{field}': 'strategy' is required")
        elif strategy not in valid_strategies:
            errors.append(
                f"Field '{field}': invalid strategy '{strategy}'. "
                f"Must be one of: {valid_strategies}"
            )

    return errors


def get_nested_value(data: Dict[str, Any], path: str, separator: str = ".") -> Any:
    """Get a value from a nested dictionary using dot notation.

    Args:
        data: The dictionary to traverse.
        path: Dot-separated path to the value.
        separator: Separator used in the path.

    Returns:
        The value at the specified path, or None if not found.

    Example:
        >>> data = {"user": {"name": "John"}}
        >>> get_nested_value(data, "user.name")
        'John'
    """
    keys = path.split(separator)
    current = data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None

    return current


def set_nested_value(
    data: Dict[str, Any], path: str, value: Any, separator: str = "."
) -> None:
    """Set a value in a nested dictionary using dot notation.

    Args:
        data: The dictionary to modify.
        path: Dot-separated path to the value.
        value: The value to set.
        separator: Separator used in the path.

    Example:
        >>> data = {"user": {}}
        >>> set_nested_value(data, "user.name", "John")
        >>> data
        {'user': {'name': 'John'}}
    """
    keys = path.split(separator)
    current = data

    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    current[keys[-1]] = value


def detect_file_type(file_path: str) -> Optional[str]:
    """Detect the type of a file based on its extension.

    Args:
        file_path: Path to the file.

    Returns:
        File type (csv, json, etc.) or None if unknown.
    """
    extension = file_path.lower().split(".")[-1] if "." in file_path else ""

    type_map = {
        "csv": "csv",
        "json": "json",
        "jsonl": "jsonl",
        "parquet": "parquet",
        "xlsx": "excel",
        "xls": "excel",
    }

    return type_map.get(extension)


def truncate_string(value: str, max_length: int, suffix: str = "...") -> str:
    """Truncate a string to a maximum length.

    Args:
        value: The string to truncate.
        max_length: Maximum length of the result.
        suffix: Suffix to add if truncated.

    Returns:
        The truncated string.
    """
    if len(value) <= max_length:
        return value

    return value[: max_length - len(suffix)] + suffix


def sanitize_column_name(name: str) -> str:
    """Sanitize a column name for safe use in SQL and other contexts.

    Args:
        name: The column name to sanitize.

    Returns:
        The sanitized column name.
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"

    return sanitized.lower()


def merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries.

    Args:
        base: Base configuration.
        override: Configuration to override base values.

    Returns:
        Merged configuration.
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value

    return result
