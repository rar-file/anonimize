"""Custom exceptions and error handling for anonimize.

This module provides user-friendly error messages with actionable suggestions.
"""

from typing import List, Optional


class AnonimizeError(Exception):
    """Base exception for anonimize errors."""

    def __init__(self, message: str, suggestion: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.suggestion = suggestion

    def __str__(self) -> str:
        if self.suggestion:
            return f"{self.message}\n\n💡 Hint: {self.suggestion}"
        return self.message


class FileNotFoundError(AnonimizeError):
    """Raised when a file is not found."""

    def __init__(self, filepath: str):
        suggestion = (
            f"Check that the file exists at: {filepath}\n"
            "  - Verify the path is correct (use absolute paths if unsure)\n"
            "  - Check file permissions\n"
            "  - For relative paths, ensure you're in the right directory"
        )
        super().__init__(f"File not found: {filepath}", suggestion)


class UnsupportedFileTypeError(AnonimizeError):
    """Raised when an unsupported file type is provided."""

    SUPPORTED_TYPES = [".csv", ".json", ".jsonl"]

    def __init__(self, filepath: str):
        ext = filepath.split(".")[-1] if "." in filepath else "none"
        suggestion = (
            f"Supported formats: {', '.join(self.SUPPORTED_TYPES)}\n"
            "  - Convert your file to one of these formats\n"
            "  - For databases, use the connector API directly\n"
            "  - For other formats, see the documentation for extensibility options"
        )
        super().__init__(f"Unsupported file type: .{ext}", suggestion)


class NoPiiDetectedError(AnonimizeError):
    """Raised when no PII is detected in the data."""

    def __init__(self):
        suggestion = (
            "This could mean:\n"
            "  - The data doesn't contain recognizable PII patterns\n"
            "  - Column names don't match known PII types (try renaming columns)\n"
            "  - You may need to manually specify fields to anonymize\n\n"
            "To manually configure, use:\n"
            "  config = {'column_name': {'strategy': 'replace', 'type': 'name'}}\n"
            "  anonymize_data(data, columns=['column_name'])"
        )
        super().__init__("No PII detected in the data", suggestion)


class InvalidStrategyError(AnonimizeError):
    """Raised when an invalid strategy is specified."""

    VALID_STRATEGIES = ["replace", "mask", "hash", "remove"]

    def __init__(self, strategy: str):
        suggestion = (
            f"Valid strategies: {', '.join(self.VALID_STRATEGIES)}\n"
            "  - 'replace': Substitute with fake data (good for testing)\n"
            "  - 'mask': Show partial data (good for display)\n"
            "  - 'hash': One-way transformation (good for analytics)\n"
            "  - 'remove': Delete the data entirely (maximum privacy)"
        )
        super().__init__(f"Invalid strategy: '{strategy}'", suggestion)


class ConfigurationError(AnonimizeError):
    """Raised when configuration is invalid."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        suggestion = None
        if errors:
            suggestion = "Configuration errors:\n" + "\n".join(
                f"  - {e}" for e in errors
            )
        super().__init__(message, suggestion)


class PhoneyNotInstalledError(AnonimizeError):
    """Raised when Phoney is not installed but replace strategy is used."""

    def __init__(self):
        suggestion = (
            "Install Phoney with:\n"
            "  pip install phoney\n\n"
            "Or use a different strategy:\n"
            "  anonymize_data(data, strategy='mask')  # or 'hash', 'remove'"
        )
        super().__init__(
            "Phoney is required for the 'replace' strategy but is not installed",
            suggestion,
        )


class PermissionError(AnonimizeError):
    """Raised when there are permission issues with files."""

    def __init__(self, filepath: str, operation: str = "access"):
        suggestion = (
            f"Check permissions for: {filepath}\n"
            "  - Ensure the file isn't open in another program\n"
            "  - Check directory write permissions\n"
            "  - On Unix: ls -la $(dirname filepath)\n"
            "  - Try running with appropriate permissions"
        )
        super().__init__(
            f"Permission denied: cannot {operation} {filepath}", suggestion
        )


def format_error(e: Exception) -> str:
    """Format an exception into a user-friendly message.

    Args:
        e: The exception to format.

    Returns:
        A user-friendly error message with suggestions.
    """
    if isinstance(e, AnonimizeError):
        return str(e)

    # Handle common Python exceptions with suggestions
    if isinstance(e, FileNotFoundError):
        return str(FileNotFoundError(str(e)))

    if isinstance(e, PermissionError):
        return str(PermissionError(str(e)))

    if isinstance(e, ValueError):
        return f"Invalid value: {e}\n\n💡 Hint: Check your input parameters match the expected format."

    if isinstance(e, TypeError):
        return f"Type error: {e}\n\n💡 Hint: Check that you're passing the right types (e.g., dict, list, str)."

    if isinstance(e, KeyError):
        return f"Missing key: {e}\n\n💡 Hint: Check that your data contains all expected columns/fields."

    # Generic error for unexpected exceptions
    return (
        f"Unexpected error: {type(e).__name__}: {e}\n\n"
        "💡 Hint: This looks like a bug. Please report it at:\n"
        "   https://github.com/rar-file/anonimize/issues"
    )
