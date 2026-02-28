"""Base anonymizer class.

This module defines the abstract base class that all anonymizers must implement.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseAnonymizer(ABC):
    """Abstract base class for all anonymizers.

    All anonymizer implementations must inherit from this class and
    implement the `anonymize` method.

    Attributes:
        config: Default configuration for the anonymizer.

    Example:
        >>> class MyAnonymizer(BaseAnonymizer):
        ...     def anonymize(self, data, config):
        ...         # Implementation
        ...         pass
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the anonymizer.

        Args:
            config: Default configuration dictionary.
        """
        self.config = config or {}
        self._stats = {
            "records_processed": 0,
            "fields_anonymized": 0,
            "errors": 0,
        }
        logger.debug(f"{self.__class__.__name__} initialized")

    @abstractmethod
    def anonymize(
        self, data: Any, config: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Any:
        """Anonymize data according to the configuration.

        This method must be implemented by all subclasses.

        Args:
            data: Data to anonymize.
            config: Anonymization configuration.
            **kwargs: Additional implementation-specific arguments.

        Returns:
            Anonymized data.

        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        pass

    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate the anonymization configuration.

        Args:
            config: Configuration to validate.

        Returns:
            List of validation error messages. Empty if valid.
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

    def get_stats(self) -> Dict[str, int]:
        """Get anonymization statistics.

        Returns:
            Dictionary with processing statistics.
        """
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset processing statistics."""
        self._stats = {
            "records_processed": 0,
            "fields_anonymized": 0,
            "errors": 0,
        }

    def _update_stats(self, records: int = 0, fields: int = 0, errors: int = 0) -> None:
        """Update processing statistics.

        Args:
            records: Number of records processed.
            fields: Number of fields anonymized.
            errors: Number of errors encountered.
        """
        self._stats["records_processed"] += records
        self._stats["fields_anonymized"] += fields
        self._stats["errors"] += errors
