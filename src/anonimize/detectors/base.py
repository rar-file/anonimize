"""Base detector class.

This module defines the abstract base class that all PII detectors must implement.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

logger = logging.getLogger(__name__)


class BaseDetector(ABC):
    """Abstract base class for all PII detectors.

    All PII detector implementations must inherit from this class and
    implement the `detect` method.

    Example:
        >>> class MyDetector(BaseDetector):
        ...     def detect(self, data):
        ...         # Implementation
        ...         pass
    """

    def __init__(self, confidence_threshold: float = 0.5):
        """Initialize the detector.

        Args:
            confidence_threshold: Minimum confidence level for a detection (0-1).
        """
        self.confidence_threshold = confidence_threshold
        logger.debug(f"{self.__class__.__name__} initialized")

    @abstractmethod
    def detect(self, data: Any, **kwargs) -> Dict[str, Dict[str, Any]]:
        """Detect PII in the provided data.

        This method must be implemented by all subclasses.

        Args:
            data: Data to analyze for PII.
            **kwargs: Additional implementation-specific arguments.

        Returns:
            Dictionary mapping field names to detection results.
            Each result should contain at least a 'type' key.

        Example:
            >>> detector.detect({"name": "John Doe"})
            {'name': {'type': 'name', 'confidence': 0.9}}
        """
        pass

    def _filter_by_confidence(
        self, results: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Filter detection results by confidence threshold.

        Args:
            results: Detection results.

        Returns:
            Filtered results with confidence >= threshold.
        """
        return {
            field: info
            for field, info in results.items()
            if info.get("confidence", 1.0) >= self.confidence_threshold
        }

    def _normalize_field_name(self, name: str) -> str:
        """Normalize a field name for consistent detection.

        Args:
            name: Field name to normalize.

        Returns:
            Normalized field name.
        """
        return name.lower().strip().replace(" ", "_")
