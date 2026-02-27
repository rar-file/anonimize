"""Regex-based PII detector.

This module provides a detector that uses regular expressions
to identify PII in data.
"""

import re
import logging
from typing import Any, Dict, List, Optional, Pattern, Union

from anonimize.detectors.base import BaseDetector

logger = logging.getLogger(__name__)


class RegexDetector(BaseDetector):
    """PII detector using regular expressions.
    
    This detector uses a set of predefined regex patterns to identify
    common types of PII such as emails, phone numbers, SSNs, etc.
    
    Attributes:
        patterns: Dictionary of regex patterns for each PII type.
    
    Example:
        >>> detector = RegexDetector()
        >>> detector.detect({"email": "john@example.com"})
        {'email': {'type': 'email', 'confidence': 1.0}}
    """

    # Default regex patterns for common PII types
    DEFAULT_PATTERNS = {
        "email": {
            "pattern": re.compile(
                r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            ),
            "confidence": 1.0,
        },
        "phone": {
            "pattern": re.compile(
                r"^(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$"
            ),
            "confidence": 0.9,
        },
        "ssn": {
            "pattern": re.compile(
                r"^(?!000|666|9\d{2})\d{3}-?(?!00)\d{2}-?(?!0000)\d{4}$"
            ),
            "confidence": 0.95,
        },
        "credit_card": {
            "pattern": re.compile(
                r"^(?:4[0-9]{12}(?:[0-9]{3})?|"  # Visa
                r"5[1-5][0-9]{14}|"  # MasterCard
                r"3[47][0-9]{13}|"  # American Express
                r"3(?:0[0-5]|[68][0-9])[0-9]{11}|"  # Diners Club
                r"6(?:011|5[0-9]{2})[0-9]{12}|"  # Discover
                r"(?:2131|1800|35\d{3})\d{11})$"  # JCB
            ),
            "confidence": 0.9,
        },
        "ipv4": {
            "pattern": re.compile(
                r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
                r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
            ),
            "confidence": 0.85,
        },
        "ipv6": {
            "pattern": re.compile(
                r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|"
                r"^::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}$|"
                r"^[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}$"
            ),
            "confidence": 0.85,
        },
        "uuid": {
            "pattern": re.compile(
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
            ),
            "confidence": 0.8,
        },
        "url": {
            "pattern": re.compile(
                r"^https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?$"
            ),
            "confidence": 0.8,
        },
    }

    # Field name patterns for detecting PII based on column names
    FIELD_NAME_PATTERNS = {
        "email": re.compile(r"e[-_]?mail", re.IGNORECASE),
        "phone": re.compile(r"phone|tel|mobile|cell|fax", re.IGNORECASE),
        "ssn": re.compile(r"ssn|social[-_]?security|tax[-_]?id", re.IGNORECASE),
        "name": re.compile(r"name|full[-_]?name", re.IGNORECASE),
        "first_name": re.compile(r"first[-_]?name|fname|given[-_]?name", re.IGNORECASE),
        "last_name": re.compile(r"last[-_]?name|lname|surname|family[-_]?name", re.IGNORECASE),
        "address": re.compile(r"address|street|addr", re.IGNORECASE),
        "city": re.compile(r"city|town", re.IGNORECASE),
        "country": re.compile(r"country|nation", re.IGNORECASE),
        "zip": re.compile(r"zip|postal[-_]?code|postcode", re.IGNORECASE),
        "credit_card": re.compile(r"credit[-_]?card|cc[-_]?num|card[-_]?num", re.IGNORECASE),
        "ip_address": re.compile(r"ip[-_]?addr|ip[-_]?address|remote[-_]?ip", re.IGNORECASE),
        "date_of_birth": re.compile(r"dob|date[-_]?of[-_]?birth|birth[-_]?date|birthday", re.IGNORECASE),
        "password": re.compile(r"password|passwd|pwd", re.IGNORECASE),
        "username": re.compile(r"user[-_]?name|login|user[-_]?id|userid", re.IGNORECASE),
    }

    def __init__(
        self,
        confidence_threshold: float = 0.5,
        custom_patterns: Optional[Dict[str, Dict[str, Union[str, Pattern, float]]]] = None,
        check_field_names: bool = True,
    ):
        """Initialize the regex detector.
        
        Args:
            confidence_threshold: Minimum confidence for detection.
            custom_patterns: Custom regex patterns to add/override.
            check_field_names: Whether to check field names for PII indicators.
        """
        super().__init__(confidence_threshold)
        self.check_field_names = check_field_names
        
        # Initialize patterns
        self.patterns = self.DEFAULT_PATTERNS.copy()
        
        # Add custom patterns
        if custom_patterns:
            for pii_type, config in custom_patterns.items():
                pattern = config.get("pattern")
                if isinstance(pattern, str):
                    pattern = re.compile(pattern)
                
                self.patterns[pii_type] = {
                    "pattern": pattern,
                    "confidence": config.get("confidence", 0.8),
                }

    def detect(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]], str],
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """Detect PII in data using regex patterns.
        
        Args:
            data: Data to analyze (dict, list of dicts, or string).
            **kwargs: Additional arguments.
        
        Returns:
            Dictionary mapping field names to detection results.
        
        Example:
            >>> detector = RegexDetector()
            >>> detector.detect({"email": "test@example.com"})
            {'email': {'type': 'email', 'confidence': 1.0}}
        """
        results = {}
        
        if isinstance(data, str):
            # Single string detection
            result = self._detect_in_value(data)
            if result:
                return {"value": result}
        
        elif isinstance(data, dict):
            # Dictionary detection
            for field, value in data.items():
                if isinstance(value, str):
                    # Check value against patterns
                    result = self._detect_in_value(value)
                    if result:
                        results[field] = result
                    # Check field name
                    elif self.check_field_names:
                        name_result = self._detect_by_field_name(field)
                        if name_result:
                            results[field] = name_result
        
        elif isinstance(data, list) and data:
            # List detection - analyze first item
            return self.detect(data[0])
        
        return self._filter_by_confidence(results)

    def _detect_in_value(self, value: str) -> Optional[Dict[str, Any]]:
        """Detect PII type in a single value.
        
        Args:
            value: String value to analyze.
        
        Returns:
            Detection result or None.
        """
        for pii_type, config in self.patterns.items():
            pattern = config["pattern"]
            confidence = config["confidence"]
            
            if pattern.match(str(value)):
                return {
                    "type": pii_type,
                    "confidence": confidence,
                    "pattern": pii_type,
                }
        
        return None

    def _detect_by_field_name(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Detect PII type based on field name.
        
        Args:
            field_name: Name of the field.
        
        Returns:
            Detection result or None.
        """
        normalized_name = self._normalize_field_name(field_name)
        
        for pii_type, pattern in self.FIELD_NAME_PATTERNS.items():
            if pattern.search(normalized_name):
                return {
                    "type": pii_type,
                    "confidence": 0.7,  # Lower confidence for name-based detection
                    "detected_by": "field_name",
                }
        
        return None

    def add_pattern(
        self,
        pii_type: str,
        pattern: Union[str, Pattern],
        confidence: float = 0.8,
    ) -> None:
        """Add a custom regex pattern.
        
        Args:
            pii_type: Type of PII this pattern detects.
            pattern: Regex pattern (string or compiled).
            confidence: Confidence level for this pattern.
        """
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        
        self.patterns[pii_type] = {
            "pattern": pattern,
            "confidence": confidence,
        }
        
        logger.debug(f"Added pattern for '{pii_type}' with confidence {confidence}")

    def get_supported_types(self) -> List[str]:
        """Get list of supported PII types.
        
        Returns:
            List of PII type names.
        """
        return list(self.patterns.keys())
