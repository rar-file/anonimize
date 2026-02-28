"""Heuristic-based PII detector.

This module provides a detector that uses heuristics and statistical
analysis to identify PII in data.
"""

import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Union

from anonimize.detectors.base import BaseDetector

logger = logging.getLogger(__name__)


class HeuristicDetector(BaseDetector):
    """PII detector using heuristics and statistical analysis.

    This detector uses various heuristics such as:
    - Entropy analysis for detecting random/high-entropy values
    - Format analysis for structured data
    - Frequency analysis for identifying unique identifiers
    - Keyword matching for common PII terms

    Example:
        >>> detector = HeuristicDetector()
        >>> detector.detect({"user_id": "a1b2c3d4e5f6"})
        {'user_id': {'type': 'identifier', 'confidence': 0.75}}
    """

    # Common PII-related keywords
    PII_KEYWORDS = {
        "personal": ["name", "email", "phone", "address", "birth", "age", "gender"],
        "financial": ["account", "card", "bank", "routing", "swift", "iban"],
        "identification": ["ssn", "social", "tax", "passport", "license", "id"],
        "contact": ["email", "phone", "mobile", "fax", "contact"],
        "location": [
            "address",
            "city",
            "state",
            "zip",
            "postal",
            "country",
            "coordinates",
        ],
    }

    # Character entropy thresholds for detecting high-entropy identifiers
    HIGH_ENTROPY_THRESHOLD = 3.5

    # Unique value ratio threshold for detecting identifiers
    UNIQUE_RATIO_THRESHOLD = 0.9

    def __init__(
        self,
        confidence_threshold: float = 0.5,
        analyze_entropy: bool = True,
        analyze_uniqueness: bool = True,
        analyze_formats: bool = True,
    ):
        """Initialize the heuristic detector.

        Args:
            confidence_threshold: Minimum confidence for detection.
            analyze_entropy: Whether to analyze value entropy.
            analyze_uniqueness: Whether to analyze value uniqueness.
            analyze_formats: Whether to analyze value formats.
        """
        super().__init__(confidence_threshold)
        self.analyze_entropy = analyze_entropy
        self.analyze_uniqueness = analyze_uniqueness
        self.analyze_formats = analyze_formats

    def detect(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """Detect PII in data using heuristics.

        Args:
            data: Data to analyze.
            **kwargs: Additional arguments including 'sample_size'.

        Returns:
            Dictionary mapping field names to detection results.

        Example:
            >>> detector = HeuristicDetector()
            >>> data = {"user_id": "abc123", "name": "John"}
            >>> detector.detect(data)
            {'user_id': {'type': 'identifier', 'confidence': 0.8}}
        """
        results = {}

        if isinstance(data, dict):
            # Single record analysis
            for field, value in data.items():
                result = self._analyze_field(field, value)
                if result:
                    results[field] = result

        elif isinstance(data, list) and data:
            # Multi-record analysis for better statistical insights
            sample_size = kwargs.get("sample_size", len(data))
            sample = data[:sample_size]

            # Collect all values for each field
            field_values: Dict[str, List[Any]] = {}
            for record in sample:
                if isinstance(record, dict):
                    for field, value in record.items():
                        if field not in field_values:
                            field_values[field] = []
                        field_values[field].append(value)

            # Analyze each field
            for field, values in field_values.items():
                result = self._analyze_field_with_samples(field, values)
                if result:
                    results[field] = result

        return self._filter_by_confidence(results)

    def _analyze_field(
        self,
        field_name: str,
        value: Any,
    ) -> Optional[Dict[str, Any]]:
        """Analyze a single field value.

        Args:
            field_name: Name of the field.
            value: Value to analyze.

        Returns:
            Detection result or None.
        """
        if not isinstance(value, str):
            return None

        scores = []

        # Check field name keywords
        keyword_score = self._check_keywords(field_name)
        if keyword_score > 0:
            scores.append(("keyword_match", keyword_score))

        # Check value entropy
        if self.analyze_entropy:
            entropy_score = self._check_entropy(value)
            if entropy_score > 0:
                scores.append(("high_entropy", entropy_score))

        # Check value format
        if self.analyze_formats:
            format_score = self._check_format(value)
            if format_score > 0:
                scores.append(("format", format_score))

        if not scores:
            return None

        # Combine scores
        max_score = max(score for _, score in scores)
        reasons = [reason for reason, score in scores if score == max_score]

        return {
            "type": self._infer_type(field_name, reasons),
            "confidence": min(max_score, 1.0),
            "detected_by": reasons,
        }

    def _analyze_field_with_samples(
        self,
        field_name: str,
        values: List[Any],
    ) -> Optional[Dict[str, Any]]:
        """Analyze a field using multiple sample values.

        Args:
            field_name: Name of the field.
            values: List of sample values.

        Returns:
            Detection result or None.
        """
        # Filter to string values
        string_values = [v for v in values if isinstance(v, str) and v]

        if not string_values:
            return None

        scores = []

        # Check field name keywords
        keyword_score = self._check_keywords(field_name)
        if keyword_score > 0:
            scores.append(("keyword_match", keyword_score))

        # Check uniqueness ratio
        if self.analyze_uniqueness and len(string_values) > 1:
            uniqueness_score = self._check_uniqueness(string_values)
            if uniqueness_score > 0:
                scores.append(("high_uniqueness", uniqueness_score))

        # Check average entropy
        if self.analyze_entropy:
            entropies = [self._calculate_entropy(v) for v in string_values[:10]]
            avg_entropy = sum(entropies) / len(entropies)
            if avg_entropy > self.HIGH_ENTROPY_THRESHOLD:
                scores.append(("high_entropy", min(avg_entropy / 5, 0.9)))

        # Check format consistency
        if self.analyze_formats:
            format_score = self._check_format_consistency(string_values)
            if format_score > 0:
                scores.append(("consistent_format", format_score))

        if not scores:
            return None

        # Combine scores
        max_score = max(score for _, score in scores)
        reasons = [reason for reason, score in scores if score > 0.5]

        return {
            "type": self._infer_type(field_name, reasons),
            "confidence": min(max_score, 1.0),
            "detected_by": reasons,
            "sample_size": len(values),
        }

    def _check_keywords(self, field_name: str) -> float:
        """Check if field name contains PII-related keywords.

        Args:
            field_name: Name of the field.

        Returns:
            Confidence score (0-1).
        """
        normalized = field_name.lower().replace("_", "").replace("-", "")

        for category, keywords in self.PII_KEYWORDS.items():
            for keyword in keywords:
                if keyword in normalized:
                    # Higher confidence for exact matches
                    if normalized == keyword:
                        return 0.9
                    return 0.7

        return 0.0

    def _check_entropy(self, value: str) -> float:
        """Check if value has high entropy (random-looking).

        Args:
            value: String value to check.

        Returns:
            Confidence score based on entropy.
        """
        entropy = self._calculate_entropy(value)

        if entropy > self.HIGH_ENTROPY_THRESHOLD:
            return min(entropy / 5, 0.85)

        return 0.0

    def _calculate_entropy(self, value: str) -> float:
        """Calculate Shannon entropy of a string.

        Args:
            value: String to calculate entropy for.

        Returns:
            Shannon entropy value.
        """
        if not value:
            return 0.0

        # Calculate character frequencies
        freq = Counter(value)
        length = len(value)

        # Calculate entropy
        import math

        entropy = -sum(
            (count / length) * math.log2(count / length) for count in freq.values()
        )

        return entropy

    def _check_uniqueness(self, values: List[str]) -> float:
        """Check ratio of unique values.

        Args:
            values: List of values.

        Returns:
            Confidence score based on uniqueness ratio.
        """
        if len(values) < 2:
            return 0.0

        unique_ratio = len(set(values)) / len(values)

        if unique_ratio > self.UNIQUE_RATIO_THRESHOLD:
            return unique_ratio * 0.8

        return 0.0

    def _check_format(self, value: str) -> float:
        """Check if value matches a structured format.

        Args:
            value: String value to check.

        Returns:
            Confidence score based on format match.
        """
        # Check for UUID-like format
        if re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            value,
            re.I,
        ):
            return 0.9

        # Check for hash-like format
        if re.match(r"^[0-9a-f]{32,64}$", value, re.I):
            return 0.85

        # Check for API key format
        if re.match(r"^[a-zA-Z0-9]{20,}$", value):
            return 0.75

        return 0.0

    def _check_format_consistency(self, values: List[str]) -> float:
        """Check if all values have consistent format.

        Args:
            values: List of values.

        Returns:
            Confidence score based on format consistency.
        """
        if len(values) < 2:
            return 0.0

        # Sample up to 20 values for efficiency
        sample = values[:20]

        # Check if all values have similar length
        lengths = [len(v) for v in sample]
        avg_length = sum(lengths) / len(lengths)
        length_variance = sum((l - avg_length) ** 2 for l in lengths) / len(lengths)

        # Low variance suggests consistent format
        if length_variance < 5:
            return 0.6

        return 0.0

    def _infer_type(self, field_name: str, reasons: List[str]) -> str:
        """Infer PII type based on field name and detection reasons.

        Args:
            field_name: Name of the field.
            reasons: List of detection reasons.

        Returns:
            Inferred PII type.
        """
        normalized = field_name.lower()

        # Check for specific types based on field name
        if any(kw in normalized for kw in ["id", "uuid", "guid"]):
            return "identifier"

        if any(kw in normalized for kw in ["name", "first", "last"]):
            return "name"

        if any(kw in normalized for kw in ["email", "mail"]):
            return "email"

        if any(kw in normalized for kw in ["phone", "tel", "mobile", "fax"]):
            return "phone"

        if any(kw in normalized for kw in ["address", "street", "city", "zip"]):
            return "address"

        if any(kw in normalized for kw in ["ssn", "social", "tax"]):
            return "ssn"

        if any(kw in normalized for kw in ["card", "account", "bank"]):
            return "financial"

        # Default based on detection reason
        if "high_entropy" in reasons or "high_uniqueness" in reasons:
            return "identifier"

        return "unknown"

    def set_entropy_threshold(self, threshold: float) -> None:
        """Set the entropy threshold for high-entropy detection.

        Args:
            threshold: New threshold value.
        """
        self.HIGH_ENTROPY_THRESHOLD = threshold
        logger.debug(f"Entropy threshold set to {threshold}")

    def add_keywords(self, category: str, keywords: List[str]) -> None:
        """Add keywords to a category.

        Args:
            category: Category name.
            keywords: List of keywords to add.
        """
        if category not in self.PII_KEYWORDS:
            self.PII_KEYWORDS[category] = []

        self.PII_KEYWORDS[category].extend(keywords)
        logger.debug(f"Added keywords to '{category}': {keywords}")
