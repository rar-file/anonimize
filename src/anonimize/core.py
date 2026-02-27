"""Core anonymization engine for anonimize.

This module contains the main Anonymizer class that orchestrates
the anonymization process across different data sources.
"""

from typing import Any, Dict, List, Optional, Union
import logging

# Import Phoney for fake data generation
try:
    from phoney import Phoney
except ImportError:
    Phoney = None

from anonimize.detectors.regex import RegexDetector
from anonimize.detectors.heuristic import HeuristicDetector

logger = logging.getLogger(__name__)


class Anonymizer:
    """Main anonymization engine for PII data.
    
    This class orchestrates the anonymization process, providing a unified
    interface for detecting and anonymizing PII across various data sources.
    
    Attributes:
        locale: The locale for generating fake data (default: "en_US").
        preserve_relationships: Whether to preserve relationships between fields.
        seed: Random seed for reproducible results.
    
    Example:
        >>> anon = Anonymizer(locale="en_US", seed=42)
        >>> config = {"name": {"strategy": "replace", "type": "name"}}
        >>> result = anon.anonymize(data, config)
    """

    # Supported anonymization strategies
    STRATEGIES = {"replace", "hash", "mask", "remove"}

    def __init__(
        self,
        locale: str = "en_US",
        preserve_relationships: bool = True,
        seed: Optional[int] = None,
    ):
        """Initialize the Anonymizer.
        
        Args:
            locale: Locale for generating fake data.
            preserve_relationships: Whether to preserve field relationships.
            seed: Random seed for reproducible results.
        """
        self.locale = locale
        self.preserve_relationships = preserve_relationships
        self.seed = seed
        self._config: Dict[str, Any] = {}
        self._value_cache: Dict[str, str] = {}
        
        # Initialize Phoney for fake data generation
        if Phoney:
            self._phoney = Phoney(locale=locale)
            if seed is not None:
                self._phoney.seed(seed)
        else:
            self._phoney = None
            logger.warning("Phoney not installed. Replace strategy will not work.")
        
        # Initialize detectors
        self._regex_detector = RegexDetector()
        self._heuristic_detector = HeuristicDetector()
        
        logger.debug(f"Anonymizer initialized with locale={locale}, seed={seed}")

    def configure(self, config: Dict[str, Any]) -> "Anonymizer":
        """Configure the anonymizer with custom settings.
        
        Args:
            config: Configuration dictionary with settings.
        
        Returns:
            Self for method chaining.
        
        Example:
            >>> anon = Anonymizer()
            >>> anon.configure({"locale": "de_DE", "seed": 123})
        """
        self._config.update(config)
        
        if "locale" in config:
            self.locale = config["locale"]
            if self._phoney:
                self._phoney = Phoney(locale=self.locale)
        
        if "preserve_relationships" in config:
            self.preserve_relationships = config["preserve_relationships"]
        
        if "seed" in config and self._phoney:
            self.seed = config["seed"]
            self._phoney.seed(self.seed)
        
        logger.debug(f"Configuration updated: {config}")
        return self

    def detect_pii(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        method: str = "regex",
    ) -> Dict[str, Dict[str, str]]:
        """Detect PII fields in the data.
        
        Args:
            data: Data to analyze for PII.
            method: Detection method ('regex', 'heuristic', or 'combined').
        
        Returns:
            Dictionary of detected fields with their types.
        
        Example:
            >>> anon = Anonymizer()
            >>> data = {"name": "John Doe", "email": "john@example.com"}
            >>> anon.detect_pii(data)
            {'name': {'type': 'name'}, 'email': {'type': 'email'}}
        """
        if method == "regex":
            return self._regex_detector.detect(data)
        elif method == "heuristic":
            return self._heuristic_detector.detect(data)
        elif method == "combined":
            regex_results = self._regex_detector.detect(data)
            heuristic_results = self._heuristic_detector.detect(data)
            # Merge results, preferring regex detections
            merged = heuristic_results.copy()
            merged.update(regex_results)
            return merged
        else:
            raise ValueError(f"Unknown detection method: {method}")

    def anonymize(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        config: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Anonymize data according to the configuration.
        
        Args:
            data: Data to anonymize.
            config: Anonymization configuration. If None, auto-detect PII.
        
        Returns:
            Anonymized data.
        
        Example:
            >>> anon = Anonymizer()
            >>> data = {"name": "John Doe"}
            >>> config = {"name": {"strategy": "replace", "type": "name"}}
            >>> anon.anonymize(data, config)
            {'name': 'Jane Smith'}
        """
        if config is None:
            # Auto-detect PII and use replace strategy
            detected = self.detect_pii(data)
            config = {
                field: {"strategy": "replace", "type": info.get("type", "string")}
                for field, info in detected.items()
            }
        
        # Handle list of records
        if isinstance(data, list):
            return [self._anonymize_record(record, config) for record in data]
        
        return self._anonymize_record(data, config)

    def _anonymize_record(
        self,
        record: Dict[str, Any],
        config: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Anonymize a single record.
        
        Args:
            record: Record to anonymize.
            config: Anonymization configuration.
        
        Returns:
            Anonymized record.
        """
        result = record.copy()
        
        for field, settings in config.items():
            if field not in result:
                continue
            
            strategy = settings.get("strategy", "replace")
            pii_type = settings.get("type", "string")
            original_value = result[field]
            
            if original_value is None:
                continue
            
            if strategy == "replace":
                result[field] = self._replace_value(original_value, pii_type, field)
            elif strategy == "hash":
                result[field] = self._hash_value(original_value, settings)
            elif strategy == "mask":
                result[field] = self._mask_value(original_value, settings)
            elif strategy == "remove":
                result[field] = None
            else:
                logger.warning(f"Unknown strategy '{strategy}' for field '{field}'")
        
        return result

    def _replace_value(self, value: Any, pii_type: str, field: str) -> str:
        """Replace a value with fake data.
        
        Args:
            value: Original value.
            pii_type: Type of PII to generate.
            field: Field name for relationship preservation.
        
        Returns:
            Fake value.
        """
        if not self._phoney:
            return str(value)
        
        # Check cache for relationship preservation
        cache_key = f"{field}:{value}"
        if self.preserve_relationships and cache_key in self._value_cache:
            return self._value_cache[cache_key]
        
        # Generate fake value based on type
        generators = {
            "name": self._phoney.name,
            "first_name": self._phoney.first_name,
            "last_name": self._phoney.last_name,
            "email": self._phoney.email,
            "phone": self._phoney.phone_number,
            "ssn": self._phoney.ssn,
            "address": self._phoney.street_address,
            "city": self._phoney.city,
            "country": self._phoney.country,
            "company": self._phoney.company,
            "credit_card": self._phoney.credit_card_number,
            "ip_address": self._phoney.ipv4,
            "uuid": self._phoney.uuid4,
        }
        
        generator = generators.get(pii_type, lambda: str(value))
        fake_value = generator()
        
        # Cache the result
        if self.preserve_relationships:
            self._value_cache[cache_key] = fake_value
        
        return fake_value

    def _hash_value(self, value: Any, settings: Dict[str, Any]) -> str:
        """Hash a value.
        
        Args:
            value: Value to hash.
            settings: Configuration including salt and algorithm.
        
        Returns:
            Hashed value.
        """
        import hashlib
        
        salt = settings.get("salt", "")
        algorithm = settings.get("algorithm", "sha256")
        
        value_str = str(value)
        if salt:
            value_str = f"{salt}:{value_str}"
        
        hasher = hashlib.new(algorithm)
        hasher.update(value_str.encode("utf-8"))
        return hasher.hexdigest()

    def _mask_value(self, value: Any, settings: Dict[str, Any]) -> str:
        """Mask a value.
        
        Args:
            value: Value to mask.
            settings: Configuration including mask_char and preserve_last.
        
        Returns:
            Masked value.
        """
        value_str = str(value)
        mask_char = settings.get("mask_char", "*")
        preserve_last = settings.get("preserve_last", 4)
        
        if len(value_str) <= preserve_last:
            return mask_char * len(value_str)
        
        masked_length = len(value_str) - preserve_last
        return mask_char * masked_length + value_str[-preserve_last:]

    def clear_cache(self) -> None:
        """Clear the value cache used for relationship preservation."""
        self._value_cache.clear()
        logger.debug("Value cache cleared")

    def get_stats(self) -> Dict[str, Any]:
        """Get anonymization statistics.
        
        Returns:
            Dictionary with statistics.
        """
        return {
            "cached_values": len(self._value_cache),
            "locale": self.locale,
            "preserve_relationships": self.preserve_relationships,
        }
