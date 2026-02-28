"""Specialized anonymizers for specific PII types.

This module provides sophisticated anonymizers for common PII types
with support for validation, formatting preservation, and domain-specific logic.
"""

import hashlib
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class AnonymizationStrategy(Enum):
    """Available anonymization strategies."""

    REPLACE = "replace"
    HASH = "hash"
    MASK = "mask"
    REMOVE = "remove"
    PRESERVE_FORMAT = "preserve_format"
    DETERMINISTIC = "deterministic"


@dataclass
class AnonymizationResult:
    """Result of anonymization operation.

    Attributes:
        value: The anonymized value
        original_type: Type of the original data
        strategy_used: Strategy that was applied
        metadata: Additional metadata about the transformation
    """

    value: str
    original_type: str
    strategy_used: str
    metadata: Dict[str, Any]


class BaseSpecializedAnonymizer(ABC):
    """Base class for specialized anonymizers.

    Provides common functionality for validation, strategy selection,
    and result tracking.
    """

    # Type identifier for this anonymizer
    PII_TYPE: str = ""

    def __init__(self, seed: Optional[int] = None):
        """Initialize the anonymizer.

        Args:
            seed: Random seed for deterministic results
        """
        self.seed = seed
        self._faker = None
        self._init_faker()
        self._deterministic_cache: Dict[str, str] = {}

    def _init_faker(self) -> None:
        """Initialize Faker instance for fake data generation."""
        try:
            from faker import Faker

            self._faker = Faker()
            if self.seed is not None:
                Faker.seed(self.seed)
        except ImportError:
            logger.warning("Faker not installed. Using fallback generation.")
            self._faker = None

    @abstractmethod
    def validate(self, value: str) -> bool:
        """Validate that the value is of the expected type.

        Args:
            value: Value to validate

        Returns:
            True if valid, False otherwise
        """
        pass

    @abstractmethod
    def _generate_fake(self, original: str, preserve_domain: bool = False) -> str:
        """Generate a fake value.

        Args:
            original: Original value for context
            preserve_domain: Whether to preserve certain characteristics

        Returns:
            Fake value
        """
        pass

    def anonymize(
        self,
        value: str,
        strategy: AnonymizationStrategy = AnonymizationStrategy.REPLACE,
        **kwargs,
    ) -> AnonymizationResult:
        """Anonymize a value using the specified strategy.

        Args:
            value: Value to anonymize
            strategy: Anonymization strategy to use
            **kwargs: Strategy-specific options

        Returns:
            AnonymizationResult with the anonymized value and metadata
        """
        if not value or not isinstance(value, str):
            return AnonymizationResult(
                value=value if value else "",
                original_type=self.PII_TYPE,
                strategy_used="none",
                metadata={"error": "empty_or_invalid_value"},
            )

        is_valid = self.validate(value)

        if strategy == AnonymizationStrategy.REPLACE:
            result = self._do_replace(value, **kwargs)
        elif strategy == AnonymizationStrategy.HASH:
            result = self._do_hash(value, **kwargs)
        elif strategy == AnonymizationStrategy.MASK:
            result = self._do_mask(value, **kwargs)
        elif strategy == AnonymizationStrategy.REMOVE:
            result = ""
        elif strategy == AnonymizationStrategy.PRESERVE_FORMAT:
            result = self._do_preserve_format(value, **kwargs)
        elif strategy == AnonymizationStrategy.DETERMINISTIC:
            result = self._do_deterministic(value, **kwargs)
        else:
            result = value

        return AnonymizationResult(
            value=result,
            original_type=self.PII_TYPE,
            strategy_used=strategy.value,
            metadata={
                "was_valid": is_valid,
                "original_length": len(value),
            },
        )

    def _do_replace(self, value: str, preserve_domain: bool = False, **kwargs) -> str:
        """Replace with fake value."""
        return self._generate_fake(value, preserve_domain=preserve_domain)

    def _do_hash(
        self, value: str, algorithm: str = "sha256", salt: str = "", **kwargs
    ) -> str:
        """Hash the value."""
        hasher = hashlib.new(algorithm)
        salted = f"{salt}:{value}" if salt else value
        hasher.update(salted.encode("utf-8"))
        return hasher.hexdigest()

    def _do_mask(
        self,
        value: str,
        mask_char: str = "*",
        preserve_first: int = 0,
        preserve_last: int = 4,
        **kwargs,
    ) -> str:
        """Mask the value."""
        if len(value) <= preserve_first + preserve_last:
            return mask_char * len(value)

        first = value[:preserve_first] if preserve_first > 0 else ""
        last = value[-preserve_last:] if preserve_last > 0 else ""
        middle_len = len(value) - preserve_first - preserve_last

        return first + (mask_char * middle_len) + last

    def _do_preserve_format(self, value: str, **kwargs) -> str:
        """Replace while preserving format."""
        fake = self._generate_fake(value, preserve_domain=False)
        return self._apply_format(value, fake)

    def _do_deterministic(self, value: str, **kwargs) -> str:
        """Generate deterministic fake value (same input = same output)."""
        if value in self._deterministic_cache:
            return self._deterministic_cache[value]

        # Use hash to seed the fake generation for determinism
        hash_int = int(hashlib.sha256(value.encode()).hexdigest(), 16)
        old_seed = self.seed
        self.seed = hash_int % (2**32)
        self._init_faker()

        fake = self._generate_fake(value, preserve_domain=False)

        self.seed = old_seed
        self._init_faker()

        self._deterministic_cache[value] = fake
        return fake

    def _apply_format(self, template: str, value: str) -> str:
        """Apply the format of template to value.

        Args:
            template: Original value with desired format
            value: New value to format

        Returns:
            Formatted value
        """
        # Simple approach: replace alphanumeric characters
        result = []
        value_idx = 0

        for char in template:
            if char.isalnum() and value_idx < len(value):
                result.append(value[value_idx])
                value_idx += 1
            else:
                result.append(char)

        # Append any remaining characters from value
        while value_idx < len(value):
            result.append(value[value_idx])
            value_idx += 1

        return "".join(result)


class EmailAnonymizer(BaseSpecializedAnonymizer):
    """Anonymizer for email addresses.

    Supports domain preservation, username generation, and various
    email format variations.

    Example:
        >>> anon = EmailAnonymizer()
        >>> result = anon.anonymize("john.doe@example.com")
        >>> result.value
        'alice.smith@other.com'
        >>> result = anon.anonymize("john@company.com", preserve_domain=True)
        >>> result.value  # Domain preserved
        'alice@company.com'
    """

    PII_TYPE = "email"

    # Email validation regex (simplified but covers most cases)
    EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    # Regex to detect consecutive dots which are invalid
    CONSECUTIVE_DOTS_REGEX = re.compile(r"\.\.+")

    # Common disposable email domains
    DISPOSABLE_DOMAINS = {
        "tempmail.com",
        "throwaway.com",
        "mailinator.com",
        "guerrillamail.com",
        "yopmail.com",
    }

    def validate(self, value: str) -> bool:
        """Validate email format."""
        if not value or not isinstance(value, str):
            return False
        stripped = value.strip()
        # Check basic regex and also reject consecutive dots
        if not bool(self.EMAIL_REGEX.match(stripped)):
            return False
        # Reject emails with consecutive dots in local part
        if "@" in stripped:
            local = stripped.split("@")[0]
            if self.CONSECUTIVE_DOTS_REGEX.search(local):
                return False
        return True

    def _generate_fake(self, original: str, preserve_domain: bool = False) -> str:
        """Generate fake email."""
        if preserve_domain:
            domain = original.split("@")[1] if "@" in original else "example.com"
        else:
            if self._faker:
                domain = self._faker.free_email_domain()
            else:
                domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com"]
                domain = domains[hash(original) % len(domains)]

        if self._faker:
            username = self._faker.user_name()
        else:
            # Fallback username generation
            import random

            names = ["user", "person", "account", "contact", "member"]
            username = f"{random.choice(names)}{random.randint(1000, 9999)}"

        return f"{username}@{domain}"

    def anonymize(
        self,
        value: str,
        strategy: AnonymizationStrategy = AnonymizationStrategy.REPLACE,
        preserve_domain: bool = False,
        preserve_tld: bool = False,
        **kwargs,
    ) -> AnonymizationResult:
        """Anonymize email with additional options.

        Args:
            value: Email to anonymize
            strategy: Anonymization strategy
            preserve_domain: Keep the original domain
            preserve_tld: Keep only the TLD (e.g., .com, .org)
            **kwargs: Additional options
        """
        if preserve_tld and not preserve_domain:
            kwargs["preserve_tld"] = True

        kwargs["preserve_domain"] = preserve_domain
        return super().anonymize(value, strategy, **kwargs)

    def _do_replace(
        self,
        value: str,
        preserve_domain: bool = False,
        preserve_tld: bool = False,
        **kwargs,
    ) -> str:
        """Replace email with fake."""
        if preserve_tld and not preserve_domain:
            parts = value.split("@")
            if len(parts) == 2:
                tld = parts[1].split(".")[-1] if "." in parts[1] else "com"
                if self._faker:
                    local = self._faker.user_name()
                    domain = f"{self._faker.domain_word()}.{tld}"
                else:
                    import random

                    local = f"user{random.randint(1000, 9999)}"
                    domain = f"example.{tld}"
                return f"{local}@{domain}"

        return self._generate_fake(value, preserve_domain)

    def _do_mask(
        self, value: str, mask_char: str = "*", preserve_domain: bool = True, **kwargs
    ) -> str:
        """Mask email, preserving the domain part."""
        if "@" not in value:
            return mask_char * len(value)

        local, domain = value.rsplit("@", 1)
        masked_local = mask_char * len(local)
        return f"{masked_local}@{domain}"

    def is_disposable(self, email: str) -> bool:
        """Check if email uses a disposable domain.

        Args:
            email: Email address to check

        Returns:
            True if disposable domain
        """
        if "@" not in email:
            return False
        domain = email.split("@")[1].lower()
        return domain in self.DISPOSABLE_DOMAINS


class PhoneAnonymizer(BaseSpecializedAnonymizer):
    """Anonymizer for phone numbers.

    Supports multiple international formats and preserves
    country codes when desired.

    Example:
        >>> anon = PhoneAnonymizer()
        >>> anon.anonymize("+1 (555) 123-4567").value
        '+1 (555) 987-6543'
        >>> anon.anonymize("+44 20 7946 0958", preserve_country=True).value
        '+44 20 1234 5678'
    """

    PII_TYPE = "phone"

    # Phone patterns by region
    PATTERNS = {
        "US": re.compile(
            r"^(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$"
        ),
        "UK": re.compile(
            r"^(\+?44[-.\s]?)?0?([0-9]{2,4})[-.\s]?([0-9]{3,4})[-.\s]?([0-9]{3,4})$"
        ),
        "EU": re.compile(
            r"^(\+?[0-9]{1,3})[-.\s]?([0-9]{1,4})[-.\s]?([0-9]{2,4})[-.\s]?([0-9]{2,4})$"
        ),
        "GENERIC": re.compile(r"^[+]?[0-9\s.-]{7,20}$"),
    }

    # Country code mapping
    COUNTRY_CODES = {
        "1": "US",
        "44": "UK",
        "49": "DE",
        "33": "FR",
        "39": "IT",
        "34": "ES",
        "31": "NL",
        "41": "CH",
        "43": "AT",
        "32": "BE",
    }

    def validate(self, value: str) -> bool:
        """Validate phone number format."""
        cleaned = re.sub(r"[\s.-]", "", value)
        for pattern in self.PATTERNS.values():
            if pattern.match(value) or pattern.match(cleaned):
                return True
        return False

    def _extract_country_code(self, phone: str) -> Tuple[str, str]:
        """Extract country code from phone number.

        Args:
            phone: Phone number

        Returns:
            (country_code, rest_of_number) tuple
        """
        cleaned = re.sub(r"[^\d+]", "", phone)

        if cleaned.startswith("+"):
            # Look for country code
            for code in sorted(self.COUNTRY_CODES.keys(), key=len, reverse=True):
                if cleaned[1:].startswith(code):
                    return (code, cleaned[1 + len(code) :])
            return ("", cleaned[1:])
        elif cleaned.startswith("1") and len(cleaned) == 11:
            return ("1", cleaned[1:])

        return ("", cleaned)

    def _generate_fake(self, original: str, preserve_country: bool = False) -> str:
        """Generate fake phone number."""
        country_code, rest = self._extract_country_code(original)

        if preserve_country and country_code:
            cc = country_code
        else:
            cc = "1"  # Default to US

        if self._faker:
            # Generate appropriate format based on country
            if cc == "1":
                fake = self._faker.numerify(text="(###) ###-####")
            elif cc == "44":
                fake = self._faker.numerify(text="20 #### ####")
            else:
                fake = self._faker.numerify(text="##########")
        else:
            import random

            fake = "".join(str(random.randint(0, 9)) for _ in range(10))
            fake = f"({fake[:3]}) {fake[3:6]}-{fake[6:]}"

        return f"+{cc} {fake}" if cc else fake

    def anonymize(
        self,
        value: str,
        strategy: AnonymizationStrategy = AnonymizationStrategy.REPLACE,
        preserve_country: bool = False,
        preserve_format: bool = True,
        **kwargs,
    ) -> AnonymizationResult:
        """Anonymize phone number.

        Args:
            value: Phone number to anonymize
            strategy: Anonymization strategy
            preserve_country: Keep the country code
            preserve_format: Keep the formatting characters
        """
        kwargs["preserve_country"] = preserve_country
        kwargs["preserve_format"] = preserve_format
        return super().anonymize(value, strategy, **kwargs)

    def _do_replace(
        self,
        value: str,
        preserve_country: bool = False,
        preserve_format: bool = True,
        **kwargs,
    ) -> str:
        """Replace phone with fake."""
        fake = self._generate_fake(value, preserve_country)

        if preserve_format and not preserve_country:
            # Only apply format preservation if not preserving country
            # (country preservation already includes formatting)
            return self._apply_format(value, fake)
        return fake

    def _do_mask(
        self, value: str, mask_char: str = "*", preserve_country: bool = False, **kwargs
    ) -> str:
        """Mask phone number."""
        country_code, _ = self._extract_country_code(value)

        if preserve_country and country_code:
            # Mask only the local part
            prefix = f"+{country_code} "
            local = (
                value[len(prefix) :]
                if value.startswith(prefix)
                else value[value.find(str(country_code)) + len(str(country_code)) :]
            )
            masked_local = self._mask_digits(local, mask_char)
            return prefix + masked_local

        return self._mask_digits(value, mask_char)

    def _mask_digits(self, value: str, mask_char: str) -> str:
        """Mask all digits in the value."""
        result = []
        digits_seen = 0

        for char in value:
            if char.isdigit():
                digits_seen += 1
                # Keep last 4 digits visible
                if digits_seen <= len([c for c in value if c.isdigit()]) - 4:
                    result.append(mask_char)
                else:
                    result.append(char)
            else:
                result.append(char)

        return "".join(result)


class SSNAnonymizer(BaseSpecializedAnonymizer):
    """Anonymizer for Social Security Numbers (US format).

    Handles various SSN formats and provides secure anonymization
    suitable for HIPAA and other compliance requirements.

    Example:
        >>> anon = SSNAnonymizer()
        >>> anon.anonymize("123-45-6789").value
        '987-65-4321'
        >>> anon.anonymize("123-45-6789", strategy=AnonymizationStrategy.MASK).value
        '***-**-6789'
    """

    PII_TYPE = "ssn"

    # SSN validation pattern
    # Groups: 001-899 (first), 01-99 (second), 0001-9999 (third)
    SSN_PATTERN = re.compile(
        r"^(?!000|666|9\d{2})([0-8]\d{2}|7[0-2]0)-?(?!00)(\d{2})-?(?!0000)(\d{4})$"
    )

    # Invalid SSNs (test numbers, etc.)
    INVALID_SSNS = {"078-05-1120", "219-09-9999", "457-55-5462"}

    def validate(self, value: str) -> bool:
        """Validate SSN format.

        Checks for valid SSN format excluding invalid/test numbers.
        """
        cleaned = value.replace("-", "").replace(" ", "")

        if len(cleaned) != 9 or not cleaned.isdigit():
            return False

        # Check against invalid list
        formatted = f"{cleaned[:3]}-{cleaned[3:5]}-{cleaned[5:]}"
        if formatted in self.INVALID_SSNS:
            return False

        # Validate with regex
        return bool(self.SSN_PATTERN.match(value.strip()))

    def _generate_fake(self, original: str, **kwargs) -> str:
        """Generate a valid but fake SSN.

        Uses the SSA's high group list rules for valid SSN generation.
        """
        import random

        if self._faker:
            # Use Faker's SSN generator
            fake = self._faker.ssn()
        else:
            # Generate valid SSN
            # Area number: 001-899, excluding 666, 900-999
            while True:
                area = random.randint(1, 899)
                if area != 666:
                    break

            # Group number: 01-99
            group = random.randint(1, 99)

            # Serial number: 0001-9999
            serial = random.randint(1, 9999)

            fake = f"{area:03d}-{group:02d}-{serial:04d}"

        return fake

    def anonymize(
        self,
        value: str,
        strategy: AnonymizationStrategy = AnonymizationStrategy.MASK,
        **kwargs,
    ) -> AnonymizationResult:
        """Anonymize SSN.

        Defaults to MASK strategy for security.

        Args:
            value: SSN to anonymize
            strategy: Anonymization strategy (default: MASK)
        """
        return super().anonymize(value, strategy, **kwargs)

    def _do_mask(self, value: str, mask_char: str = "*", **kwargs) -> str:
        """Mask SSN, showing only last 4 digits."""
        digits = re.sub(r"\D", "", value)
        if len(digits) == 9:
            return f"{mask_char*3}-{mask_char*2}-{digits[5:]}"
        return mask_char * len(value)

    def _do_hash(
        self, value: str, algorithm: str = "sha256", salt: str = "", **kwargs
    ) -> str:
        """Hash SSN securely."""
        # SSNs require strong hashing
        hasher = hashlib.new(algorithm)
        salted = f"{salt}:ssn:{value}" if salt else f"ssn:{value}"
        hasher.update(salted.encode("utf-8"))
        return hasher.hexdigest()


# Factory for creating anonymizers
ANONYMIZER_REGISTRY: Dict[str, type] = {
    "email": EmailAnonymizer,
    "phone": PhoneAnonymizer,
    "ssn": SSNAnonymizer,
}


def get_anonymizer(
    pii_type: str, seed: Optional[int] = None
) -> BaseSpecializedAnonymizer:
    """Get an anonymizer for a specific PII type.

    Args:
        pii_type: Type of PII ('email', 'phone', 'ssn', etc.)
        seed: Random seed for reproducibility

    Returns:
        Specialized anonymizer instance

    Raises:
        ValueError: If PII type is not supported
    """
    pii_type = pii_type.lower()

    if pii_type not in ANONYMIZER_REGISTRY:
        raise ValueError(
            f"Unknown PII type: {pii_type}. "
            f"Supported types: {list(ANONYMIZER_REGISTRY.keys())}"
        )

    return ANONYMIZER_REGISTRY[pii_type](seed=seed)


def register_anonymizer(pii_type: str, anonymizer_class: type) -> None:
    """Register a custom anonymizer.

    Args:
        pii_type: Type identifier for the anonymizer
        anonymizer_class: Class inheriting from BaseSpecializedAnonymizer
    """
    if not issubclass(anonymizer_class, BaseSpecializedAnonymizer):
        raise TypeError("Anonymizer must inherit from BaseSpecializedAnonymizer")

    ANONYMIZER_REGISTRY[pii_type.lower()] = anonymizer_class
    logger.info(f"Registered anonymizer for type: {pii_type}")
