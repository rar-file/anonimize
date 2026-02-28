"""Tests for specialized anonymizers."""

import pytest
from anonimize.anonymizers.specialized import (
    EmailAnonymizer,
    PhoneAnonymizer,
    SSNAnonymizer,
    AnonymizationStrategy,
    get_anonymizer,
    register_anonymizer,
    BaseSpecializedAnonymizer,
)


class TestEmailAnonymizer:
    """Test cases for EmailAnonymizer."""

    def test_init(self):
        """Test anonymizer initialization."""
        anon = EmailAnonymizer()
        assert anon.PII_TYPE == "email"
        assert anon.seed is None

    def test_init_with_seed(self):
        """Test initialization with seed."""
        anon = EmailAnonymizer(seed=42)
        assert anon.seed == 42

    def test_validate_valid_emails(self):
        """Test validation of valid emails."""
        anon = EmailAnonymizer()

        valid_emails = [
            "test@example.com",
            "user.name@domain.co.uk",
            "user+tag@example.com",
            "123@test.org",
            "UPPER@EXAMPLE.COM",
        ]

        for email in valid_emails:
            assert anon.validate(email), f"Should validate: {email}"

    def test_validate_invalid_emails(self):
        """Test validation of invalid emails."""
        anon = EmailAnonymizer()

        invalid_emails = [
            "not-an-email",
            "@example.com",
            "user@",
            "user..name@example.com",
            "",
            "spaces in@email.com",
        ]

        for email in invalid_emails:
            assert not anon.validate(email), f"Should not validate: {email}"

    def test_replace_strategy(self):
        """Test replace strategy."""
        anon = EmailAnonymizer(seed=42)

        result = anon.anonymize(
            "john@example.com", strategy=AnonymizationStrategy.REPLACE
        )

        assert result.value != "john@example.com"
        assert result.original_type == "email"
        assert result.strategy_used == "replace"
        assert "@" in result.value

    def test_replace_with_domain_preservation(self):
        """Test replace with domain preservation."""
        anon = EmailAnonymizer(seed=42)

        result = anon.anonymize(
            "user@company.com",
            strategy=AnonymizationStrategy.REPLACE,
            preserve_domain=True,
        )

        assert result.value.endswith("@company.com")
        assert "@" in result.value

    def test_replace_with_tld_preservation(self):
        """Test replace with TLD preservation."""
        anon = EmailAnonymizer(seed=42)

        result = anon.anonymize(
            "user@example.org",
            strategy=AnonymizationStrategy.REPLACE,
            preserve_tld=True,
        )

        assert result.value.endswith(".org")

    def test_hash_strategy(self):
        """Test hash strategy."""
        anon = EmailAnonymizer()

        result = anon.anonymize("test@example.com", strategy=AnonymizationStrategy.HASH)

        assert result.value != "test@example.com"
        assert len(result.value) == 64  # SHA256 hex length

    def test_hash_with_salt(self):
        """Test hash with salt."""
        anon = EmailAnonymizer()

        result1 = anon.anonymize(
            "test@example.com", strategy=AnonymizationStrategy.HASH, salt="salt1"
        )
        result2 = anon.anonymize(
            "test@example.com", strategy=AnonymizationStrategy.HASH, salt="salt2"
        )

        assert result1.value != result2.value

    def test_mask_strategy(self):
        """Test mask strategy."""
        anon = EmailAnonymizer()

        result = anon.anonymize("user@example.com", strategy=AnonymizationStrategy.MASK)

        assert result.value.startswith("****")
        assert "@example.com" in result.value

    def test_mask_custom_char(self):
        """Test mask with custom character."""
        anon = EmailAnonymizer()

        result = anon.anonymize(
            "user@example.com", strategy=AnonymizationStrategy.MASK, mask_char="#"
        )

        assert "#" in result.value
        assert "*" not in result.value

    def test_remove_strategy(self):
        """Test remove strategy."""
        anon = EmailAnonymizer()

        result = anon.anonymize(
            "test@example.com", strategy=AnonymizationStrategy.REMOVE
        )

        assert result.value == ""

    def test_deterministic_strategy(self):
        """Test deterministic strategy returns same result for same input."""
        anon = EmailAnonymizer()

        result1 = anon.anonymize(
            "test@example.com", strategy=AnonymizationStrategy.DETERMINISTIC
        )
        result2 = anon.anonymize(
            "test@example.com", strategy=AnonymizationStrategy.DETERMINISTIC
        )

        assert result1.value == result2.value

    def test_deterministic_different_inputs(self):
        """Test deterministic strategy returns different results for different inputs."""
        anon = EmailAnonymizer()

        result1 = anon.anonymize(
            "user1@example.com", strategy=AnonymizationStrategy.DETERMINISTIC
        )
        result2 = anon.anonymize(
            "user2@example.com", strategy=AnonymizationStrategy.DETERMINISTIC
        )

        assert result1.value != result2.value

    def test_is_disposable(self):
        """Test disposable email detection."""
        anon = EmailAnonymizer()

        assert anon.is_disposable("user@tempmail.com")
        assert anon.is_disposable("user@MAILINATOR.COM")  # Case insensitive
        assert not anon.is_disposable("user@gmail.com")

    def test_empty_value(self):
        """Test handling of empty value."""
        anon = EmailAnonymizer()

        result = anon.anonymize("")

        assert result.value == ""
        assert result.metadata.get("error") == "empty_or_invalid_value"


class TestPhoneAnonymizer:
    """Test cases for PhoneAnonymizer."""

    def test_init(self):
        """Test anonymizer initialization."""
        anon = PhoneAnonymizer()
        assert anon.PII_TYPE == "phone"

    def test_validate_us_numbers(self):
        """Test validation of US phone numbers."""
        anon = PhoneAnonymizer()

        valid_numbers = [
            "+1 (555) 123-4567",
            "555-123-4567",
            "555.123.4567",
            "5551234567",
            "1-555-123-4567",
        ]

        for number in valid_numbers:
            assert anon.validate(number), f"Should validate: {number}"

    def test_validate_international_numbers(self):
        """Test validation of international phone numbers."""
        anon = PhoneAnonymizer()

        valid_numbers = [
            "+44 20 7946 0958",  # UK
            "+49 30 12345678",  # Germany
            "+33 1 42 86 82 00",  # France
        ]

        for number in valid_numbers:
            assert anon.validate(number), f"Should validate: {number}"

    def test_replace_strategy(self):
        """Test replace strategy."""
        anon = PhoneAnonymizer(seed=42)

        result = anon.anonymize("+1 (555) 123-4567")

        assert result.value != "+1 (555) 123-4567"
        assert result.strategy_used == "replace"

    def test_replace_preserve_country(self):
        """Test replace with country preservation."""
        anon = PhoneAnonymizer(seed=42)

        result = anon.anonymize("+44 20 7946 0958", preserve_country=True)

        assert result.value.startswith("+44")

    def test_replace_preserve_format(self):
        """Test replace with format preservation."""
        anon = PhoneAnonymizer(seed=42)

        original = "+1 (555) 123-4567"
        result = anon.anonymize(original, preserve_format=True)

        # Check format is preserved (has parentheses and dash)
        assert "(" in result.value or "-" in result.value

    def test_mask_strategy(self):
        """Test mask strategy for phone."""
        anon = PhoneAnonymizer()

        result = anon.anonymize(
            "+1 (555) 123-4567", strategy=AnonymizationStrategy.MASK
        )

        assert "*" in result.value
        assert "4567" in result.value  # Last 4 preserved

    def test_mask_preserve_country(self):
        """Test mask with country preservation."""
        anon = PhoneAnonymizer()

        result = anon.anonymize(
            "+44 20 7946 0958",
            strategy=AnonymizationStrategy.MASK,
            preserve_country=True,
        )

        assert result.value.startswith("+44")


class TestSSNAnonymizer:
    """Test cases for SSNAnonymizer."""

    def test_init(self):
        """Test anonymizer initialization."""
        anon = SSNAnonymizer()
        assert anon.PII_TYPE == "ssn"

    def test_validate_valid_ssns(self):
        """Test validation of valid SSNs."""
        anon = SSNAnonymizer()

        valid_ssns = [
            "123-45-6789",
            "555-12-3456",
            "001-01-0001",
        ]

        for ssn in valid_ssns:
            assert anon.validate(ssn), f"Should validate: {ssn}"

    def test_validate_invalid_ssns(self):
        """Test validation of invalid SSNs."""
        anon = SSNAnonymizer()

        invalid_ssns = [
            "000-12-3456",  # Invalid area
            "666-12-3456",  # Invalid area
            "123-00-4567",  # Invalid group
            "123-45-0000",  # Invalid serial
            "078-05-1120",  # Known invalid (Woolworth's)
        ]

        for ssn in invalid_ssns:
            assert not anon.validate(ssn), f"Should not validate: {ssn}"

    def test_default_mask_strategy(self):
        """Test that SSN defaults to mask strategy."""
        anon = SSNAnonymizer()

        result = anon.anonymize("123-45-6789")

        assert result.value == "***-**-6789"
        assert result.strategy_used == "mask"

    def test_mask_strategy(self):
        """Test explicit mask strategy."""
        anon = SSNAnonymizer()

        result = anon.anonymize("123-45-6789", strategy=AnonymizationStrategy.MASK)

        assert result.value.startswith("***-**-")
        assert result.value.endswith("6789")

    def test_mask_custom_char(self):
        """Test mask with custom character."""
        anon = SSNAnonymizer()

        result = anon.anonymize(
            "123-45-6789", strategy=AnonymizationStrategy.MASK, mask_char="#"
        )

        assert result.value == "###-##-6789"

    def test_replace_strategy(self):
        """Test replace strategy."""
        anon = SSNAnonymizer(seed=42)

        result = anon.anonymize("123-45-6789", strategy=AnonymizationStrategy.REPLACE)

        assert result.value != "123-45-6789"
        assert len(result.value.replace("-", "")) == 9

    def test_hash_strategy(self):
        """Test hash strategy."""
        anon = SSNAnonymizer()

        result = anon.anonymize("123-45-6789", strategy=AnonymizationStrategy.HASH)

        assert len(result.value) == 64

    def test_generate_fake_valid_format(self):
        """Test that generated fake SSNs are valid."""
        anon = SSNAnonymizer(seed=42)

        fake = anon._generate_fake("123-45-6789")

        # Should match SSN pattern
        assert len(fake.replace("-", "")) == 9
        parts = fake.split("-")
        assert len(parts) == 3


class TestAnonymizerFactory:
    """Test cases for anonymizer factory functions."""

    def test_get_email_anonymizer(self):
        """Test getting email anonymizer."""
        anon = get_anonymizer("email")
        assert isinstance(anon, EmailAnonymizer)

    def test_get_phone_anonymizer(self):
        """Test getting phone anonymizer."""
        anon = get_anonymizer("phone")
        assert isinstance(anon, PhoneAnonymizer)

    def test_get_ssn_anonymizer(self):
        """Test getting SSN anonymizer."""
        anon = get_anonymizer("ssn")
        assert isinstance(anon, SSNAnonymizer)

    def test_get_anonymizer_case_insensitive(self):
        """Test that type is case insensitive."""
        anon1 = get_anonymizer("EMAIL")
        anon2 = get_anonymizer("Email")
        assert isinstance(anon1, EmailAnonymizer)
        assert isinstance(anon2, EmailAnonymizer)

    def test_get_anonymizer_unknown_type(self):
        """Test error on unknown type."""
        with pytest.raises(ValueError, match="Unknown PII type"):
            get_anonymizer("unknown")

    def test_register_custom_anonymizer(self):
        """Test registering custom anonymizer."""

        class CustomAnonymizer(BaseSpecializedAnonymizer):
            PII_TYPE = "custom"

            def validate(self, value: str) -> bool:
                return True

            def _generate_fake(self, original: str, **kwargs) -> str:
                return "custom_fake"

        register_anonymizer("custom_type", CustomAnonymizer)

        anon = get_anonymizer("custom_type")
        assert isinstance(anon, CustomAnonymizer)

    def test_register_invalid_anonymizer(self):
        """Test error on invalid anonymizer registration."""

        class NotAnAnonymizer:
            pass

        with pytest.raises(
            TypeError, match="must inherit from BaseSpecializedAnonymizer"
        ):
            register_anonymizer("bad", NotAnAnonymizer)


class TestAnonymizationResult:
    """Test cases for AnonymizationResult."""

    def test_result_structure(self):
        """Test result dataclass structure."""
        anon = EmailAnonymizer(seed=42)

        result = anon.anonymize("test@example.com")

        assert hasattr(result, "value")
        assert hasattr(result, "original_type")
        assert hasattr(result, "strategy_used")
        assert hasattr(result, "metadata")

        assert isinstance(result.metadata, dict)
