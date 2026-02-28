"""Tests for the core Anonymizer class."""

import pytest
from unittest.mock import Mock, patch

from anonimize.core import Anonymizer


class TestAnonymizer:
    """Test cases for the Anonymizer class."""

    def test_init_default(self):
        """Test Anonymizer initialization with default values."""
        anon = Anonymizer()
        assert anon.locale == "en_US"
        assert anon.preserve_relationships is True
        assert anon.seed is None

    def test_init_custom(self):
        """Test Anonymizer initialization with custom values."""
        anon = Anonymizer(locale="de_DE", preserve_relationships=False, seed=42)
        assert anon.locale == "de_DE"
        assert anon.preserve_relationships is False
        assert anon.seed == 42

    def test_configure(self):
        """Test configuration method."""
        anon = Anonymizer()
        result = anon.configure({"locale": "fr_FR", "seed": 123})

        assert result is anon  # Returns self for chaining
        assert anon.locale == "fr_FR"
        assert anon.seed == 123

    def test_anonymize_with_replace_strategy(self):
        """Test anonymize with replace strategy."""
        anon = Anonymizer(seed=42)

        data = {"name": "John Doe", "email": "john@example.com"}
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }

        result = anon.anonymize(data, config)

        assert result["name"] != "John Doe"
        assert result["email"] != "john@example.com"

    def test_anonymize_with_hash_strategy(self):
        """Test anonymize with hash strategy."""
        anon = Anonymizer()

        data = {"email": "john@example.com"}
        config = {
            "email": {"strategy": "hash", "type": "email"},
        }

        result = anon.anonymize(data, config)

        assert result["email"] != "john@example.com"
        assert len(result["email"]) == 64  # SHA256 hex length

    def test_anonymize_with_mask_strategy(self):
        """Test anonymize with mask strategy."""
        anon = Anonymizer()

        data = {"phone": "123-456-7890"}
        config = {
            "phone": {"strategy": "mask", "type": "phone", "preserve_last": 4},
        }

        result = anon.anonymize(data, config)

        assert result["phone"].endswith("7890")
        assert result["phone"].startswith("*")

    def test_anonymize_with_remove_strategy(self):
        """Test anonymize with remove strategy."""
        anon = Anonymizer()

        data = {"ssn": "123-45-6789"}
        config = {
            "ssn": {"strategy": "remove", "type": "ssn"},
        }

        result = anon.anonymize(data, config)

        assert result["ssn"] is None

    def test_anonymize_list_of_records(self):
        """Test anonymizing a list of records."""
        anon = Anonymizer(seed=42)

        data = [
            {"name": "John Doe"},
            {"name": "Jane Smith"},
        ]
        config = {
            "name": {"strategy": "replace", "type": "name"},
        }

        result = anon.anonymize(data, config)

        assert len(result) == 2
        assert all(r["name"] not in ["John Doe", "Jane Smith"] for r in result)

    def test_preserve_relationships(self):
        """Test that relationships are preserved when enabled."""
        anon = Anonymizer(seed=42, preserve_relationships=True)

        data = [
            {"name": "John", "email": "john@example.com"},
            {"name": "John", "email": "john@example.com"},
        ]
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }

        result = anon.anonymize(data, config)

        # Same original values should produce same anonymized values
        assert result[0]["name"] == result[1]["name"]
        assert result[0]["email"] == result[1]["email"]

    def test_clear_cache(self):
        """Test clearing the value cache."""
        anon = Anonymizer(seed=42, preserve_relationships=True)

        data = {"name": "John"}
        config = {"name": {"strategy": "replace", "type": "name"}}

        anon.anonymize(data, config)
        assert len(anon._value_cache) > 0

        anon.clear_cache()
        assert len(anon._value_cache) == 0

    def test_get_stats(self):
        """Test getting anonymization statistics."""
        anon = Anonymizer(seed=42, preserve_relationships=True)

        data = {"name": "John"}
        config = {"name": {"strategy": "replace", "type": "name"}}

        anon.anonymize(data, config)
        stats = anon.get_stats()

        assert "cached_values" in stats
        assert "locale" in stats
        assert "preserve_relationships" in stats

    def test_detect_pii_regex(self):
        """Test PII detection with regex method."""
        anon = Anonymizer()

        data = {
            "email": "test@example.com",
            "phone": "123-456-7890",
            "name": "John Doe",
        }

        result = anon.detect_pii(data, method="regex")

        assert "email" in result
        assert result["email"]["type"] == "email"

    def test_detect_pii_heuristic(self):
        """Test PII detection with heuristic method."""
        anon = Anonymizer()

        data = {
            "user_name": "john_doe",
            "user_email": "test@example.com",
        }

        result = anon.detect_pii(data, method="heuristic")

        # Should detect based on field names
        assert len(result) > 0

    def test_detect_pii_combined(self):
        """Test PII detection with combined method."""
        anon = Anonymizer()

        data = {
            "email": "test@example.com",
            "user_name": "john_doe",
        }

        result = anon.detect_pii(data, method="combined")

        assert len(result) >= 1

    def test_detect_pii_invalid_method(self):
        """Test PII detection with invalid method raises error."""
        anon = Anonymizer()

        with pytest.raises(ValueError, match="Unknown detection method"):
            anon.detect_pii({}, method="invalid")

    def test_anonymize_auto_detect(self):
        """Test auto-detection of PII when no config provided."""
        anon = Anonymizer()

        data = {
            "email": "test@example.com",
            "unknown_field": "some value",
        }

        result = anon.anonymize(data)

        # Email should be detected and anonymized
        assert result["email"] != "test@example.com"


class TestAnonymizerStrategies:
    """Test specific anonymization strategies."""

    def test_hash_with_salt(self):
        """Test hashing with salt."""
        anon = Anonymizer()

        data = {"value": "test"}
        config = {
            "value": {"strategy": "hash", "salt": "mysalt", "algorithm": "sha256"},
        }

        result = anon.anonymize(data, config)

        # Should produce consistent hash
        result2 = anon.anonymize(data, config)
        assert result["value"] == result2["value"]

    def test_mask_custom_char(self):
        """Test masking with custom character."""
        anon = Anonymizer()

        data = {"value": "1234567890"}
        config = {
            "value": {"strategy": "mask", "mask_char": "#", "preserve_last": 2},
        }

        result = anon.anonymize(data, config)

        assert result["value"] == "########90"

    def test_unknown_strategy(self):
        """Test that unknown strategy logs warning but doesn't fail."""
        anon = Anonymizer()

        data = {"value": "test"}
        config = {
            "value": {"strategy": "unknown_strategy"},
        }

        # Should not raise error, just keep original value
        result = anon.anonymize(data, config)
        assert result["value"] == "test"
