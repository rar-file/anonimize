"""Tests for CSV anonymizer."""

import csv
import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from anonimize.anonymizers.csv_anon import CSVAnonymizer


class TestCSVAnonymizer:
    """Test cases for CSVAnonymizer."""

    @pytest.fixture
    def sample_csv(self):
        """Create a sample CSV file for testing."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["name", "email", "phone", "age"])
                writer.writerow(["John Doe", "john@example.com", "123-456-7890", "30"])
                writer.writerow(["Jane Smith", "jane@example.com", "098-765-4321", "25"])
            yield str(csv_path)

    def test_init_default(self):
        """Test CSVAnonymizer initialization."""
        anon = CSVAnonymizer()
        assert anon.chunk_size == 10000
        assert anon.encoding == "utf-8"
        assert anon.delimiter == ","

    def test_init_custom(self):
        """Test CSVAnonymizer initialization with custom values."""
        anon = CSVAnonymizer(chunk_size=500, encoding="utf-16", delimiter=";")
        assert anon.chunk_size == 500
        assert anon.encoding == "utf-16"
        assert anon.delimiter == ";"

    def test_anonymize_basic(self, sample_csv):
        """Test basic CSV anonymization."""
        anon = CSVAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            config = {
                "name": {"strategy": "replace", "type": "name"},
                "email": {"strategy": "mask", "type": "email"},
            }
            
            result = anon.anonymize(sample_csv, str(output_path), config)
            
            assert result["records_processed"] == 2
            assert result["fields_anonymized"] > 0
            assert output_path.exists()
            
            # Verify output content
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 2
                assert rows[0]["name"] != "John Doe"
                assert "*" in rows[0]["email"]
                assert rows[0]["age"] == "30"  # Not anonymized

    def test_anonymize_file_not_found(self):
        """Test that FileNotFoundError is raised for missing input."""
        anon = CSVAnonymizer()
        
        with pytest.raises(FileNotFoundError):
            anon.anonymize("nonexistent.csv", "output.csv", {})

    def test_anonymize_invalid_config(self, sample_csv):
        """Test that ValueError is raised for invalid config."""
        anon = CSVAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            config = {
                "name": {"strategy": "invalid_strategy"},
            }
            
            with pytest.raises(ValueError, match="Invalid configuration"):
                anon.anonymize(sample_csv, str(output_path), config)

    def test_validate_config_valid(self):
        """Test validation of valid configuration."""
        anon = CSVAnonymizer()
        
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "hash", "type": "email"},
        }
        
        errors = anon.validate_config(config)
        assert len(errors) == 0

    def test_validate_config_invalid_strategy(self):
        """Test validation detects invalid strategy."""
        anon = CSVAnonymizer()
        
        config = {
            "name": {"strategy": "invalid"},
        }
        
        errors = anon.validate_config(config)
        assert len(errors) == 1
        assert "invalid strategy" in errors[0].lower()

    def test_validate_config_missing_strategy(self):
        """Test validation detects missing strategy."""
        anon = CSVAnonymizer()
        
        config = {
            "name": {"type": "name"},
        }
        
        errors = anon.validate_config(config)
        assert len(errors) == 1
        assert "strategy" in errors[0].lower()

    def test_detect_columns(self, sample_csv):
        """Test column detection."""
        anon = CSVAnonymizer()
        
        detected = anon.detect_columns(sample_csv)
        
        assert isinstance(detected, dict)
        # Email should be detected
        assert "email" in detected or "phone" in detected

    def test_preview(self, sample_csv):
        """Test preview functionality."""
        anon = CSVAnonymizer()
        
        config = {
            "name": {"strategy": "replace", "type": "name"},
        }
        
        preview = anon.preview(sample_csv, config, num_rows=2)
        
        assert len(preview) == 2
        assert preview[0]["name"] != "John Doe"

    def test_column_mapping(self, sample_csv):
        """Test column mapping functionality."""
        anon = CSVAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            config = {
                "customer_name": {"strategy": "replace", "type": "name"},
            }
            
            column_mapping = {
                "name": "customer_name",
            }
            
            result = anon.anonymize(
                sample_csv,
                str(output_path),
                config,
                column_mapping=column_mapping
            )
            
            assert result["fields_anonymized"] > 0
            
            # Verify output
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert rows[0]["name"] != "John Doe"

    def test_get_stats(self, sample_csv):
        """Test getting statistics."""
        anon = CSVAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            config = {
                "name": {"strategy": "replace", "type": "name"},
            }
            
            anon.anonymize(sample_csv, str(output_path), config)
            stats = anon.get_stats()
            
            assert stats["records_processed"] == 2
            assert stats["fields_anonymized"] > 0

    def test_reset_stats(self, sample_csv):
        """Test resetting statistics."""
        anon = CSVAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            config = {"name": {"strategy": "replace", "type": "name"}}
            
            anon.anonymize(sample_csv, str(output_path), config)
            assert anon.get_stats()["records_processed"] == 2
            
            anon.reset_stats()
            assert anon.get_stats()["records_processed"] == 0


class TestCSVAnonymizerEdgeCases:
    """Test edge cases for CSVAnonymizer."""

    def test_empty_csv(self):
        """Test handling of empty CSV (headers only)."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "empty.csv"
            output_path = Path(tmpdir) / "output.csv"
            
            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["name", "email"])
            
            anon = CSVAnonymizer()
            result = anon.anonymize(str(csv_path), str(output_path), {})
            
            assert result["records_processed"] == 0

    def test_csv_with_empty_values(self):
        """Test handling of CSV with empty values."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "partial.csv"
            output_path = Path(tmpdir) / "output.csv"
            
            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["name", "email"])
                writer.writerow(["John", ""])
                writer.writerow(["", "jane@example.com"])
            
            anon = CSVAnonymizer()
            config = {
                "name": {"strategy": "replace", "type": "name"},
                "email": {"strategy": "replace", "type": "email"},
            }
            
            result = anon.anonymize(str(csv_path), str(output_path), config)
            
            assert result["records_processed"] == 2
            
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert rows[0]["name"] != "John"
                assert rows[0]["email"] == ""  # Empty preserved

    def test_semicolon_delimiter(self):
        """Test handling of semicolon-delimited CSV."""
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "semicolon.csv"
            output_path = Path(tmpdir) / "output.csv"
            
            with open(csv_path, "w", newline="") as f:
                f.write("name;email\n")
                f.write("John;john@example.com\n")
            
            anon = CSVAnonymizer(delimiter=";")
            config = {
                "name": {"strategy": "replace", "type": "name"},
            }
            
            result = anon.anonymize(str(csv_path), str(output_path), config)
            
            assert result["records_processed"] == 1
