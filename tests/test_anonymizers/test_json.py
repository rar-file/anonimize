"""Tests for JSON anonymizer."""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from anonimize.anonymizers.json_anon import JSONAnonymizer


class TestJSONAnonymizer:
    """Test cases for JSONAnonymizer."""

    @pytest.fixture
    def sample_json(self):
        """Create a sample JSON file for testing."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test.json"
            data = {
                "users": [
                    {"name": "John Doe", "email": "john@example.com"},
                    {"name": "Jane Smith", "email": "jane@example.com"},
                ]
            }
            with open(json_path, "w") as f:
                json.dump(data, f)
            yield str(json_path)

    def test_init_default(self):
        """Test JSONAnonymizer initialization."""
        anon = JSONAnonymizer()
        assert anon.encoding == "utf-8"
        assert anon.indent == 2

    def test_init_custom(self):
        """Test JSONAnonymizer initialization with custom values."""
        anon = JSONAnonymizer(encoding="utf-16", indent=None)
        assert anon.encoding == "utf-16"
        assert anon.indent is None

    def test_anonymize_basic(self, sample_json):
        """Test basic JSON anonymization."""
        anon = JSONAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json"
            
            config = {
                "users.*.name": {"strategy": "replace", "type": "name"},
                "users.*.email": {"strategy": "mask", "type": "email"},
            }
            
            result = anon.anonymize(sample_json, str(output_path), config)
            
            assert "output_path" in result
            assert output_path.exists()
            
            # Verify output content
            with open(output_path, "r") as f:
                data = json.load(f)
                
                assert data["users"][0]["name"] != "John Doe"
                assert "*" in data["users"][0]["email"]

    def test_anonymize_file_not_found(self):
        """Test that FileNotFoundError is raised for missing input."""
        anon = JSONAnonymizer()
        
        with pytest.raises(FileNotFoundError):
            anon.anonymize("nonexistent.json", "output.json", {})

    def test_anonymize_invalid_config(self, sample_json):
        """Test that ValueError is raised for invalid config."""
        anon = JSONAnonymizer()
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json"
            
            config = {
                "name": {"strategy": "invalid_strategy"},
            }
            
            with pytest.raises(ValueError, match="Invalid configuration"):
                anon.anonymize(sample_json, str(output_path), config)

    def test_anonymize_simple_object(self):
        """Test anonymization of simple JSON object."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "simple.json"
            output_path = Path(tmpdir) / "output.json"
            
            data = {"name": "John Doe", "email": "john@example.com"}
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "name": {"strategy": "replace", "type": "name"},
                "email": {"strategy": "hash", "type": "email"},
            }
            
            anon.anonymize(str(json_path), str(output_path), config)
            
            with open(output_path, "r") as f:
                result = json.load(f)
                
                assert result["name"] != "John Doe"
                assert result["email"] != "john@example.com"

    def test_anonymize_jsonlines(self):
        """Test anonymization of JSON Lines format."""
        with TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "data.jsonl"
            output_path = Path(tmpdir) / "output.jsonl"
            
            with open(jsonl_path, "w") as f:
                f.write(json.dumps({"name": "John", "email": "john@example.com"}) + "\n")
                f.write(json.dumps({"name": "Jane", "email": "jane@example.com"}) + "\n")
            
            anon = JSONAnonymizer()
            config = {
                "name": {"strategy": "replace", "type": "name"},
            }
            
            result = anon.anonymize(
                str(jsonl_path),
                str(output_path),
                config,
                is_jsonlines=True
            )
            
            assert result["records_processed"] == 2
            
            with open(output_path, "r") as f:
                lines = f.readlines()
                assert len(lines) == 2
                data = json.loads(lines[0])
                assert data["name"] != "John"

    def test_detect_fields(self, sample_json):
        """Test field detection."""
        anon = JSONAnonymizer()
        
        detected = anon.detect_fields(sample_json)
        
        assert isinstance(detected, dict)

    def test_preview(self, sample_json):
        """Test preview functionality."""
        anon = JSONAnonymizer()
        
        config = {
            "users.*.name": {"strategy": "replace", "type": "name"},
        }
        
        preview = anon.preview(sample_json, config, num_records=1)
        
        assert len(preview) == 1
        assert preview[0]["users"][0]["name"] != "John Doe"

    def test_preview_list(self):
        """Test preview with list data."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "list.json"
            
            data = [
                {"name": "John", "email": "john@example.com"},
                {"name": "Jane", "email": "jane@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
            ]
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "*.name": {"strategy": "replace", "type": "name"},
            }
            
            preview = anon.preview(str(json_path), config, num_records=2)
            
            assert len(preview) == 2

    def test_nested_structure(self):
        """Test anonymization of nested structures."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "nested.json"
            output_path = Path(tmpdir) / "output.json"
            
            data = {
                "company": {
                    "name": "Acme Corp",
                    "employees": [
                        {"name": "John Doe", "email": "john@acme.com"},
                    ],
                }
            }
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "company.name": {"strategy": "replace", "type": "company"},
                "company.employees.*.name": {"strategy": "replace", "type": "name"},
            }
            
            anon.anonymize(str(json_path), str(output_path), config)
            
            with open(output_path, "r") as f:
                result = json.load(f)
                
                assert result["company"]["name"] != "Acme Corp"
                assert result["company"]["employees"][0]["name"] != "John Doe"


class TestJSONAnonymizerEdgeCases:
    """Test edge cases for JSONAnonymizer."""

    def test_empty_json_object(self):
        """Test handling of empty JSON object."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "empty.json"
            output_path = Path(tmpdir) / "output.json"
            
            with open(json_path, "w") as f:
                json.dump({}, f)
            
            anon = JSONAnonymizer()
            result = anon.anonymize(str(json_path), str(output_path), {})
            
            assert "output_path" in result

    def test_null_values(self):
        """Test handling of null values."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "nulls.json"
            output_path = Path(tmpdir) / "output.json"
            
            data = {"name": None, "email": "test@example.com"}
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "name": {"strategy": "replace", "type": "name"},
                "email": {"strategy": "hash", "type": "email"},
            }
            
            anon.anonymize(str(json_path), str(output_path), config)
            
            with open(output_path, "r") as f:
                result = json.load(f)
                assert result["name"] is None
                assert result["email"] != "test@example.com"

    def test_non_string_values(self):
        """Test handling of non-string values."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "mixed.json"
            output_path = Path(tmpdir) / "output.json"
            
            data = {
                "name": "John",
                "age": 30,
                "active": True,
                "score": 95.5,
            }
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "name": {"strategy": "replace", "type": "name"},
            }
            
            anon.anonymize(str(json_path), str(output_path), config)
            
            with open(output_path, "r") as f:
                result = json.load(f)
                assert result["name"] != "John"
                assert result["age"] == 30
                assert result["active"] is True
                assert result["score"] == 95.5

    def test_deeply_nested(self):
        """Test handling of deeply nested structures."""
        with TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "deep.json"
            output_path = Path(tmpdir) / "output.json"
            
            data = {
                "level1": {
                    "level2": {
                        "level3": {
                            "name": "Deep Name",
                            "email": "deep@example.com",
                        }
                    }
                }
            }
            with open(json_path, "w") as f:
                json.dump(data, f)
            
            anon = JSONAnonymizer()
            config = {
                "level1.level2.level3.name": {"strategy": "replace", "type": "name"},
            }
            
            anon.anonymize(str(json_path), str(output_path), config)
            
            with open(output_path, "r") as f:
                result = json.load(f)
                assert result["level1"]["level2"]["level3"]["name"] != "Deep Name"
