"""Performance benchmarks for anonimize."""

import pytest
from anonimize import Anonymizer
from anonimize.anonymizers.csv_anon import CSVAnonymizer
from anonimize.anonymizers.json_anon import JSONAnonymizer


class TestAnonymizerBenchmarks:
    """Benchmarks for core Anonymizer class."""

    def test_replace_strategy_single_record(self, benchmark):
        """Benchmark replace strategy on single record."""
        anon = Anonymizer()
        data = {"name": "John Doe", "email": "john@example.com"}
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        result = benchmark(anon.anonymize, data, config)
        assert result["name"] != "John Doe"

    def test_replace_strategy_small_batch(self, benchmark):
        """Benchmark replace strategy on 100 records."""
        anon = Anonymizer()
        data = [
            {"name": f"User{i}", "email": f"user{i}@test.com"}
            for i in range(100)
        ]
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        result = benchmark(anon.anonymize, data, config)
        assert len(result) == 100

    def test_replace_strategy_large_batch(self, benchmark):
        """Benchmark replace strategy on 1000 records."""
        anon = Anonymizer()
        data = [
            {"name": f"User{i}", "email": f"user{i}@test.com"}
            for i in range(1000)
        ]
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        result = benchmark(anon.anonymize, data, config)
        assert len(result) == 1000

    def test_hash_strategy(self, benchmark):
        """Benchmark hash strategy."""
        anon = Anonymizer()
        data = {"ssn": "123-45-6789"}
        config = {"ssn": {"strategy": "hash"}}
        
        result = benchmark(anon.anonymize, data, config)
        assert result["ssn"] != "123-45-6789"

    def test_mask_strategy(self, benchmark):
        """Benchmark mask strategy."""
        anon = Anonymizer()
        data = {"card": "1234-5678-9012-3456"}
        config = {"card": {"strategy": "mask", "preserve_last": 4}}
        
        result = benchmark(anon.anonymize, data, config)
        assert result["card"].endswith("3456")

    def test_remove_strategy(self, benchmark):
        """Benchmark remove strategy."""
        anon = Anonymizer()
        data = {"password": "secret123"}
        config = {"password": {"strategy": "remove"}}
        
        result = benchmark(anon.anonymize, data, config)
        assert result["password"] is None

    def test_relationship_preservation(self, benchmark):
        """Benchmark with relationship preservation enabled."""
        anon = Anonymizer(preserve_relationships=True)
        data = [
            {"name": "John", "referrer": "John"}
            for _ in range(100)
        ]
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "referrer": {"strategy": "replace", "type": "name"},
        }
        
        result = benchmark(anon.anonymize, data, config)
        # All "John" values should be replaced with same fake name
        fake_name = result[0]["name"]
        assert all(r["name"] == fake_name for r in result)

    def test_auto_detection(self, benchmark):
        """Benchmark PII auto-detection."""
        anon = Anonymizer()
        data = {
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "+1-555-123-4567",
        }
        
        result = benchmark(anon.detect_pii, data)
        assert "email" in result


class TestCSVAnonymizerBenchmarks:
    """Benchmarks for CSV anonymizer."""

    def test_csv_small_file(self, benchmark, tmp_path):
        """Benchmark CSV anonymization on small file."""
        # Create test file
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "name,email\n" +
            "\n".join([f"User{i},user{i}@test.com" for i in range(100)])
        )
        
        anon = CSVAnonymizer()
        output_file = tmp_path / "output.csv"
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        def anonymize_csv():
            anon.anonymize(str(input_file), str(output_file), config)
        
        benchmark(anonymize_csv)
        assert output_file.exists()

    def test_csv_medium_file(self, benchmark, tmp_path):
        """Benchmark CSV anonymization on medium file (1000 rows)."""
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "name,email\n" +
            "\n".join([f"User{i},user{i}@test.com" for i in range(1000)])
        )
        
        anon = CSVAnonymizer()
        output_file = tmp_path / "output.csv"
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        def anonymize_csv():
            anon.anonymize(str(input_file), str(output_file), config)
        
        benchmark(anonymize_csv)


class TestJSONAnonymizerBenchmarks:
    """Benchmarks for JSON anonymizer."""

    def test_json_simple(self, benchmark, tmp_path):
        """Benchmark JSON anonymization (simple structure)."""
        input_file = tmp_path / "test.json"
        input_file.write_text(
            '{"name": "John", "email": "john@example.com"}'
        )
        
        anon = JSONAnonymizer()
        output_file = tmp_path / "output.json"
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
        }
        
        def anonymize_json():
            anon.anonymize(str(input_file), str(output_file), config)
        
        benchmark(anonymize_json)
        assert output_file.exists()

    def test_json_nested(self, benchmark, tmp_path):
        """Benchmark JSON anonymization (nested structure)."""
        import json
        
        # Create nested data
        data = {
            "users": [
                {
                    "name": f"User{i}",
                    "email": f"user{i}@test.com",
                    "address": {"city": f"City{i}"}
                }
                for i in range(100)
            ]
        }
        
        input_file = tmp_path / "test.json"
        input_file.write_text(json.dumps(data))
        
        anon = JSONAnonymizer()
        output_file = tmp_path / "output.json"
        config = {
            "users.*.name": {"strategy": "replace", "type": "name"},
            "users.*.email": {"strategy": "replace", "type": "email"},
        }
        
        def anonymize_json():
            anon.anonymize(str(input_file), str(output_file), config)
        
        benchmark(anonymize_json)


class TestHashAlgorithmsBenchmarks:
    """Benchmarks for different hash algorithms."""

    @pytest.mark.parametrize("algorithm", ["sha256", "sha512", "md5"])
    def test_hash_algorithms(self, benchmark, algorithm):
        """Benchmark different hash algorithms."""
        anon = Anonymizer()
        data = {"value": "test-data-to-hash"}
        config = {"value": {"strategy": "hash", "algorithm": algorithm}}
        
        result = benchmark(anon.anonymize, data, config)
        assert result["value"] != "test-data-to-hash"


class TestMemoryBenchmarks:
    """Memory usage benchmarks."""

    def test_memory_with_large_dataset(self):
        """Test memory efficiency with large dataset."""
        import tracemalloc
        
        tracemalloc.start()
        
        anon = Anonymizer()
        data = [
            {
                "name": f"User{i}",
                "email": f"user{i}@example.com",
                "phone": f"+1-555-{i:04d}",
            }
            for i in range(10000)
        ]
        
        config = {
            "name": {"strategy": "replace", "type": "name"},
            "email": {"strategy": "replace", "type": "email"},
            "phone": {"strategy": "mask", "type": "phone"},
        }
        
        # Take memory snapshot before
        snapshot1 = tracemalloc.take_snapshot()
        
        # Anonymize
        result = anon.anonymize(data, config)
        
        # Take memory snapshot after
        snapshot2 = tracemalloc.take_snapshot()
        
        # Calculate memory usage
        stats = snapshot2.compare_to(snapshot1, 'lineno')
        total_memory = sum(stat.size_diff for stat in stats if stat.size_diff > 0)
        
        # Should use less than 50MB for 10k records
        assert total_memory < 50 * 1024 * 1024
        
        tracemalloc.stop()
