#!/usr/bin/env python3
"""Example: Anonymize a CSV file with PII data.

This example demonstrates how to use the CSVAnonymizer to anonymize
personally identifiable information in a CSV file while preserving
data structure and relationships.
"""

import csv
import os
import tempfile
from pathlib import Path

from anonimize.anonymizers.csv_anon import CSVAnonymizer


def create_sample_csv(filepath: str) -> None:
    """Create a sample CSV file with PII data.
    
    Args:
        filepath: Path where to create the sample CSV file.
    """
    data = [
        ["id", "name", "email", "phone", "ssn", "city", "company"],
        ["1", "John Doe", "john.doe@example.com", "555-123-4567", "123-45-6789", "New York", "Acme Corp"],
        ["2", "Jane Smith", "jane.smith@company.org", "555-987-6543", "987-65-4321", "Los Angeles", "Tech Solutions"],
        ["3", "Bob Johnson", "bob.j@test.net", "555-456-7890", "456-78-9012", "Chicago", "Data Systems"],
        ["4", "Alice Williams", "alice.w@enterprise.com", "555-234-5678", "234-56-7890", "Houston", "Cloud Services"],
        ["5", "Charlie Brown", "charlie.brown@mail.com", "555-876-5432", "876-54-3210", "Phoenix", "Innovation Labs"],
    ]
    
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    print(f"Created sample CSV: {filepath}")


def main():
    """Run the CSV anonymization example."""
    # Create a temporary directory for our files
    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = Path(tmpdir) / "customers.csv"
        output_file = Path(tmpdir) / "customers_anonymized.csv"
        
        # Create sample data
        create_sample_csv(str(input_file))
        
        # Display original data
        print("\n" + "=" * 60)
        print("ORIGINAL DATA:")
        print("=" * 60)
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                print(f"  {row['name']:<20} | {row['email']:<25} | {row['phone']:<15} | {row['ssn']}")
        
        # Configure anonymization
        # Strategies: replace, hash, mask, remove
        config = {
            # Replace with realistic fake data
            "name": {"strategy": "replace", "type": "name"},
            "city": {"strategy": "replace", "type": "city"},
            "company": {"strategy": "replace", "type": "company"},
            
            # Mask sensitive data (show only last 4 digits)
            "phone": {"strategy": "mask", "type": "phone", "preserve_last": 4, "mask_char": "*"},
            "ssn": {"strategy": "mask", "type": "ssn", "preserve_last": 4, "mask_char": "X"},
            
            # Hash email (one-way transformation)
            "email": {"strategy": "hash", "type": "email", "algorithm": "sha256"},
        }
        
        # Create anonymizer and process
        print("\n" + "=" * 60)
        print("ANONYMIZING...")
        print("=" * 60)
        
        anonymizer = CSVAnonymizer()
        
        # Preview the anonymization (first 3 rows)
        print("\nPreview (first 3 rows):")
        preview = anonymizer.preview(str(input_file), config, num_rows=3)
        for row in preview:
            print(f"  {row['name']:<20} | {row['email'][:25]:<25} | {row['phone']:<15} | {row['ssn']}")
        
        # Anonymize the file
        stats = anonymizer.anonymize(str(input_file), str(output_file), config)
        
        print(f"\nAnonymization complete!")
        print(f"  Records processed: {stats['records_processed']}")
        print(f"  Fields anonymized: {stats['fields_anonymized']}")
        print(f"  Output file: {stats['output_path']}")
        
        # Display anonymized data
        print("\n" + "=" * 60)
        print("ANONYMIZED DATA:")
        print("=" * 60)
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                print(f"  {row['name']:<20} | {row['email'][:25]:<25} | {row['phone']:<15} | {row['ssn']}")
        
        # Show column detection example
        print("\n" + "=" * 60)
        print("AUTO-DETECTED PII COLUMNS:")
        print("=" * 60)
        detected = anonymizer.detect_columns(str(input_file), sample_size=100)
        for col, pii_type in detected.items():
            print(f"  {col:<15} -> {pii_type}")
        
        # Example with custom column mapping
        print("\n" + "=" * 60)
        print("COLUMN MAPPING EXAMPLE:")
        print("=" * 60)
        
        # If your CSV has different column names than your config
        custom_output = Path(tmpdir) / "customers_mapped.csv"
        
        # Config uses "customer_name" but CSV has "name"
        mapping_config = {
            "customer_name": {"strategy": "replace", "type": "name"},
            "customer_email": {"strategy": "mask", "type": "email"},
        }
        
        column_mapping = {
            "name": "customer_name",
            "email": "customer_email",
        }
        
        anonymizer.anonymize(
            str(input_file),
            str(custom_output),
            mapping_config,
            column_mapping=column_mapping
        )
        
        print(f"Anonymized with column mapping -> {custom_output}")
        with open(custom_output, "r") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            print(f"  Original: name={row['name']}, email={row['email']}")


if __name__ == "__main__":
    main()
