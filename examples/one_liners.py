#!/usr/bin/env python3
"""5 One-Liner Examples for Anonimize

These examples show how to get started with anonimize in just one line of code.
Copy-paste and run!

Usage:
    python examples/one_liners.py
"""

from pathlib import Path
import tempfile
import csv
import json

# Setup: Create sample data
def setup_sample_data():
    """Create sample files for the examples."""
    tmpdir = Path(tempfile.mkdtemp())
    
    # CSV sample
    csv_path = tmpdir / "customers.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "phone", "ssn"])
        writer.writerow(["1", "John Doe", "john@example.com", "555-123-4567", "123-45-6789"])
        writer.writerow(["2", "Jane Smith", "jane@company.org", "555-987-6543", "987-65-4321"])
        writer.writerow(["3", "Bob Johnson", "bob@mail.net", "555-456-7890", "456-78-9012"])
    
    # JSON sample
    json_path = tmpdir / "users.json"
    data = [
        {"id": 1, "name": "Alice Williams", "email": "alice@tech.com", "phone": "555-111-2222"},
        {"id": 2, "name": "Charlie Brown", "email": "charlie@corp.org", "phone": "555-333-4444"},
    ]
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    
    return tmpdir, csv_path, json_path


def example_1_basic_csv():
    """
    EXAMPLE 1: Basic CSV anonymization - Just 3 lines!
    
    The simplest possible way to anonymize a CSV file.
    Auto-detects PII and replaces with realistic fake data.
    """
    print("=" * 60)
    print("EXAMPLE 1: Basic CSV Anonymization (3 lines)")
    print("=" * 60)
    
    tmpdir, csv_path, _ = setup_sample_data()
    output_path = tmpdir / "customers_safe.csv"
    
    # THE ONE-LINER:
    from anonimize import anonymize
    result = anonymize(csv_path, output_path)
    
    print(f"\n✓ Input:  {csv_path}")
    print(f"✓ Output: {result}")
    print("\nCode used:")
    print('  from anonimize import anonymize')
    print(f'  anonymize("{csv_path.name}", "customers_safe.csv")')
    print()


def example_2_in_memory_data():
    """
    EXAMPLE 2: Anonymize in-memory data - No files needed!
    
    Perfect for data pipelines, APIs, or when working with data
    that's already in Python.
    """
    print("=" * 60)
    print("EXAMPLE 2: In-Memory Data Anonymization (1 line)")
    print("=" * 60)
    
    # Your data
    users = [
        {"name": "John Doe", "email": "john@example.com"},
        {"name": "Jane Smith", "email": "jane@company.org"},
    ]
    
    # THE ONE-LINER:
    from anonimize import anonymize_data
    safe_users = anonymize_data(users)
    
    print("\nOriginal data:")
    for u in users:
        print(f"  {u}")
    
    print("\nAnonymized data:")
    for u in safe_users:
        print(f"  {u}")
    
    print("\nCode used:")
    print('  from anonimize import anonymize_data')
    print('  safe_users = anonymize_data(users)')
    print()


def example_3_preview_before_commit():
    """
    EXAMPLE 3: Preview changes before committing
    
    See what will be anonymized without changing anything.
    Great for checking your data before production.
    """
    print("=" * 60)
    print("EXAMPLE 3: Preview Changes (1 line)")
    print("=" * 60)
    
    tmpdir, csv_path, _ = setup_sample_data()
    
    # THE ONE-LINER:
    from anonimize import preview
    preview_data = preview(csv_path, num_rows=2)
    
    print(f"\nPreview of {csv_path.name}:")
    for row in preview_data:
        print(f"  {row}")
    
    print("\nCode used:")
    print('  from anonimize import preview')
    print(f'  preview("{csv_path.name}")')
    print()


def example_4_different_strategies():
    """
    EXAMPLE 4: Use different anonymization strategies
    
    Choose how to anonymize: replace, mask, hash, or remove.
    Each strategy has different use cases.
    """
    print("=" * 60)
    print("EXAMPLE 4: Different Strategies (1 line each)")
    print("=" * 60)
    
    data = {"name": "John Doe", "email": "john@example.com", "ssn": "123-45-6789"}
    
    from anonimize import anonymize_data
    
    print("\nOriginal:")
    print(f"  {data}")
    
    print("\nWith 'replace' (default):")
    print(f"  {anonymize_data(data, strategy='replace')}")
    
    print("\nWith 'mask':")
    print(f"  {anonymize_data(data, strategy='mask')}")
    
    print("\nWith 'hash':")
    print(f"  {anonymize_data(data, strategy='hash')}")
    
    print("\nCode used:")
    print('  anonymize_data(data, strategy="replace")  # Fake data')
    print('  anonymize_data(data, strategy="mask")     # j***@example.com')
    print('  anonymize_data(data, strategy="hash")     # One-way hash')
    print()


def example_5_detect_only():
    """
    EXAMPLE 5: Just detect PII without anonymizing
    
    Find out what PII exists in your data before deciding
    what to anonymize. Useful for data audits.
    """
    print("=" * 60)
    print("EXAMPLE 5: Detect PII Only (1 line)")
    print("=" * 60)
    
    data = {
        "customer_name": "Alice Johnson",
        "email_address": "alice@example.com",
        "phone_number": "555-123-4567",
        "order_total": 150.00,
    }
    
    # THE ONE-LINER:
    from anonimize import detect_pii
    detected = detect_pii(data)
    
    print("\nData analyzed:")
    print(f"  {data}")
    
    print("\nPII detected:")
    for field, info in detected.items():
        pii_type = info.get("type", info) if isinstance(info, dict) else info
        print(f"  - {field}: {pii_type}")
    
    print("\nCode used:")
    print('  from anonimize import detect_pii')
    print('  detected = detect_pii(data)')
    print()


def print_summary():
    """Print a summary of all examples."""
    print("=" * 60)
    print("SUMMARY: 5 Ways to Anonymize in 1-3 Lines")
    print("=" * 60)
    print()
    print("1. Basic file anonymization:")
    print("   from anonimize import anonymize")
    print('   anonymize("input.csv", "output.csv")')
    print()
    print("2. In-memory data:")
    print("   from anonimize import anonymize_data")
    print("   safe_data = anonymize_data(my_data)")
    print()
    print("3. Preview before committing:")
    print("   from anonimize import preview")
    print('   preview("data.csv")')
    print()
    print("4. Different strategies:")
    print("   anonymize_data(data, strategy='mask')")
    print()
    print("5. Detect PII only:")
    print("   from anonimize import detect_pii")
    print("   detected = detect_pii(data)")
    print()
    print("That's it! You're ready to anonymize data. 🎉")
    print()
    print("Next steps:")
    print("  - Try the interactive wizard: anonimize --wizard")
    print("  - Read the tutorial: examples/tutorial.ipynb")
    print("  - See full docs: https://github.com/rar-file/anonimize")


def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "   ANONIMIZE - 5 ONE-LINER EXAMPLES".center(58) + "║")
    print("║" + "   Get productive in 5 minutes".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    example_1_basic_csv()
    example_2_in_memory_data()
    example_3_preview_before_commit()
    example_4_different_strategies()
    example_5_detect_only()
    print_summary()


if __name__ == "__main__":
    main()
