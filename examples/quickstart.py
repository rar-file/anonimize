#!/usr/bin/env python3
"""
Anonimize Quick Start Example

This example demonstrates the most common use cases for anonimize.
Run this script to see anonimize in action!
"""

from anonimize import Anonymizer


def example_basic_usage():
    """Example 1: Basic anonymization with auto-detection."""
    print("\n" + "="*60)
    print("Example 1: Basic Usage")
    print("="*60)
    
    # Create anonymizer with seed for reproducibility
    anon = Anonymizer(seed=42)
    
    # Sample data with PII
    data = {
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "+1-555-123-4567",
        "ssn": "123-45-6789",
        "address": "123 Main St, New York, NY 10001",
        "salary": 85000
    }
    
    print("\nOriginal data:")
    for key, value in data.items():
        print(f"  {key}: {value}")
    
    # Auto-detect and anonymize
    result = anon.anonymize(data)
    
    print("\nAnonymized data:")
    for key, value in result.items():
        print(f"  {key}: {value}")


def example_strategies():
    """Example 2: Different anonymization strategies."""
    print("\n" + "="*60)
    print("Example 2: Anonymization Strategies")
    print("="*60)
    
    anon = Anonymizer()
    data = {
        "name": "Jane Smith",
        "email": "jane@company.com",
        "ssn": "987-65-4321",
        "notes": "Customer prefers email contact"
    }
    
    strategies = {
        "name": {"strategy": "replace", "type": "name"},
        "email": {"strategy": "mask"},  # j***@company.com
        "ssn": {"strategy": "hash"},     # One-way hash
        "notes": {"strategy": "remove"}  # Deleted
    }
    
    print("\nOriginal:", data)
    result = anon.anonymize(data, strategies)
    print("\nWith strategies:")
    print(f"  name (replace):  {result['name']}")
    print(f"  email (mask):    {result['email']}")
    print(f"  ssn (hash):      {result['ssn']}")
    print(f"  notes (remove):  {result['notes']}")


def example_relationship_preservation():
    """Example 3: Relationship preservation."""
    print("\n" + "="*60)
    print("Example 3: Relationship Preservation")
    print("="*60)
    
    # Same seed ensures same input → same output
    anon = Anonymizer(seed=123)
    
    # Same person appears multiple times
    records = [
        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com"},
        {"id": 2, "name": "Bob Smith", "email": "bob@test.com"},
        {"id": 3, "name": "Alice Johnson", "email": "alice@example.com"},  # Same person!
    ]
    
    print("\nOriginal records:")
    for r in records:
        print(f"  ID {r['id']}: {r['name']}, {r['email']}")
    
    results = anon.anonymize(records)
    
    print("\nAnonymized records:")
    for r in results:
        print(f"  ID {r['id']}: {r['name']}, {r['email']}")
    
    print("\n✓ Notice: Alice appears twice with the SAME anonymized values!")
    print("  This preserves relationships across your dataset.")


def example_csv_workflow():
    """Example 4: Working with CSV files."""
    print("\n" + "="*60)
    print("Example 4: CSV File Workflow")
    print("="*60)
    
    import tempfile
    import os
    from anonimize.anonymizers.csv_anon import CSVAnonymizer
    
    # Create a sample CSV
    csv_content = """name,email,phone,department
John Doe,john@company.com,555-0101,Engineering
Jane Smith,jane@company.com,555-0102,Marketing
Bob Johnson,bob@company.com,555-0103,Sales
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name
    
    try:
        print(f"\nSample CSV created at: {temp_path}")
        print("\nOriginal content:")
        print(csv_content)
        
        # Anonymize the CSV
        anon = CSVAnonymizer(seed=42)
        output_path = temp_path.replace('.csv', '_anonymized.csv')
        
        anon.anonymize(
            input_path=temp_path,
            output_path=output_path,
            strategy="replace"
        )
        
        print(f"\nAnonymized CSV saved to: {output_path}")
        with open(output_path, 'r') as f:
            print("\nAnonymized content:")
            print(f.read())
        
        os.remove(output_path)
    finally:
        os.remove(temp_path)


def example_detection():
    """Example 5: PII Detection."""
    print("\n" + "="*60)
    print("Example 5: PII Detection")
    print("="*60)
    
    anon = Anonymizer()
    
    data = {
        "customer_name": "Alice Wonder",
        "contact_email": "alice@wonderland.com",
        "phone_number": "+44-20-7946-0958",
        "social_security": "555-55-5555",
        "credit_card": "4111-1111-1111-1111",
        "order_id": "ORD-12345-XYZ",
        "total": 199.99
    }
    
    print("\nData to analyze:")
    for key, value in data.items():
        print(f"  {key}: {value}")
    
    detected = anon.detect_pii(data)
    
    print("\nDetected PII:")
    for field, info in detected.items():
        pii_type = info.get('type', 'unknown') if isinstance(info, dict) else info
        print(f"  ✓ {field}: {pii_type}")


if __name__ == "__main__":
    print("\n" + "🛡️ " * 15)
    print("  ANONIMIZE - Quick Start Examples")
    print("🛡️ " * 15)
    
    try:
        example_basic_usage()
        example_strategies()
        example_relationship_preservation()
        example_csv_workflow()
        example_detection()
        
        print("\n" + "="*60)
        print("✓ All examples completed successfully!")
        print("="*60)
        print("\nNext steps:")
        print("  1. Try with your own data: anonimize data.csv")
        print("  2. Run the wizard: anonimize --wizard")
        print("  3. Read the docs: https://github.com/rar-file/anonimize")
        print()
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
