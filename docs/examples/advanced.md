# Advanced Examples

## Example 16: Multi-Table Database Anonymization

```python
from anonimize.anonymizers.database import DatabaseAnonymizer

anon = DatabaseAnonymizer("postgresql://user:pass@localhost/db")

# Anonymize multiple related tables
config = {
    "users": {
        "email": {"strategy": "replace", "type": "email"},
        "phone": {"strategy": "mask", "type": "phone"},
    },
    "orders": {
        "customer_email": {"strategy": "replace", "type": "email"},
        "shipping_address": {"strategy": "replace", "type": "address"},
    },
    "payments": {
        "card_number": {"strategy": "mask", "mask_char": "X", "preserve_last": 4},
        "card_holder": {"strategy": "replace", "type": "name"},
    }
}

anon.anonymize(config=config)
```

## Example 17: Conditional Anonymization

```python
from anonimize import Anonymizer
import re

class ConditionalAnonymizer(Anonymizer):
    def anonymize_conditional(self, data, config, condition_field, condition_value):
        """Only anonymize if condition is met."""
        if data.get(condition_field) == condition_value:
            return self.anonymize(data, config)
        return data

anon = ConditionalAnonymizer()

data = {
    "role": "external",
    "name": "John Doe",
    "email": "john@example.com"
}

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

# Only anonymize if role is 'external'
result = anon.anonymize_conditional(data, config, "role", "external")
```

## Example 18: Custom Hash with Salt

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {"user_id": "12345", "ssn": "123-45-6789"}

# Use unique salt per user for additional security
config = {
    "user_id": {"strategy": "hash", "algorithm": "sha256", "salt": "app_salt_2024"},
    "ssn": {
        "strategy": "hash",
        "algorithm": "sha512",
        "salt": "sensitive_data_salt"
    },
}

result = anon.anonymize(data, config)
print(result)
```

## Example 19: Large CSV Processing with Chunks

```python
from anonimize.anonymizers.csv_anon import CSVAnonymizer
import pandas as pd

# Process large CSV in chunks to minimize memory usage
def process_large_csv(input_path, output_path, chunk_size=10000):
    anon = CSVAnonymizer()
    
    config = {
        "email": {"strategy": "replace", "type": "email"},
        "name": {"strategy": "replace", "type": "name"},
    }
    
    # Read and process in chunks
    chunk_iterator = pd.read_csv(input_path, chunksize=chunk_size)
    
    first_chunk = True
    for chunk in chunk_iterator:
        # Convert to records
        records = chunk.to_dict('records')
        
        # Anonymize
        anonymized = anon.anonymize(records, config)
        
        # Write to output
        df = pd.DataFrame(anonymized)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        df.to_csv(output_path, mode=mode, header=header, index=False)
        
        first_chunk = False
        print(f"Processed {len(records)} records")

process_large_csv("large_input.csv", "anonymized_output.csv")
```

## Example 20: Nested JSON Anonymization

```python
from anonimize.anonymizers.json_anon import JSONAnonimizer
import json

anon = JSONAnonimizer()

# Handle deeply nested structures
config = {
    # Root level
    "company.name": {"strategy": "replace", "type": "company"},
    
    # Nested employees
    "employees.*.name": {"strategy": "replace", "type": "name"},
    "employees.*.email": {"strategy": "replace", "type": "email"},
    "employees.*.phone": {"strategy": "mask", "type": "phone"},
    
    # Nested address
    "employees.*.address.street": {"strategy": "replace", "type": "address"},
    "employees.*.address.city": {"strategy": "replace", "type": "city"},
    
    # Deep nesting - emergency contacts
    "employees.*.emergency_contact.name": {"strategy": "replace", "type": "name"},
    "employees.*.emergency_contact.phone": {"strategy": "mask", "type": "phone"},
}

with open("nested_data.json") as f:
    data = json.load(f)

anonymized = anon.anonymize(data, config)

with open("anonymized_output.json", "w") as f:
    json.dump(anonymized, f, indent=2)
```

## Example 21: Audit Trail Logging

```python
from anonimize import Anonymizer
import logging
import json
from datetime import datetime

# Set up audit logging
logging.basicConfig(
    filename='anonymization_audit.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

class AuditedAnonymizer(Anonymizer):
    def anonymize(self, data, config=None):
        start_time = datetime.now()
        
        # Perform anonymization
        result = super().anonymize(data, config)
        
        # Log audit entry
        audit_entry = {
            "timestamp": start_time.isoformat(),
            "duration_ms": (datetime.now() - start_time).total_seconds() * 1000,
            "records_processed": len(data) if isinstance(data, list) else 1,
            "fields_configured": list(config.keys()) if config else [],
            "cache_size": len(self._value_cache),
        }
        
        logging.info(json.dumps(audit_entry))
        
        return result

anon = AuditedAnonymizer()

# Usage
config = {"name": {"strategy": "replace", "type": "name"}}
result = anon.anonymize({"name": "John Doe"}, config)
```

## Example 22: PII Detection Pipeline

```python
from anonimize import Anonymizer
from anonimize.detectors.regex import RegexDetector
from anonimize.detectors.heuristic import HeuristicDetector

# Create detection pipeline
anon = Anonymizer()
regex_detector = RegexDetector()
heuristic_detector = HeuristicDetector()

# Sample data to analyze
sample_data = {
    "full_name": "John Doe",
    "contact_email": "john@example.com",
    "phone_num": "+1-555-123-4567",
    "ssn_id": "123-45-6789",
    "user_id": "user_12345",  # Not PII
}

# Multi-stage detection
print("=== Regex Detection ===")
regex_results = regex_detector.detect(sample_data)
print(regex_results)

print("\n=== Heuristic Detection ===")
heuristic_results = heuristic_detector.detect(sample_data)
print(heuristic_results)

print("\n=== Combined Detection ===")
combined = anon.detect_pii(sample_data, method="combined")
print(combined)

# Auto-anonymize detected fields
auto_config = {
    field: {"strategy": "replace", "type": info.get("type", "string")}
    for field, info in combined.items()
}

result = anon.anonymize(sample_data, auto_config)
print("\n=== Anonymized Result ===")
print(result)
```

## Example 23: Database Migration with Anonymization

```python
from anonimize.anonymizers.database import DatabaseAnonymizer
from sqlalchemy import create_engine, text

def migrate_and_anonymize(source_url, target_url, anonymization_config):
    """Migrate data from source to target with anonymization."""
    
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    
    # Create target schema
    with target_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                phone TEXT
            )
        """))
        conn.commit()
    
    # Read from source
    with source_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM users"))
        rows = [dict(row._mapping) for row in result]
    
    print(f"Read {len(rows)} rows from source")
    
    # Anonymize
    anon = DatabaseAnonymizer(target_url)
    anonymized = anon.anonymize(rows, anonymization_config)
    
    # Write to target
    with target_engine.connect() as conn:
        for row in anonymized:
            conn.execute(
                text("INSERT INTO users (id, name, email, phone) VALUES (:id, :name, :email, :phone)"),
                row
            )
        conn.commit()
    
    print(f"Migrated and anonymized {len(anonymized)} rows")

# Usage
config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
    "phone": {"strategy": "mask", "type": "phone"},
}

migrate_and_anonymize(
    "postgresql://user:pass@prod/db",
    "postgresql://user:pass@staging/db",
    config
)
```

## Example 24: GDPR Right to Erasure Implementation

```python
from anonimize import Anonymizer

class GDPRAnonymizer(Anonymizer):
    """Specialized anonymizer for GDPR right to erasure."""
    
    def right_to_erasure(self, data, user_id_field, user_id):
        """
        Anonymize all data for a specific user.
        Implements GDPR Article 17 - Right to erasure.
        """
        gdpr_config = {
            # Irreversible anonymization for all PII
            "name": {"strategy": "hash", "algorithm": "sha256", "salt": str(user_id)},
            "email": {"strategy": "hash", "algorithm": "sha256", "salt": str(user_id)},
            "phone": {"strategy": "hash", "algorithm": "sha256", "salt": str(user_id)},
            "address": {"strategy": "hash", "algorithm": "sha256", "salt": str(user_id)},
            "ip_address": {"strategy": "hash", "algorithm": "sha256", "salt": str(user_id)},
            # Remove sensitive data entirely
            "ssn": {"strategy": "remove"},
            "credit_card": {"strategy": "remove"},
        }
        
        # Filter for specific user
        user_records = [
            record for record in data
            if record.get(user_id_field) == user_id
        ]
        
        if not user_records:
            return data
        
        # Anonymize user's records
        anonymized = self.anonymize(user_records, gdpr_config)
        
        # Merge back with other records
        result = []
        for record in data:
            if record.get(user_id_field) == user_id:
                result.append(anonymized.pop(0))
            else:
                result.append(record)
        
        return result

# Usage
gdpr = GDPRAnonymizer()

data = [
    {"user_id": 1, "name": "Alice", "email": "alice@example.com"},
    {"user_id": 2, "name": "Bob", "email": "bob@example.com"},
    {"user_id": 1, "name": "Alice", "email": "alice@work.com"},
]

# Erase all data for user_id=1
anonymized = gdpr.right_to_erasure(data, "user_id", 1)
print(anonymized)
```
