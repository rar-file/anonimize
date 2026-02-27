# Basic Examples

## Example 1: Simple Data Anonymization

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {
    "name": "John Doe",
    "email": "john@example.com"
}

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

result = anon.anonymize(data, config)
print(result)
```

## Example 2: Hash Strategy

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {"ssn": "123-45-6789"}
config = {
    "ssn": {"strategy": "hash", "algorithm": "sha256"}
}

result = anon.anonymize(data, config)
print(result)  # {'ssn': 'a3f5c2...'}
```

## Example 3: Mask Strategy

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {"credit_card": "4532-1234-5678-9012"}
config = {
    "credit_card": {
        "strategy": "mask",
        "mask_char": "*",
        "preserve_last": 4
    }
}

result = anon.anonymize(data, config)
print(result)  # {'credit_card': '************9012'}
```

## Example 4: Remove Strategy

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {"name": "John", "password": "secret123"}
config = {
    "password": {"strategy": "remove"}
}

result = anon.anonymize(data, config)
print(result)  # {'name': 'John', 'password': None}
```

## Example 5: Batch Processing List

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Charlie", "email": "charlie@example.com"},
]

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

results = anon.anonymize(data, config)
for result in results:
    print(result)
```

## Example 6: Auto-Detect PII

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = {
    "name": "John Doe",
    "email": "john@example.com",
    "phone": "+1-555-123-4567",
    "address": "123 Main St"
}

# Auto-detect and anonymize
result = anon.anonymize(data)
print(result)
```

## Example 7: CSV File Anonymization

```python
from anonimize.anonymizers.csv_anon import CSVAnonymizer

anon = CSVAnonymizer()

config = {
    "first_name": {"strategy": "replace", "type": "first_name"},
    "last_name": {"strategy": "replace", "type": "last_name"},
    "email": {"strategy": "replace", "type": "email"},
}

anon.anonymize(
    input_path="customers.csv",
    output_path="customers_anonymized.csv",
    config=config
)
```

## Example 8: JSON File Anonymization

```python
from anonimize.anonymizers.json_anon import JSONAnonymizer

anon = JSONAnonymizer()

config = {
    "users.*.name": {"strategy": "replace", "type": "name"},
    "users.*.email": {"strategy": "replace", "type": "email"},
}

anon.anonymize(
    input_path="data.json",
    output_path="data_anonymized.json",
    config=config
)
```

## Example 9: SQLite Database Anonymization

```python
from anonimize.anonymizers.database import DatabaseAnonymizer

anon = DatabaseAnonymizer("sqlite:///mydb.db")

config = {
    "customers": {
        "name": {"strategy": "replace", "type": "name"},
        "email": {"strategy": "replace", "type": "email"},
    }
}

anon.anonymize(config=config)
```

## Example 10: PostgreSQL Anonymization

```python
from anonimize.anonymizers.database import DatabaseAnonymizer

anon = DatabaseAnonymizer("postgresql://user:pass@localhost/db")

config = {
    "users": {
        "full_name": {"strategy": "replace", "type": "name"},
        "email_address": {"strategy": "replace", "type": "email"},
        "phone": {"strategy": "mask", "type": "phone"},
    }
}

anon.anonymize(config=config)
```

## Example 11: Preserve Relationships

```python
from anonimize import Anonymizer

# Enable relationship preservation
anon = Anonymizer(preserve_relationships=True)

data = [
    {"user_id": 1, "name": "John", "referrer_name": "John"},
    {"user_id": 2, "name": "Jane", "referrer_name": "John"},
]

config = {
    "name": {"strategy": "replace", "type": "name"},
    "referrer_name": {"strategy": "replace", "type": "name"},
}

results = anon.anonymize(data, config)
# Both "John" values will be replaced with the same fake name
print(results)
```

## Example 12: Reproducible Results with Seed

```python
from anonimize import Anonymizer

# Use seed for reproducible results
anon = Anonymizer(seed=42)

data = {"name": "John Doe"}
config = {"name": {"strategy": "replace", "type": "name"}}

result1 = anon.anonymize(data, config)

# Create new instance with same seed
anon2 = Anonymizer(seed=42)
result2 = anon2.anonymize(data, config)

assert result1 == result2  # Same result!
```

## Example 13: Custom Locale

```python
from anonimize import Anonymizer

# Use German locale for fake data
anon = Anonymizer(locale="de_DE")

data = {"name": "John", "city": "New York"}
config = {
    "name": {"strategy": "replace", "type": "name"},
    "city": {"strategy": "replace", "type": "city"},
}

result = anon.anonymize(data, config)
print(result)  # German names and cities
```

## Example 14: Statistics and Monitoring

```python
from anonimize import Anonymizer

anon = Anonymizer()

data = [{"name": f"User{i}", "email": f"user{i}@test.com"} for i in range(100)]
config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

results = anon.anonymize(data, config)

# Get statistics
stats = anon.get_stats()
print(f"Cached values: {stats['cached_values']}")
```

## Example 15: Clear Cache

```python
from anonimize import Anonymizer

anon = Anonymizer(preserve_relationships=True)

# Process first batch
data1 = [{"name": "John"}, {"name": "Jane"}]
anon.anonymize(data1, {"name": {"strategy": "replace", "type": "name"}})

# Clear cache before processing different data
anon.clear_cache()

# Process second batch (no relationship with first batch)
data2 = [{"name": "John"}, {"name": "Jane"}]
anon.anonymize(data2, {"name": {"strategy": "replace", "type": "name"}})
```
