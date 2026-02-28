# Getting Started

## Installation

### Prerequisites

- Python 3.8 or higher
- pip

### Basic Installation

```bash
pip install anonimize
```

### With Database Support

```bash
# For PostgreSQL
pip install "anonimize[postgresql]"

# For MySQL
pip install "anonimize[mysql]"

# For all database drivers
pip install "anonimize[all]"
```

### Development Installation

```bash
git clone https://github.com/rar-file/anonimize.git
cd anonimize
pip install -e ".[dev,all]"
```

## Quick Start

### 1. Basic Anonymization

```python
from anonimize import Anonymizer

# Create anonymizer
anon = Anonymizer()

# Define data
data = {
    "name": "John Doe",
    "email": "john@example.com",
    "phone": "+1-555-123-4567"
}

# Configure anonymization
config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
    "phone": {"strategy": "mask", "type": "phone"},
}

# Anonymize
result = anon.anonymize(data, config)
print(result)
```

Output:
```python
{
    'name': 'Alice Smith',
    'email': 'alice@email.com',
    'phone': '***-***-4567'
}
```

### 2. Batch Processing

```python
from anonimize import Anonymizer

anon = Anonymizer()

# Process multiple records
data = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"},
]

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

results = anon.anonymize(data, config)
```

### 3. File Anonymization

```python
from anonimize.anonymizers.csv_anon import CSVAnonymizer

anon = CSVAnonymizer()

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "replace", "type": "email"},
}

anon.anonymize(
    input_path="input.csv",
    output_path="output.csv",
    config=config
)
```

### 4. Database Anonymization

```python
from anonimize.anonymizers.database import DatabaseAnonymizer

anon = DatabaseAnonymizer("postgresql://user:pass@localhost/db")

config = {
    "users": {
        "name": {"strategy": "replace", "type": "name"},
        "email": {"strategy": "replace", "type": "email"},
    }
}

anon.anonymize(config=config)
```

## Next Steps

- Learn about [Anonymization Strategies](../user-guide/strategies.md)
- Explore [PII Detection](../user-guide/detection.md)
- See more [Examples](../examples/basic.md)
