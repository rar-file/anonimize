# Anonimize

[![CI](https://github.com/rar-file/anonimize/actions/workflows/ci.yml/badge.svg)](https://github.com/rar-file/anonimize/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/anonimize.svg)](https://badge.fury.io/py/anonimize)
[![Python versions](https://img.shields.io/pypi/pyversions/anonimize.svg)](https://pypi.org/project/anonimize/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Enterprise-grade data anonymization platform for PII protection

Anonimize is a comprehensive data anonymization toolkit that helps organizations protect sensitive information while preserving data utility. Built with enterprise requirements in mind, it supports multiple anonymization strategies, database connectors, and file formats.

## Features

### Anonymization Strategies
- **Replace**: Substitute PII with realistic fake data (consistent per value)
- **Hash**: One-way cryptographic hashing
- **Mask**: Partial redaction (e.g., `j***@example.com`)
- **Token**: Reversible encryption for testing scenarios

### Supported PII Types
- Email addresses
- Phone numbers (US/international)
- Social Security Numbers
- Credit card numbers (with Luhn validation)
- Names, addresses, dates
- Custom regex patterns

### Database Connectors
- PostgreSQL
- MySQL / MariaDB
- SQLite (file & in-memory)
- MongoDB

### File Formats
- JSON / JSONL
- CSV / TSV
- Parquet
- Excel (xlsx)
- Avro
- XML

## Quick Start

```bash
pip install anonimize
```

```python
from anonimize import Anonymizer

# Basic usage
anon = Anonymizer(locale="en_US", seed=42)

# Anonymize a record
data = {
    "name": "John Doe",
    "email": "john@example.com",
    "ssn": "123-45-6789"
}

config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "mask"},
    "ssn": {"strategy": "hash"}
}

result = anon.anonymize(data, config)
# Result: {
#   "name": "Sarah Smith",  # Consistent fake name
#   "email": "j***@example.com",
#   "ssn": "SSN-A7B3C9D2E1F8"
# }
```

## Database Anonymization

```python
from anonimize.connectors import PostgreSQLConnector
from anonimize import Anonymizer

# Connect to database
with PostgreSQLConnector("postgresql://user:pass@localhost/db") as conn:
    # Anonymize entire table
    anon = Anonymizer()
    
    for batch in conn.read_table("users", batch_size=1000):
        anonymized = [anon.anonymize(row, config) for row in batch]
        conn.write_table("users_anonymized", anonymized)
```

## CLI Usage

```bash
# Anonymize a CSV file
anonimize anonymize data.csv --config config.yaml --output anonymized.csv

# Anonymize database table
anonimize db anonymize \
  --connection postgresql://user:pass@localhost/db \
  --table users \
  --config config.yaml

# Dry run (preview changes)
anonimize anonymize data.csv --config config.yaml --dry-run
```

## Configuration

```yaml
# config.yaml
strategies:
  email:
    type: "email"
    strategy: "mask"
    options:
      mask_local: true
      
  ssn:
    type: "ssn"
    strategy: "replace"
    
  credit_card:
    type: "credit_card"
    strategy: "token"
    options:
      encryption_key: ${TOKEN_KEY}

global:
  preserve_relationships: true
  locale: "en_US"
  seed: 42
```

## Advanced Features

### Differential Privacy
```python
from anonimize.privacy import add_noise

# Add calibrated noise for privacy guarantees
anonymized_value = add_noise(
    value=100000,
    epsilon=0.1,  # Privacy budget
    sensitivity=10000
)
```

### K-Anonymity
```python
from anonimize.privacy import ensure_k_anonymity

# Ensure each record is indistinguishable from k-1 others
anonymized = ensure_k_anonymity(
    data=df,
    k=5,
    quasi_identifiers=['age', 'zipcode', 'gender']
)
```

### Data Lineage
```python
from anonimize.lineage import LineageTracker

tracker = LineageTracker()

with tracker.track():
    result = anonymizer.anonymize(data, config)

# Get audit trail
print(tracker.get_report())
# Shows: what was anonymized, how, when, by whom
```

## Installation

### Basic
```bash
pip install anonimize
```

### With Database Support
```bash
pip install "anonimize[postgresql,mysql]"
```

### All Features
```bash
pip install "anonimize[all]"
```

## Development

```bash
git clone https://github.com/rar-file/anonimize.git
cd anonimize
pip install -e ".[dev]"
pytest
```

## Architecture

```
anonimize/
├── anonymizers/     # PII-specific anonymization logic
├── connectors/      # Database connectors
├── formats/         # File format handlers
├── detectors/       # PII detection algorithms
├── privacy/         # Differential privacy, k-anonymity
└── lineage/         # Data lineage tracking
```

## License

MIT License - see [LICENSE](LICENSE) file.

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
