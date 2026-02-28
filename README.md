# Anonimize 🛡️

[![CI](https://github.com/rar-file/anonimize/actions/workflows/ci.yml/badge.svg)](https://github.com/rar-file/anonimize/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-161%20passing-brightgreen.svg)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen.svg)](.coverage)
[![PyPI version](https://badge.fury.io/py/anonimize.svg)](https://badge.fury.io/py/anonimize)
[![Python versions](https://img.shields.io/pypi/pyversions/anonimize.svg)](https://pypi.org/project/anonimize/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Docker](https://img.shields.io/badge/Docker-ghcr.io-blue?logo=docker)](https://github.com/rar-file/anonimize/pkgs/container/anonimize)
[![Downloads](https://img.shields.io/pypi/dm/anonimize.svg)](https://pypi.org/project/anonimize/)

> **Simple, fast, and reliable data anonymization for everyone.**

Anonimize makes it easy to protect sensitive information (PII) in your data while preserving data relationships and utility. Perfect for creating safe development datasets, sharing test data, or complying with privacy regulations like GDPR and CCPA.

✅ Now with 160+ tests and CI/CD ready!

---

## 🚀 Quick Start

### Installation

```bash
# Basic installation
pip install anonimize

# With all database and format support
pip install "anonimize[all]"

# Or install specific extras
pip install "anonimize[postgresql,mongodb,excel]"
```

### 30-Second Example

```python
from anonimize import Anonymizer

# Create anonymizer
anon = Anonymizer(seed=42)

# Your data with PII
data = {
    "name": "John Doe",
    "email": "john@example.com",
    "phone": "+1-555-0123",
    "ssn": "123-45-6789"
}

# Anonymize it
result = anon.anonymize(data)

print(result)
# {
#   "name": "Sarah Smith",     # Consistent fake name
#   "email": "sarah@email.com", # Different but realistic email
#   "phone": "+1-555-9876",     # Different phone number
#   "ssn": "456-78-9012"        # Different SSN
# }
```

### Command Line

```bash
# Detect PII in a file
anonimize detect customers.csv

# Anonymize a file
anonimize customers.csv --output customers_safe.csv

# Preview changes first
anonimize customers.csv --dry-run

# Interactive wizard (recommended for first-time users)
anonimize --wizard
```

---

## ✨ Features

### 🎯 Core Capabilities
- **PII Detection**: Automatically detects emails, phones, SSNs, credit cards, names, addresses
- **4 Anonymization Strategies**:
  - `replace` - Substitute with realistic fake data (default)
  - `mask` - Partial redaction (e.g., `j***@example.com`)
  - `hash` - One-way cryptographic hashing
  - `remove` - Delete the field entirely
- **Relationship Preservation**: Same input always produces same output (consistency)
- **Reproducibility**: Set a seed for consistent results across runs

### 📁 Supported Formats
- **Files**: CSV, JSON, JSONL, Parquet, Excel (.xlsx), Avro, XML
- **Databases**: PostgreSQL, MySQL, SQLite, MongoDB
- **Streaming**: Process files larger than memory

### 🔒 Privacy Features
- **Differential Privacy**: Add calibrated noise for mathematical privacy guarantees
- **K-Anonymity**: Ensure records can't be uniquely identified
- **Audit Logging**: Track what was anonymized and how

---

## 📖 Usage Guide

### Python API

#### Basic Usage

```python
from anonimize import Anonymizer

# Initialize
anon = Anonymizer(locale="en_US", seed=42)

# Auto-detect and anonymize PII
data = {"name": "John", "email": "john@example.com"}
result = anon.anonymize(data)
```

#### With Configuration

```python
config = {
    "name": {"strategy": "replace", "type": "name"},
    "email": {"strategy": "mask"},
    "ssn": {"strategy": "hash", "algorithm": "sha256"},
    "salary": {"strategy": "remove"}
}

result = anon.anonymize(data, config)
```

#### Database Anonymization

```python
from anonimize.connectors import create_connector

# Connect to database
conn = create_connector("postgresql://user:pass@localhost/db")

# Anonymize table
with conn:
    for batch in conn.scan_table("users", batch_size=1000):
        anonymized = [anon.anonymize(row) for row in batch]
        conn.write_table("users_anonymized", anonymized)
```

### CLI Usage

#### Commands

```bash
# Anonymize a file
anonimize data.csv --output safe.csv --strategy mask

# Detect PII without anonymizing
anonimize detect data.json --format json

# Preview first 5 rows
anonimize preview data.csv --num-rows 5

# Generate config file
anonimize config --generate --output anonimize.yaml
```

#### CLI Options

| Option | Description |
|--------|-------------|
| `--output, -o` | Output file path |
| `--strategy, -s` | Strategy: replace, mask, hash, remove |
| `--dry-run` | Preview changes without writing |
| `--columns, -c` | Comma-separated columns to anonymize |
| `--locale` | Locale for fake data (default: en_US) |
| `--seed` | Random seed for reproducibility |
| `--wizard, -w` | Interactive setup wizard |

### Configuration File

Create `anonimize.yaml`:

```yaml
global:
  locale: "en_US"
  seed: 42
  preserve_relationships: true

columns:
  email:
    strategy: "mask"
    type: "email"
  
  name:
    strategy: "replace"
    type: "name"
  
  ssn:
    strategy: "hash"
    options:
      algorithm: "sha256"
      salt: "your-secret-salt"
  
  salary:
    strategy: "remove"

detection:
  confidence_threshold: 0.7
  check_field_names: true
```

Use it:
```bash
anonimize data.csv --config anonimize.yaml
```

---

## 🐳 Docker Usage

```bash
# Pull the image
docker pull ghcr.io/rar-file/anonimize:latest

# Run anonymization
docker run --rm -v $(pwd)/data:/data \
  ghcr.io/rar-file/anonimize:latest \
  /data/customers.csv --output /data/safe.csv

# Or use docker-compose
docker-compose run --rm anonimize data/customers.csv
```

---

## 🔧 Advanced Features

### Differential Privacy

```python
from anonimize.privacy import DPAnonymizer

# Create DP anonymizer with privacy budget
dp = DPAnonymizer(total_epsilon=1.0, mechanism="laplace")

# Anonymize with privacy guarantee
result = dp.anonymize_numeric(
    value=100000,
    sensitivity=1000,
    epsilon=0.1
)
```

### K-Anonymity

```python
from anonimize.privacy import ensure_k_anonymity

# Ensure each record is indistinguishable from k-1 others
anonymized_df = ensure_k_anonymity(
    data=df,
    k=5,
    quasi_identifiers=['age', 'zipcode', 'gender']
)
```

### Custom PII Types

```python
from anonimize.detectors import register_pattern

# Register custom pattern
register_pattern(
    name="employee_id",
    pattern=r"EMP-\d{5}",
    category="identifier"
)

# Now it will be detected and can be anonymized
```

---

## 📦 Installation Options

| Extra | Includes | Install Command |
|-------|----------|-----------------|
| `postgresql` | PostgreSQL support | `pip install "anonimize[postgresql]"` |
| `mysql` | MySQL/MariaDB support | `pip install "anonimize[mysql]"` |
| `mongodb` | MongoDB support | `pip install "anonimize[mongodb]"` |
| `parquet` | Parquet file support | `pip install "anonimize[parquet]"` |
| `excel` | Excel file support | `pip install "anonimize[excel]"` |
| `avro` | Avro file support | `pip install "anonimize[avro]"` |
| `cli` | Interactive CLI wizard | `pip install "anonimize[cli]"` |
| `dev` | Development tools | `pip install "anonimize[dev]"` |
| `all` | Everything above | `pip install "anonimize[all]"` |

---

## 🧪 Development

```bash
# Clone the repo
git clone https://github.com/rar-file/anonimize.git
cd anonimize

# Install in development mode
pip install -e ".[dev,all]"

# Run tests
pytest

# Run linting
ruff check src/
black --check src/

# Install pre-commit hooks
pre-commit install
```

### Project Structure

```
anonimize/
├── src/anonimize/       # Main package
│   ├── core.py          # Core Anonymizer class
│   ├── anonymizers/     # Format-specific anonymizers
│   ├── connectors/      # Database connectors
│   ├── detectors/       # PII detection
│   ├── formats/         # File format handlers
│   ├── privacy/         # Differential privacy, k-anonymity
│   └── cli/             # Command line interface
├── tests/               # Test suite
├── docs/                # Documentation
└── examples/            # Example scripts
```

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit (`git commit -m 'Add amazing feature'`)
6. Push (`git push origin feature/amazing-feature`)
7. Open a Pull Request

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file.

---

## 🙏 Acknowledgments

- Built with [Phoney](https://github.com/rar-file/phoney) for realistic fake data generation
- Inspired by privacy research from Google DP Library and Microsoft Differential Privacy

---

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/rar-file/anonimize/issues)
- **Discussions**: [GitHub Discussions](https://github.com/rar-file/anonimize/discussions)
- **Documentation**: [Full Docs](https://github.com/rar-file/anonimize/tree/main/docs)

---

**Made with ❤️ for data privacy**
