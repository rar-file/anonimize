# Installation

## Requirements

- Python 3.8 or higher
- pip 20.0 or higher

## Install from PyPI

```bash
pip install anonimize
```

## Install with Optional Dependencies

### Database Support

```bash
# PostgreSQL
pip install "anonimize[postgresql]"

# MySQL
pip install "anonimize[mysql]"

# All databases
pip install "anonimize[all]"
```

### Development Tools

```bash
pip install "anonimize[dev]"
```

## Verify Installation

```python
from anonimize import Anonymizer

anon = Anonymizer()
print(anon.get_stats())
```

## Upgrade

```bash
pip install --upgrade anonimize
```

## Uninstall

```bash
pip uninstall anonimize
```
