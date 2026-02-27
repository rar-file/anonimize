# API Reference - Core

## Anonymizer

::: anonimize.core.Anonymizer
    options:
      show_source: true
      show_root_heading: true

## Anonymizer Configuration

The `Anonymizer` class can be configured with the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `locale` | `str` | `"en_US"` | Locale for fake data generation |
| `preserve_relationships` | `bool` | `True` | Whether to preserve relationships between fields |
| `seed` | `Optional[int]` | `None` | Random seed for reproducible results |

## Configuration Dictionary Format

```python
config = {
    "field_name": {
        "strategy": "replace",  # or "hash", "mask", "remove"
        "type": "email",        # PII type for replace strategy
        # Additional strategy-specific options
    }
}
```

## Error Handling

The Anonymizer raises the following exceptions:

- `ValueError`: When an invalid strategy or configuration is provided
- `TypeError`: When data types don't match expected formats

## Example Usage

```python
from anonimize import Anonymizer

# Basic usage
anon = Anonymizer()

# With configuration
anon = Anonymizer(
    locale="de_DE",
    preserve_relationships=True,
    seed=42
)

# Chain configuration
anon = Anonymizer().configure({"locale": "fr_FR"})
```
