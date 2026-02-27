# Architecture

## Overview

Anonimize follows a modular architecture designed for extensibility and performance.

## Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Anonymizer (Core)                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │  Strategy       │  │  Configuration                  │  │
│  │  Registry       │  │  - Fields to anonymize          │  │
│  │  - replace      │  │  - Strategy per field           │  │
│  │  - hash         │  │  - Type hints                   │  │
│  │  - mask         │  │  - Options                      │  │
│  │  - remove       │  │                                 │  │
│  └─────────────────┘  └─────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Data Source Interface                    │  │
│  │         (BaseAnonymizer - Abstract Base)             │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  CSVAnonymizer│    │ JSONAnonymizer│    │   Database    │
│               │    │               │    │  Anonymizer   │
│ • Chunked     │    │ • Nested      │    │ • SQLAlchemy  │
│   processing  │    │   paths       │    │ • Transactions│
│ • Streaming   │    │ • Batch       │    │ • Batch       │
└───────────────┘    └───────────────┘    └───────────────┘
```

## Strategy Pattern

Anonimize uses the Strategy pattern for anonymization methods:

```python
class BaseAnonymizer(ABC):
    @abstractmethod
    def anonymize(self, data, config):
        pass

class ReplaceStrategy:
    def execute(self, value, pii_type):
        # Generate fake data
        pass

class HashStrategy:
    def execute(self, value, algorithm, salt):
        # Hash the value
        pass
```

## Relationship Preservation

The relationship preservation system uses a value cache:

```
Original Value ──> Cache Key (field:value) ──> Fake Value
                          │
                          ▼
                    ┌─────────────┐
                    │ Value Cache │
                    │  (dict)     │
                    └─────────────┘
```

This ensures that the same original value always maps to the same fake value within a session.

## Detection Pipeline

```
Input Data ──> Regex Detector ──┐
                                 ├──> Merged Results ──> Configuration
         ──> Heuristic Detector ┘
```

## Performance Considerations

### Memory Efficiency
- Streaming processing for large files
- Chunked database operations
- Configurable cache size limits

### Processing Speed
- Batch operations where possible
- Efficient data structures
- Minimal data copying

## Extension Points

1. **Custom Strategies**: Implement the strategy interface
2. **Custom Detectors**: Extend BaseDetector
3. **New Data Sources**: Extend BaseAnonymizer
