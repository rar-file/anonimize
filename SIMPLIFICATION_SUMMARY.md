# Anonimize Simplification Summary

This document summarizes the changes made to simplify and document anonimize for 5-minute productivity.

## What Was Added

### 1. Simple API (`src/anonimize/simple.py`)
- **Dead-simple interface**: `from anonimize import anonymize`
- **3-line usage**: import → anonymize → done
- **Auto-detection**: No configuration needed for basic usage
- **Functions added**:
  - `anonymize()` - File anonymization (CSV, JSON, JSONL)
  - `anonymize_data()` - In-memory data anonymization
  - `detect_pii()` - PII detection without anonymizing
  - `preview()` - Preview changes before applying

### 2. Interactive CLI Wizard (`src/anonimize/cli/wizard.py`)
- **Autonomy-style guided setup**
- **6-step wizard**:
  1. Welcome and explanation
  2. Select data source (CSV, JSON, JSONL)
  3. Auto-detect PII
  4. Configure columns to anonymize
  5. Preview changes
  6. Execute with progress bar
- **Fallback support** without questionary library

### 3. Enhanced CLI (`src/anonimize/cli/__init__.py`)
- **Commands**:
  - `anonimize FILE` - Simple anonymization
  - `anonimize detect FILE` - PII detection
  - `anonimize preview FILE` - Preview changes
  - `anonimize config --generate` - Generate config file
  - `anonimize --wizard` - Launch interactive wizard
- **Options**: `--dry-run`, `--strategy`, `--output`, `--columns`, `--locale`, `--seed`
- **Better error messages** with suggestions

### 4. Five One-Liner Examples (`examples/one_liners.py`)
1. Basic file anonymization
2. In-memory data
3. Preview changes
4. Different strategies
5. Detect PII only

### 5. Tutorial Notebook (`examples/tutorial.ipynb`)
- **7-part tutorial** covering:
  1. The Basics (2 min)
  2. Understanding Detection (2 min)
  3. Choosing Strategies (2 min)
  4. File Operations (2 min)
  5. Advanced Features (2 min)
  6. Common Patterns (2 min)
  7. CLI Usage (1 min)
- Interactive examples with explanations

### 6. Config File Generator
- Generate sample `anonimize.yaml` with CLI:
  ```bash
  anonimize config --generate
  ```
- Includes all common configuration options

### 7. Progress Bars
- Added to `CSVAnonymizer.anonymize()`
- Optional `show_progress=True` parameter
- Requires `tqdm` (installed with `[cli]` extra)
- Shows row count, elapsed time, ETA

### 8. Better Error Messages (`src/anonimize/errors.py`)
- Custom exception classes with suggestions
- `AnonimizeError` - Base with hint system
- `FileNotFoundError` - Check path, permissions
- `UnsupportedFileTypeError` - Supported formats list
- `NoPiiDetectedError` - Why and what to do
- `InvalidStrategyError` - Valid strategies list
- `PhoneyNotInstalledError` - Install instructions
- `format_error()` helper for any exception

### 9. Auto-Detect PII Types (Smart Defaults)
- **Value-based detection**: Email, phone, SSN, credit card, IP, UUID
- **Name-based detection**: Column names like 'email', 'phone', 'ssn'
- **Confidence scores**: Shows detection certainty
- **No configuration required** for basic usage

### 10. Simplified Imports
Now supports:
```python
from anonimize import anonymize, anonymize_data, detect_pii, preview
```

Instead of:
```python
from anonimize.core import Anonymizer
anon = Anonymizer()
# ... complex configuration
```

## Files Changed

### New Files
- `src/anonimize/simple.py` - Simple API implementation
- `src/anonimize/cli/__init__.py` - CLI entry point
- `src/anonimize/cli/wizard.py` - Interactive wizard
- `src/anonimize/cli/__main__.py` - Module execution support
- `src/anonimize/errors.py` - Custom exceptions
- `examples/one_liners.py` - 5 one-liner examples
- `examples/tutorial.ipynb` - Complete tutorial
- `README_SIMPLE.md` → `README.md` - Simplified README
- `README_OLD.md` - Original README (backed up)

### Modified Files
- `src/anonimize/__init__.py` - Added simple API exports
- `src/anonimize/anonymizers/csv_anon.py` - Added progress bar support
- `pyproject.toml` - Added CLI entry points, optional dependencies

## Quick Reference

### Installation
```bash
# Basic
pip install anonimize

# With CLI features (wizard, progress bars)
pip install "anonimize[cli]"

# All features
pip install "anonimize[all]"
```

### 3-Line Quick Start
```python
from anonimize import anonymize
anonymize("input.csv", "output.csv")
```

### CLI Quick Start
```bash
# Interactive wizard
anonimize --wizard

# Simple anonymization
anonimize data.csv

# Preview changes
anonimize data.csv --dry-run

# Detect PII
anonimize detect data.csv
```

## Testing

All existing tests pass:
```bash
python -m pytest tests/ -v
```

New functionality tested:
- Simple API with dict/list data
- CLI commands (detect, preview, config, anonymize)
- File anonymization (CSV, JSON)
- PII detection
- Error messages

## Backwards Compatibility

All changes are **backwards compatible**:
- Original `Anonymizer` class still works exactly as before
- All existing imports continue to work
- Existing code requires no changes
- New simple API is additive only

## Next Steps for Users

1. **Quick Start**: `anonimize --wizard`
2. **Tutorial**: Open `examples/tutorial.ipynb`
3. **Examples**: Run `python examples/one_liners.py`
4. **Documentation**: See simplified `README.md`

## Goal Achievement

✅ **User should be productive in 5 minutes**

- 1 minute: Install with pip
- 2 minutes: Run `anonimize --wizard` or copy 3-line example
- 2 minutes: Customize with strategies, columns, etc.

All 10 requirements met:
1. ✅ Simple API - 3 lines to anonymize
2. ✅ Interactive CLI wizard
3. ✅ 5 one-liner examples
4. ✅ --dry-run mode
5. ✅ Config file generator
6. ✅ Progress bars
7. ✅ Better error messages with suggestions
8. ✅ Auto-detect PII types
9. ✅ Simplified imports
10. ✅ Tutorial notebook
