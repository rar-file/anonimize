# Contributing to Anonimize

Thank you for your interest in contributing to Anonimize! We welcome contributions from the community and are pleased to have you join us.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- A GitHub account

### Setting Up Your Development Environment

1. **Fork the repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/anonimize.git
   cd anonimize
   ```

3. **Set up the upstream remote**:
   ```bash
   git remote add upstream https://github.com/rar-file/anonimize.git
   ```

4. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

5. **Install development dependencies**:
   ```bash
   pip install -e ".[dev,all]"
   ```

6. **Verify your setup**:
   ```bash
   pytest
   ```

## Development Workflow

### Branching Strategy

We follow a simplified GitFlow workflow:

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/*` - Feature branches
- `bugfix/*` - Bug fix branches
- `hotfix/*` - Urgent production fixes

### Making Changes

1. **Create a new branch** from `develop`:
   ```bash
   git checkout develop
   git pull upstream develop
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following our coding standards

3. **Run tests and linting**:
   ```bash
   # Run tests
   pytest

   # Run linting
   black src tests
   ruff check src tests
   mypy src
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub

## Project Structure

```
anonimize/
├── src/anonimize/          # Main source code
│   ├── __init__.py
│   ├── core.py            # Main Anonymizer class
│   ├── anonymizers/       # Anonymizer implementations
│   ├── detectors/         # PII detection modules
│   └── utils.py
├── tests/                 # Test suite
│   ├── test_core.py
│   └── test_anonymizers/
├── examples/              # Usage examples
├── docs/                  # Documentation
├── benchmarks/            # Performance benchmarks
├── .github/workflows/     # CI/CD configuration
├── pyproject.toml         # Project configuration
└── README.md
```

## Coding Standards

### Python Style

We use:
- **Black** for code formatting (line length: 88)
- **Ruff** for linting
- **MyPy** for type checking

### Code Style Guidelines

1. **Follow PEP 8** with Black formatting
2. **Use type hints** for all function signatures
3. **Write docstrings** for all public APIs using Google style
4. **Keep functions focused** - single responsibility principle
5. **Use descriptive variable names**

### Example Function

```python
from typing import Dict, Any, Optional

def anonymize_field(
    value: str,
    strategy: str,
    config: Optional[Dict[str, Any]] = None
) -> str:
    """Anonymize a single field value.
    
    Args:
        value: The original value to anonymize.
        strategy: The anonymization strategy to use.
        config: Optional configuration for the strategy.
    
    Returns:
        The anonymized value.
    
    Raises:
        ValueError: If the strategy is not supported.
    
    Example:
        >>> anonymize_field("john@example.com", "mask")
        '***@example.com'
    """
    if config is None:
        config = {}
    
    # Implementation here
    pass
```

## Testing Guidelines

### Test Structure

- Tests are in the `tests/` directory
- Mirror the source structure
- Use `pytest` as the test runner

### Writing Tests

```python
import pytest
from anonimize import Anonymizer

class TestAnonymizer:
    """Test cases for the Anonymizer class."""
    
    def test_anonymize_with_replace_strategy(self):
        """Test that replace strategy works correctly."""
        anon = Anonymizer()
        data = {"name": "John Doe"}
        config = {"name": {"strategy": "replace", "type": "name"}}
        
        result = anon.anonymize(data, config)
        
        assert result["name"] != "John Doe"
        assert isinstance(result["name"], str)
    
    def test_anonymize_preserves_relationships(self):
        """Test that relationship preservation works."""
        anon = Anonymizer(preserve_relationships=True)
        # Test implementation
        pass
```

### Test Coverage

- Aim for at least 80% code coverage
- Run coverage with: `pytest --cov=anonimize --cov-report=html`

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=anonimize

# Run specific test file
pytest tests/test_core.py

# Run with verbose output
pytest -v

# Run only integration tests
pytest -m integration

# Run excluding slow tests
pytest -m "not slow"
```

## Documentation

### Code Documentation

- All public functions, classes, and modules must have docstrings
- Use Google-style docstrings
- Include type hints
- Provide usage examples where helpful

### Documentation Files

- Update `README.md` if adding major features
- Add examples to `examples/` directory
- Update API docs in `docs/api/`

### Building Documentation

```bash
# Install docs dependencies
pip install mkdocs mkdocs-material mkdocstrings[python]

# Serve docs locally
mkdocs serve

# Build docs
mkdocs build
```

## Submitting Changes

### Pull Request Process

1. **Update the README.md** with details of changes if applicable
2. **Update documentation** for any API changes
3. **Add tests** for new functionality
4. **Ensure all tests pass**
5. **Update the CHANGELOG.md** with your changes
6. **Link any related issues** in the PR description

### PR Checklist

- [ ] Code follows the style guidelines
- [ ] Self-review of code completed
- [ ] Code is commented, particularly in hard-to-understand areas
- [ ] Corresponding documentation changes made
- [ ] Tests added that prove the fix is effective or feature works
- [ ] New and existing unit tests pass locally
- [ ] Dependent changes have been merged and published

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): subject

body (optional)

footer (optional)
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Test-related changes
- `chore`: Build/tooling changes

Examples:
```
feat(anonymizer): add support for custom hash algorithms

fix(csv): handle missing columns gracefully

docs: update installation instructions
```

## Release Process

1. **Update version** in `src/anonimize/__version__.py`
2. **Update CHANGELOG.md** with release notes
3. **Create a tag**: `git tag -a v1.0.0 -m "Release version 1.0.0"`
4. **Push the tag**: `git push origin v1.0.0`
5. **GitHub Actions** will automatically build and publish

## Getting Help

- **GitHub Discussions**: For questions and ideas
- **GitHub Issues**: For bug reports and feature requests
- **Discord**: [Join our community](https://discord.gg/rarcodes)

## Recognition

Contributors will be recognized in our README.md and release notes.

Thank you for contributing to Anonimize!
