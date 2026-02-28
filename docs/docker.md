# Docker Deployment Guide

Complete guide for deploying Anonimize using Docker and Docker Compose.

## Table of Contents

- [Quick Start](#quick-start)
- [Building the Image](#building-the-image)
- [Running with Docker](#running-with-docker)
- [Docker Compose](#docker-compose)
- [Environment Variables](#environment-variables)
- [Volume Mounts](#volume-mounts)
- [Production Deployment](#production-deployment)
- [Registry Publishing](#registry-publishing)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/rar-file/anonimize.git
cd anonimize

# Build and run
docker-compose up --build anonimize

# Anonymize a file
docker-compose run --rm anonimize /data/customers.csv --output /data/safe.csv
```

---

## Building the Image

### Build Locally

```bash
# Build the production image
docker build -t anonimize:latest .

# Verify the build
docker run --rm anonimize:latest --version
```

### Multi-Stage Build

The Dockerfile uses multi-stage builds to minimize image size:

1. **Builder stage**: Compiles the Python wheel with build dependencies
2. **Production stage**: Minimal image with only runtime dependencies

```bash
# Check image size
docker images anonimize:latest
```

### Build Options

```bash
# Build with specific Python version
docker build --build-arg PYTHON_VERSION=3.11 -t anonimize:latest .

# Build with no cache (clean build)
docker build --no-cache -t anonimize:latest .
```

---

## Running with Docker

### Basic Usage

```bash
# Check version
docker run --rm anonimize:latest --version

# Show help
docker run --rm anonimize:latest --help

# Anonymize a CSV file
docker run --rm -v $(pwd)/data:/data anonimize:latest \
  /data/customers.csv --output /data/customers_safe.csv

# Detect PII in a file
docker run --rm -v $(pwd)/data:/data anonimize:latest \
  detect /data/customers.csv
```

### With Configuration File

```bash
# Mount config file
docker run --rm \
  -v $(pwd)/data:/data:rw \
  -v $(pwd)/config/anonimize.yaml:/config/anonimize.yaml:ro \
  anonimize:latest \
  /data/customers.csv --config /config/anonimize.yaml
```

### Interactive Mode

```bash
# Run wizard
docker run --rm -it -v $(pwd)/data:/data anonimize:latest --wizard
```

---

## Docker Compose

### Services

The `docker-compose.yml` includes multiple services:

| Service | Description |
|---------|-------------|
| `anonimize` | Main anonymization service |
| `postgres` | PostgreSQL for testing |
| `mongodb` | MongoDB for testing |
| `mysql` | MySQL for testing |
| `dev` | Development environment |

### Common Commands

```bash
# Run anonymization
docker-compose run --rm anonimize /data/file.csv --output /data/output.csv

# Run with database support
docker-compose up -d postgres
docker-compose run --rm anonimize /data/file.csv

# Development shell
docker-compose run --rm dev

# Run tests in container
docker-compose run --rm dev pytest

# Clean up
docker-compose down -v
```

### Example Workflows

#### Anonymize CSV with PostgreSQL

```bash
# Start database
docker-compose up -d postgres

# Wait for database to be ready
sleep 5

# Run anonymization
docker-compose run --rm -e DATABASE_URL=postgresql://test:test@postgres:5432/testdb \
  anonimize /data/customers.csv --output /data/safe.csv
```

#### Batch Processing

```bash
#!/bin/bash
# batch_anonymize.sh

DATA_DIR=./data
OUTPUT_DIR=./data/output

mkdir -p $OUTPUT_DIR

for file in $DATA_DIR/*.csv; do
    filename=$(basename "$file")
    docker-compose run --rm anonimize \
        "/data/$filename" \
        --output "/data/output/${filename%.csv}_safe.csv"
done
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ANONIMIZE_LOCALE` | Locale for fake data | `en_US` |
| `ANONIMIZE_SEED` | Random seed for reproducibility | `42` |
| `ANONIMIZE_DEBUG` | Enable debug logging | `0` |
| `DATABASE_URL` | Database connection string | - |

### Setting Environment Variables

```bash
# Command line
docker run --rm -e ANONIMIZE_LOCALE=de_DE -e ANONIMIZE_SEED=123 \
  anonimize:latest /data/file.csv

# Docker Compose
export ANONIMIZE_LOCALE=de_DE
docker-compose up anonimize
```

---

## Volume Mounts

### Required Mounts

| Container Path | Purpose | Mode |
|----------------|---------|------|
| `/data` | Input/output files | `rw` |

### Optional Mounts

| Container Path | Purpose | Mode |
|----------------|---------|------|
| `/config` | Configuration files | `ro` |

### Examples

```bash
# Mount current directory to /data
docker run --rm -v $(pwd):/data anonimize:latest file.csv

# Mount specific directories
docker run --rm \
  -v $(pwd)/input:/data/input:ro \
  -v $(pwd)/output:/data/output:rw \
  anonimize:latest /data/input/file.csv --output /data/output/file.csv

# Mount config
docker run --rm \
  -v $(pwd)/data:/data:rw \
  -v $(pwd)/config:/config:ro \
  anonimize:latest /data/file.csv --config /config/anonimize.yaml
```

---

## Production Deployment

### Security Considerations

1. **Non-root user**: Container runs as `anonimize` user (UID 1000)
2. **Read-only root filesystem**: Add `--read-only` flag
3. **Resource limits**: Set CPU and memory limits

```bash
docker run --rm \
  --read-only \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  --memory="512m" \
  --cpus="1.0" \
  -v $(pwd)/data:/data:rw \
  anonimize:latest /data/file.csv
```

### Docker Compose Production

```yaml
version: '3.8'

services:
  anonimize:
    image: ghcr.io/rar-file/anonimize:latest
    read_only: true
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 128M
    volumes:
      - ./data:/data:rw
    environment:
      - ANONIMIZE_LOCALE=en_US
      - ANONIMIZE_SEED=42
```

### Kubernetes

See `docs/kubernetes.md` for Kubernetes deployment manifests.

---

## Registry Publishing

### GitHub Container Registry

```bash
# Login
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Tag
docker tag anonimize:latest ghcr.io/rar-file/anonimize:latest
docker tag anonimize:latest ghcr.io/rar-file/anonimize:v0.1.0

# Push
docker push ghcr.io/rar-file/anonimize:latest
docker push ghcr.io/rar-file/anonimize:v0.1.0
```

### Docker Hub

```bash
# Login
docker login -u username

# Tag
docker tag anonimize:latest username/anonimize:latest

# Push
docker push username/anonimize:latest
```

---

## Troubleshooting

### Common Issues

#### Permission Denied

```bash
# Fix data directory permissions
sudo chown -R 1000:1000 ./data

# Or run with current user
docker run --rm -u $(id -u):$(id -g) -v $(pwd)/data:/data anonimize:latest
```

#### File Not Found

```bash
# Ensure absolute paths
docker run --rm -v $(pwd)/data:/data anonimize:latest /data/file.csv

# Check file exists
docker run --rm -v $(pwd)/data:/data anonimize:latest ls -la /data
```

#### Database Connection Failed

```bash
# Ensure database container is running
docker-compose up -d postgres

# Check network
docker network ls
docker network inspect anonimize_default
```

### Debugging

```bash
# Shell into container
docker run --rm -it --entrypoint bash anonimize:latest

# Check logs
docker-compose logs anonimize

# Verbose mode
docker run --rm -e ANONIMIZE_DEBUG=1 anonimize:latest /data/file.csv
```

### Health Check

The container includes a health check:

```bash
# Check health status
docker inspect --format='{{.State.Health.Status}}' container_name
```

---

## Development

### Using Dev Container

```bash
# Start development environment
docker-compose run --rm dev

# Inside container, install in editable mode
pip install -e ".[all,dev]"

# Run tests
pytest

# Run linting
ruff check src/
black src/
```

### Rebuilding After Changes

```bash
# Rebuild after code changes
docker-compose build --no-cache anonimize

# Or use dev mode for live reload
docker-compose run --rm dev python -m anonimize.cli file.csv
```

---

## Additional Resources

- [Main README](../README.md)
- [CLI Reference](../README.md#cli-usage)
- [Configuration Guide](../README.md#configuration-file)
- [Contributing Guide](../CONTRIBUTING.md)
