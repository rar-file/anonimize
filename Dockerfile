# Multi-stage build for anonimize
# Optimized for production use with minimal image size

# =============================================================================
# Build Stage
# =============================================================================
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only files needed for build
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/

# Build wheel
RUN pip install --no-cache-dir build && \
    python -m build --wheel

# =============================================================================
# Production Stage
# =============================================================================
FROM python:3.12-slim

LABEL maintainer="Anonimize Team <anonimize@example.com>"
LABEL description="Anonimize - Data anonymization tool for PII protection"
LABEL org.opencontainers.image.source="https://github.com/rar-file/anonimize"
LABEL org.opencontainers.image.documentation="https://github.com/rar-file/anonimize#readme"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy wheel from builder
COPY --from=builder /build/dist/*.whl /tmp/

# Install the package with all extras
RUN pip install --no-cache-dir /tmp/*.whl[all] && \
    rm -tmp/*.whl && \
    pip cache purge

# Create non-root user for security
RUN groupadd -r anonimize && \
    useradd -r -g anonimize -u 1000 -d /home/anonimize -s /bin/bash anonimize && \
    mkdir -p /data /home/anonimize && \
    chown -R anonimize:anonimize /data /home/anonimize

USER anonimize

# Default work directory for data
WORKDIR /data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD anonimize --version || exit 1

# Entry point
ENTRYPOINT ["anonimize"]
CMD ["--help"]
