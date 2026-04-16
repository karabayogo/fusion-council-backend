FROM python:3.12-slim AS builder

WORKDIR /build

# Install build deps
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# --- Runtime image ---
FROM python:3.12-slim

WORKDIR /app

# Install runtime system deps only
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn

# Copy application source
COPY src/ ./src/
COPY config/ ./config/

# Create data directory with correct ownership
RUN mkdir -p /app/data \
    && useradd -r -s /bin/false appuser \
    && chown -R appuser:appuser /app/data

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD ["curl", "-f", "http://localhost:8080/healthz"]

# Default: run API. Override with: docker run ... python -m fusion_council_service.domain
CMD ["uvicorn", "fusion_council_service.main:app", "--host", "0.0.0.0", "--port", "8080"]