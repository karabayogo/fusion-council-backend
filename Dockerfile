FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends sqlite3 curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

# Copy source
COPY src/ ./src/
COPY config/ ./config/

# Create data directory
RUN mkdir -p /app/data

# Non-root user
RUN useradd -r -s /bin/false appuser
USER appuser

EXPOSE 8080

# Default: run API. Override with: docker run ... python -m fusion_council_service.domain.worker_loop
CMD ["uvicorn", "fusion_council_service.main:app", "--host", "0.0.0.0", "--port", "8080"]