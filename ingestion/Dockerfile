# ── Ingestion Layer Dockerfile ─────────────────────────────────────────────────
# Build context: project root (d:\PJ DE\MarketPulse)
# Usage:
#   docker compose -f docker-compose.yml -f ingestion/docker-compose.yml up -d
#
# Why build from root? We need access to:
#   - sdk/openapi-sdk/python  (DNSE SDK)
#   - schemas/                (Avro schemas)
#   - ingestion/              (producers, consumers, common)
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# System dependencies (only what's needed for psycopg2-binary + deltalake)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Step 1: Install DNSE SDK ──────────────────────────────────────────────────
# Copy the SDK source and install it as a package.
# This is done before the rest so pip can cache this layer separately.
COPY sdk/openapi-sdk/python ./sdk/python
RUN pip install --no-cache-dir ./sdk/python

# ── Step 2: Install Python dependencies ───────────────────────────────────────
COPY ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Step 3: Copy application source ───────────────────────────────────────────
COPY ingestion/ ./ingestion/
COPY schemas/   ./schemas/

# ── Python path ───────────────────────────────────────────────────────────────
# ingestion/ is the root package; sdk/python is the DNSE SDK.
ENV PYTHONPATH=/app

# Default: no command (set per-service in docker-compose)
CMD ["python", "--version"]
