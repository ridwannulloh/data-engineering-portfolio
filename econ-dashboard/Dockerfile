# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install dependencies into a separate layer for cache efficiency
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY app.py        ./app.py
COPY ingest.py     ./ingest.py

# Cloud Run sets PORT env var; Streamlit must listen on it
ENV PORT=8080

# Non-root user for security
RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 8080

# Streamlit flags for headless Cloud Run environment
CMD ["sh", "-c", \
     "streamlit run app.py \
      --server.port=$PORT \
      --server.address=0.0.0.0 \
      --server.headless=true \
      --server.enableCORS=false \
      --server.enableXsrfProtection=false \
      --browser.gatherUsageStats=false"]
