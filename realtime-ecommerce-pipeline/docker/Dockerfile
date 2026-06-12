# ─────────────────────────────────────────────────────────────────────────
# App image for the Python pipeline services:
#   producers · bronze-sink consumer · batch (bronze→silver→gold) pipelines
#
# The PyFlink streaming jobs are NOT run from this image — they need a JVM and
# the Flink Kafka connector jar, and are run separately (see README).
# Build context is the project root: `docker build -f docker/Dockerfile ..`
# ─────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install runtime deps first for better layer caching.
COPY docker/requirements-docker.txt ./requirements-docker.txt
RUN pip install --no-cache-dir -r requirements-docker.txt

# Copy the application code.
COPY . .

# Default command is overridden per-service in docker-compose.yml.
CMD ["python", "-c", "print('Specify a command in docker-compose.yml')"]
