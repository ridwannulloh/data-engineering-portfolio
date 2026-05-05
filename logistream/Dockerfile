# eclipse-temurin already ships Java 17 — no apt-get needed for Java
FROM eclipse-temurin:17-jre-jammy

# Install Python 3 on top of the Java base image
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*
ENV PYSPARK_PYTHON=python3
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-download Spark + Delta + Kafka connector JARs into this image layer.
# Containers start instantly without downloading JARs at runtime.
RUN python scripts/warmup_spark.py
