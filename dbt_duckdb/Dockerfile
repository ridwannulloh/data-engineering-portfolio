FROM python:3.11-slim

# dbt writes the warehouse file and target/ artifacts at runtime.
ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app \
    DBT_DUCKDB_PATH=/app/warehouse/portfolio.duckdb

WORKDIR /app

# Install dbt-duckdb (pulls in dbt-core + duckdb). Pinned for reproducibility.
RUN pip install --no-cache-dir "dbt-duckdb==1.9.*"

# Project files. profiles.yml lives in the repo and is picked up via
# DBT_PROFILES_DIR above, so no ~/.dbt setup is needed.
COPY . /app

# DuckDB does not create parent directories for the database file.
RUN mkdir -p /app/warehouse

# Persist the built warehouse outside the image layer.
VOLUME ["/app/warehouse"]

# `dbt build` runs seeds -> models -> tests in DAG order.
CMD ["dbt", "build"]
