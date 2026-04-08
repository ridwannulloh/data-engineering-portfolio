# Dockerfile for Olist ETL Pipeline with Airflow
FROM apache/airflow:2.8.0-python3.11

# Switch to root to install Java (required for PySpark)
USER root

# Install Java 17 (required for PySpark 3.5)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy the pipeline source code
COPY --chown=airflow:root src/ /opt/olist-etl-pipeline/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root tests/ /opt/olist-etl-pipeline/tests/

# Create data directories with proper permissions
USER root
RUN mkdir -p /opt/olist-etl-pipeline/data/raw \
    /opt/olist-etl-pipeline/data/bronze \
    /opt/olist-etl-pipeline/data/silver \
    /opt/olist-etl-pipeline/data/gold && \
    chown -R airflow:root /opt/olist-etl-pipeline/data

USER airflow

# Set working directory
WORKDIR /opt/olist-etl-pipeline

# Set Python path so modules can be imported properly
ENV PYTHONPATH="/opt/olist-etl-pipeline:${PYTHONPATH}"
