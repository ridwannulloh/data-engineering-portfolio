FROM apache/airflow:2.9.3-python3.11

# Spark needs a JVM; the slim Airflow base image ships without one.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-jvm
ENV JAVA_HOME=/usr/lib/jvm/default-jvm

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Resolve the Delta Lake + S3A (hadoop-aws) jars into the Ivy cache at BUILD
# time so the first DAG run never touches Maven — keeps the live demo fast
# and safe to run without internet access.
COPY scripts/warm_spark_jars.py /tmp/warm_spark_jars.py
RUN python /tmp/warm_spark_jars.py
