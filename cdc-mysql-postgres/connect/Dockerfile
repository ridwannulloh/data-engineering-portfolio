# ─────────────────────────────────────────────────────────────────────────
# Kafka Connect worker for the CDC pipeline.
#
# The base Debezium image already ships the MySQL *source* connector. On top
# of it we add:
#   • the Debezium JDBC *sink* connector  (writes change events to PostgreSQL)
#   • the PostgreSQL JDBC driver           (so the sink can talk to Postgres)
#
# Both go into /kafka/connect, the Debezium image's plugin path. Versions are
# pinned to the same Debezium release as the base image for compatibility.
# ─────────────────────────────────────────────────────────────────────────
FROM quay.io/debezium/connect:2.7.3.Final

ARG DEBEZIUM_VERSION=2.7.3.Final
ARG POSTGRES_DRIVER_VERSION=42.7.3
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Debezium JDBC sink connector (bundles Hibernate + its runtime deps).
RUN curl -fSL -o /tmp/jdbc.tar.gz \
      "${MAVEN}/io/debezium/debezium-connector-jdbc/${DEBEZIUM_VERSION}/debezium-connector-jdbc-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    && tar -xzf /tmp/jdbc.tar.gz -C /kafka/connect \
    && rm /tmp/jdbc.tar.gz

# PostgreSQL JDBC driver, placed alongside the JDBC sink plugin.
RUN curl -fSL -o /kafka/connect/debezium-connector-jdbc/postgresql-${POSTGRES_DRIVER_VERSION}.jar \
      "${MAVEN}/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"

USER kafka
