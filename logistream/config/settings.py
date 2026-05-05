"""Central configuration using Pydantic Settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_shipments: str = "shipment-events"
    kafka_topic_carriers: str = "carrier-updates"
    kafka_topic_warehouses: str = "warehouse-ops"
    kafka_consumer_group: str = "logistream-spark"

    # Delta Lake paths
    delta_base_path: str = "/tmp/logistream/delta"
    delta_bronze_path: str = "/tmp/logistream/delta/bronze"
    delta_silver_path: str = "/tmp/logistream/delta/silver"
    delta_gold_path: str = "/tmp/logistream/delta/gold"

    # Spark
    spark_checkpoint_path: str = "/tmp/logistream/checkpoints"
    spark_app_name: str = "LogiStream"
    spark_master: str = "local[*]"

    # SLA thresholds (hours)
    sla_standard_hours: int = 48
    sla_express_hours: int = 24
    sla_overnight_hours: int = 12

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000


settings = Settings()
