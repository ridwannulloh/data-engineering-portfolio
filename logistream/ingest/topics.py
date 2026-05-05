"""
Kafka topic definitions and partition configuration for LogiStream.

Imported by scripts/create_topics.py and used as the single source
of truth for topic names and partition counts.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicConfig:
    name:               str
    num_partitions:     int
    replication_factor: int
    description:        str


# shipment-events carries the highest volume — 6 partitions for parallelism.
# carrier-updates and warehouse-ops are lower-frequency supplementary feeds.
TOPICS: list[TopicConfig] = [
    TopicConfig(
        name="shipment-events",
        num_partitions=6,
        replication_factor=1,
        description="Shipment lifecycle events (PICKUP → DELIVERED)",
    ),
    TopicConfig(
        name="carrier-updates",
        num_partitions=2,
        replication_factor=1,
        description="Carrier performance status reports",
    ),
    TopicConfig(
        name="warehouse-ops",
        num_partitions=3,
        replication_factor=1,
        description="Warehouse inbound/outbound scan operations",
    ),
]

TOPIC_NAMES = {t.name for t in TOPICS}
