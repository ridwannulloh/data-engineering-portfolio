"""Create Kafka topics for LogiStream. Run once after docker-compose up."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import settings
from ingest.topics import TOPICS

admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
new_topics = [
    NewTopic(
        name=t.name,
        num_partitions=t.num_partitions,
        replication_factor=t.replication_factor,
    )
    for t in TOPICS
]
try:
    admin.create_topics(new_topics)
    print(f"Created {len(new_topics)} topics:")
    for t in TOPICS:
        print(f"  {t.name} ({t.num_partitions} partitions) — {t.description}")
except TopicAlreadyExistsError:
    print("Topics already exist — skipping.")
finally:
    admin.close()
