"""
Role:
    Create Kafka topics automatically at cluster initialization.
    Existing topics are ignored.
"""

from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_BOOTSTRAP_SERVERS
from topics.topic_config import TOPICS

# kafka admin client
admin = AdminClient({
    "bootstrap.servers" : KAFKA_BOOTSTRAP_SERVERS
})

# Create Topics

existing_topics = admin.list_topics(timeout=10).topics

new_topics = []

for topic in TOPICS:
    if topic["name"] not in existing_topics :
        new_topics.append(
            NewTopic(
                topic = topic["name"],
                num_partitions = topic["partitions"],
                replication_factor = topic["replication_factor"]
            )
        )

if new_topics:
    admin.create_topics(new_topics)
    print("Topics created successfully.")

else :
    print("All topics already exist. No new topics created.")