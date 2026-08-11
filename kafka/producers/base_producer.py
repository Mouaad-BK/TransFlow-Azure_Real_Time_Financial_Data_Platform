"""
Role:
    Provide a reusable Kafka producer.

Responsibilities:
    - Connect to the Kafka cluster.
    - Publish messages to Kafka topics.
    - Handle message delivery.
    - Flush pending messages before shutdown.

This class serves as the parent producer for all data sources
(e.g., PostgreSQL, Stripe API).
"""

from confluent_kafka import Producer
from config.producer_config import producer_config


# Base class for all Kafka producers.
class BaseProducer:

    # Initialize the Kafka producer.
    def __init__(self):
        self.producer = Producer(producer_config)

    # Send a message to a Kafka topic.
    def send(self, topic, key, value):

        self.producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=self.delivery_report
        )

    # Handle the message delivery status.
    def delivery_report(self, err, msg):

        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print("Delivering messages...")

    # Send all pending messages before shutdown.
    def flush(self):

        self.producer.flush()