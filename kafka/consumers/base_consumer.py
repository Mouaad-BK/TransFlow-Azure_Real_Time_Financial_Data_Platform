"""
Role:
    Provide a reusable Kafka consumer.

Responsibilities:
    - Connect to the Kafka cluster.
    - Subscribe to Kafka topics.
    - Read messages from Kafka topics.
    - Close the consumer gracefully.

This class serves as the parent consumer for all data sources
(e.g., PostgreSQL, Stripe API).
"""

from confluent_kafka import Consumer
from config.consumer_config import consumer_config


# Base class for all Kafka consumers.
class BaseConsumer:

    # Initialize the Kafka consumer.
    def __init__(self):
        self.consumer = Consumer(consumer_config)

    # Subscribe to one or more Kafka topics.
    def subscribe(self, topics):
        self.consumer.subscribe(topics)

    # Read a message from Kafka.
    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout)

    # Close the Kafka consumer.
    def close(self):
        self.consumer.close()