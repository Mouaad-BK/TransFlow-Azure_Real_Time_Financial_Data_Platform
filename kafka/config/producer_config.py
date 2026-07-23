"""
========================================================================
Role:
    Centralized Kafka Producer configuration.

Purpose:
    - Store the common configuration shared by all producers.
    - Ensure consistent and reliable message publishing.

Note:
    Any configuration change is applied once and automatically shared
    by every producer.
========================================================================
"""

from config.settings import KAFKA_BOOTSTRAP_SERVERS

producer_config = {
    # Kafka Broker
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    # Reliability
    "acks": "all",
    "retries": 3,

    # Performance
    "compression.type": "snappy",
    "linger.ms": 10,
    "batch.size": 32768
}