"""
========================================================================
Role:
    Centralized Kafka Consumer configuration.

Purpose:
    - Store the common configuration shared by all consumers.
    - Ensure consistent and reliable message consumption.

Note:
    Any configuration change is applied once and automatically shared
    by every consumer.
========================================================================
"""

from config.settings import KAFKA_BOOTSTRAP_SERVERS

consumer_config = {
    # Kafka Broker
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    # Consumer Group
    "group.id": "financial-platform",

    # Read new messages only
    "auto.offset.reset": "latest",

    # Automatic offset commit
    "enable.auto.commit": True
}