"""
Role:
    Read transactions from the PostgreSQL Kafka topic.

Responsibilities:
    - Subscribe to the PostgreSQL Kafka topic.
    - Read transaction events.
    - Deserialize JSON messages.
    - Prepare events for the Bronze layer.
"""

import json
import sys

from consumers.base_consumer import BaseConsumer

# Add project root to the Python path
sys.path.insert(0, "..")

from azure.adls.bronze_writer import BronzeWriter

# PostgreSQL consumer class that inherits from the base consumer.
class PostgresConsumer(BaseConsumer):

    # Initialize the Postgres consumer.
    def __init__(self):
        super().__init__()

        # Subscribe to the PostgreSQL Kafka topic.
        self.subscribe(["transactions.postgresql"])

    # Read one transaction event from the PostgreSQL Kafka topic.
    def read_event(self):

        # Poll for a message from the Kafka topic.
        message = self.poll()

        # If no message is received, return None.
        if message is None:
            return None

        # If an error occurs, raise an exception.
        if message.error():
            print(message.error())
            return None

        # Deserialize the JSON message into a Python dictionary.
        event = json.loads(message.value().decode("utf-8"))

        return event

    # test with loop
    def run(self):

        bronze_writer = BronzeWriter()
        while True:
            event = self.read_event()

            if event is None:
              continue

            # Send the event to the Bronze layer.
            bronze_writer.write(event)

if __name__ == "__main__":

    consumer = PostgresConsumer()

    try:
        consumer.run()

    finally:
        consumer.close()
