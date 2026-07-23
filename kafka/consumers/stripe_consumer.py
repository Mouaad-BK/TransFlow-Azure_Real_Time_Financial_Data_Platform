"""
Role:
    Read transactions from the Stripe API Kafka topic.

Responsibilities:
    - Subscribe to the Stripe API Kafka topic.
    - Read transaction events.
    - Deserialize JSON messages.
    - Prepare events for the Bronze layer.
"""

import json

from consumers.base_consumer import BaseConsumer

# Stripe consumer class that inherits from the base consumer.
class StripeConsumer(BaseConsumer):

    # Initialize the Stripe consumer.
    def __init__(self):
        super().__init__()

        # Subscribe to the Stripe API Kafka topic.
        self.subscribe(["transactions.stripe"])

    # Read one transaction event from the Stripe API Kafka topic.
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
        while True:
            event = self.read_event()

            if event is None:
              continue

            # Send the event to the Bronze layer.
            # bronze_writer.write(event)

if __name__ == "__main__":

    consumer = StripeConsumer()

    try:
        consumer.run()

    finally:
        consumer.close()

