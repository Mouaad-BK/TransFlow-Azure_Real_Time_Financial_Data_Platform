"""
Role:
    Read transactions from Stripe API
    and publish them to Kafka.

Responsibilities:
    - Connect to Stripe API.
    - Read transactions.
    - Publish transactions to Kafka.
    - Simulate the realistic traffic.
"""

import sys
from pathlib import Path

# Add the project root to Python path.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import json
import time
from datetime import datetime

from producers.base_producer import BaseProducer
from config.traffic_config import get_traffic_delay
from data_source.api.sandbox import extract


# Stripe API Kafka producer class that inherits from the base producer.
class APIProducer(BaseProducer):
    """
    Kafka producer for Stripe API transactions.
    """

    # Initialize the Stripe API producer.
    def __init__(self):
        super().__init__()

    # Read one transaction from Stripe API.
    def read_transaction(self):

        transaction = extract()

        return self.build_event(transaction)

    # Build a transaction event.
    def build_event(self, transaction):

        event = {
            "customer": {
                "customer_id": transaction["customer"]["customer_id"],
                "age": transaction["customer"]["age"],
                "gender": transaction["customer"]["gender"],
                "annual_income": f"{transaction["customer"]["annual_income"]:.2f}",
                "credit_score": transaction["customer"]["credit_score"],
                "number_of_cards": transaction["customer"]["number_of_cards"]
            },

            "card": {
                "card_id": transaction["card"]["card_id"],
                "card_brand": transaction["card"]["card_brand"],
                "card_type": transaction["card"]["card_type"],
                "has_chip": transaction["card"]["has_chip"],
                "credit_limit": f"{transaction["card"]["card_limit"]:.2f}"
            },

            "transaction": {
                "transaction_id": transaction["transaction"]["transaction_id"],
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "amount": f"{transaction["transaction"]["amount"]:.2f}",
                "card_usage_method": transaction["transaction"]["card_usage_method"],
                "merchant_id": transaction["transaction"]["merchant_id"],
                "merchant_city": transaction["transaction"]["merchant_city"],
                "merchant_state": transaction["transaction"]["merchant_state"],
                "postal_code": transaction["transaction"]["postal_code"],
                "merchant_category_code": transaction["transaction"]["merchant_category_code"],
                "transaction_error": transaction["transaction"]["transaction_error"]
            }
        }

        return event

    # Start the Stripe API producer.
    def run(self):

        while True:

            event = self.read_transaction()

            self.send(
                topic="transactions.stripe",
                key=str(event["transaction"]["transaction_id"]),
                value=json.dumps(event)
            )

            # Ensure the message is delivered.
            self.flush()

            # Wait before publishing the next transaction.
            time.sleep(get_traffic_delay("stripe"))


if __name__ == "__main__":

    producer = APIProducer()

    producer.run()