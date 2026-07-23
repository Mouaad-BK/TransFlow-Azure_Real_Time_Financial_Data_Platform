"""
Role:
    Read transactions from PostgreSQL
    and publish them to Kafka.

Responsibilities:
    - Connect to PostgreSQL.
    - Read transactions.
    - Publish transactions to Kafka.
    - Simulate the realistic traffic.
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from producers.base_producer import BaseProducer
from config.traffic_config import get_traffic_delay


# Load environment variables.
load_dotenv(Path(__file__).resolve().parents[2] / ".env")


# PostgreSQL Kafka producer class that inherits from the base producer.
class PostgresProducer(BaseProducer):
    """
    Kafka producer for PostgreSQL transactions.
    """

    # Initialize the PostgreSQL producer.
    def __init__(self):
        super().__init__()

        self.connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

        self.cursor = self.connection.cursor()

        # Get a random starting transaction ID.
        self.cursor.execute("""
            SELECT transaction_id
            FROM transactions
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        self.current_transaction_id = self.cursor.fetchone()[0]

    # Read one transaction from PostgreSQL.
    def read_transaction(self):

        query = """
            SELECT
                t.transaction_id,

                c.customer_id,
                c.age,
                c.gender,
                c.annual_income,
                c.credit_score,
                c.number_of_cards,

                p.card_id,
                p.card_brand,
                p.card_type,
                p.has_chip,
                p.credit_limit,

                t.amount,
                t.card_usage_method,
                t.merchant_id,
                t.merchant_city,
                t.merchant_state,
                t.postal_code,
                t.merchant_category_code,
                t.transaction_error

            FROM transactions AS t

            INNER JOIN customers AS c
                ON t.customer_id = c.customer_id

            INNER JOIN payment_cards AS p
                ON t.card_id = p.card_id

            WHERE t.transaction_id >= %s

            ORDER BY t.transaction_id

            LIMIT 1;
        """

        self.cursor.execute(query, (self.current_transaction_id,))

        transaction = self.cursor.fetchone()

        # Restart from a random transaction when the end is reached.
        if transaction is None:

            self.cursor.execute("""
                SELECT transaction_id
                FROM transactions
                ORDER BY RANDOM()
                LIMIT 1;
            """)

            self.current_transaction_id = self.cursor.fetchone()[0]

            return self.read_transaction()

        # Prepare the next transaction.
        self.current_transaction_id = transaction[0] + 1

        return self.build_event(transaction)

    # Build a transaction event.
    def build_event(self, transaction):

        return {
            "customer": {
                "customer_id": transaction[1],
                "age": transaction[2],
                "gender": transaction[3],
                "annual_income": f"{transaction[4]:.2f}",
                "credit_score": transaction[5],
                "number_of_cards": transaction[6]
            },

            "card": {
                "card_id": transaction[7],
                "card_brand": transaction[8],
                "card_type": transaction[9],
                "has_chip": transaction[10],
                "credit_limit": f"{transaction[11]:.2f}"
            },

            "transaction": {
                "transaction_id": transaction[0],
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "amount": f"{transaction[12]:.2f}",
                "card_usage_method": transaction[13],
                "merchant_id": transaction[14],
                "merchant_city": transaction[15],
                "merchant_state": transaction[16],
                "postal_code": transaction[17],
                "merchant_category_code": transaction[18],
                "transaction_error": transaction[19]
            }
        }

    # Start the PostgreSQL producer.
    def run(self):

        while True:

            event = self.read_transaction()

            self.send(
                topic="transactions.postgresql",
                key=str(event["transaction"]["transaction_id"]),
                value=json.dumps(event)
            )

            # Ensure the message is delivered.
            self.flush()

            # Wait before publishing the next transaction.
            time.sleep(get_traffic_delay("postgres"))


if __name__ == "__main__":

    producer = PostgresProducer()

    producer.run()