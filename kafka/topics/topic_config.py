"""
Role:
    Centralize the configuration of all Kafka topics.
    Each topic defines its name, number of partitions,
    and replication factor.
"""

# Create TOPICS ( Poatgres and Stripe ) with 3 partitions and replication factor of 3.
TOPICS = [
    {
        "name": "transactions.postgres",
        "partitions": 3,
        "replication_factor": 3
    },
    {
        "name": "transactions.stripe",
        "partitions": 3,
        "replication_factor": 3
    }
]