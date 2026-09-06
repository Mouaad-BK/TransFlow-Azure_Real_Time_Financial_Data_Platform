"""
========================================================================
Role:
    Centralized application settings.

Purpose:
    - Load environment variables from the .env file.
    - Provide a single configuration source for the entire Kafka module.

Note:
    Updating the .env file automatically updates all modules without
    changing the application code.
========================================================================
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka
KAFKA_BROKER_ID = os.getenv("KAFKA_BROKER_ID")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_CONTROLLER_PORT = os.getenv("KAFKA_CONTROLLER_PORT")

# Schema Registry
SCHEMA_REGISTRY_PORT = os.getenv("SCHEMA_REGISTRY_PORT")

# Kafka UI
KAFKA_UI_PORT = os.getenv("KAFKA_UI_PORT")

# Docker
KAFKA_NETWORK = os.getenv("KAFKA_NETWORK")

# Python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")