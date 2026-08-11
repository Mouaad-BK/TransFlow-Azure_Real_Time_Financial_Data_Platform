import os
import json

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


# Load environment variables from .env
load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
AZURE_BRONZE_PATH = os.getenv("AZURE_BRONZE_PATH")


class BronzeWriter:

    def __init__(self):
        # Create the Azure Storage account URL
        account_url = (
            f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        )

        # Connect to Azure Storage
        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=AZURE_STORAGE_ACCOUNT_KEY
        )

        # Get the transactions container
        self.container_client = (
            self.blob_service_client
            .get_container_client(AZURE_CONTAINER_NAME)
        )

    def write(self, event):
        # Convert the event to JSON
        data = json.dumps(event, ensure_ascii=False)

        # Get the transaction ID
        transaction_id = event["transaction"]["transaction_id"]

        # Create the blob path
        blob_name = f"{AZURE_BRONZE_PATH}/{transaction_id}.json"

        # Write the transaction to Bronze
        self.container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=False
        )

        # Display a confirmation message
        print(f"Transaction {transaction_id} written to Bronze")