import os
import json
import time

from azure.storage.blob import BlobServiceClient
from prometheus_client import start_http_server, Gauge


# ============================================================
# Configuration
# ============================================================

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

CONTAINER = "transactions"
DELTA_LOG_PATH = "silver/_delta_log/"

PORT = 9103

rows_processed = Gauge(
    "databricks_silver_rows_processed_total",
    "Total number of transactions written to Silver by Databricks"
)


# ============================================================
# Read Delta Log
# ============================================================

def get_rows_processed():

    if not CONNECTION_STRING:
        print("ERROR: AZURE_STORAGE_CONNECTION_STRING not found")
        return 0

    try:
        blob_service = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )

        container_client = blob_service.get_container_client(
            CONTAINER
        )

        total_rows = 0

        blobs = container_client.list_blobs(
            name_starts_with=DELTA_LOG_PATH
        )

        for blob in blobs:

            if not blob.name.endswith(".json"):
                continue

            blob_client = container_client.get_blob_client(
                blob.name
            )

            content = blob_client.download_blob().readall().decode("utf-8")

            for line in content.splitlines():

                if not line.strip():
                    continue

                record = json.loads(line)

                commit_info = record.get("commitInfo")

                if commit_info:

                    metrics = commit_info.get(
                        "operationMetrics",
                        {}
                    )

                    total_rows += int(
                        metrics.get("numOutputRows", 0)
                    )

        return total_rows

    except Exception as e:

        print("DATABRICKS EXPORTER ERROR:", e)

        return 0


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    start_http_server(PORT)

    print(
        f"Databricks exporter running on port {PORT}"
    )

    while True:

        total = get_rows_processed()

        rows_processed.set(total)

        print(
            f"Databricks Silver rows processed: {total}"
        )

        time.sleep(30)