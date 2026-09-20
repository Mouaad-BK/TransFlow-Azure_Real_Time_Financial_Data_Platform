import time
import pyodbc
from prometheus_client import start_http_server, Gauge

SERVER = "synapse-transactions-ondemand.sql.azuresynapse.net"
DATABASE = "transactions_analytics"
USERNAME = "synapseadmin"
PASSWORD = "#SynapseTransaction."

PORT = 9102

connection_status = Gauge(
    "powerbi_synapse_connection",
    "Power BI to Synapse connectivity status"
)

def check_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;",
            timeout=10
        )

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()

        cursor.close()
        conn.close()

        connection_status.set(1)

    except Exception as e:
        print("SYNAPSE ERROR:", e)
        connection_status.set(0)

if __name__ == "__main__":
    start_http_server(PORT)

    while True:
        check_connection()
        time.sleep(30)