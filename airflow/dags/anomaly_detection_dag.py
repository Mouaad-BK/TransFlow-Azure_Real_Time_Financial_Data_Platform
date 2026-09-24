from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests


FUNCTION_URL = (
    "https://function-transactions-hmb7e2gxcxfggfdj."
    "spaincentral-01.azurewebsites.net/api/anomaly_detection"
)


def execute_anomaly_detection():

    response = requests.get(
        FUNCTION_URL,
        timeout=300
    )

    response.raise_for_status()


with DAG(
    dag_id="anomaly_detection_dag",
    start_date=datetime(2026, 9, 23),
    schedule="@hourly",
    catchup=False,
    tags=["anomaly_detection"],
    default_args={
        "owner": "mouaad",
    },
) as dag:

    execute_anomaly_detection_task = PythonOperator(
        task_id="execute_anomaly_detection",
        python_callable=execute_anomaly_detection,
    )