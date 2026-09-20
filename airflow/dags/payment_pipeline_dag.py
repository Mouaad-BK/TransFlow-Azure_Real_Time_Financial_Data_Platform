from datetime import datetime

import requests

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook


default_args = {
    "owner": "mouaad",
}


def resume_databricks_job():
    job_id = 827146690755434

    # Get Databricks connection from Airflow
    conn = BaseHook.get_connection("databricks_default")

    host = conn.host.rstrip("/")

    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"

    token = conn.password

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Get current Job settings
    get_url = f"{host}/api/2.2/jobs/get"

    response = requests.get(
        get_url,
        headers=headers,
        params={"job_id": job_id},
        timeout=30,
    )

    response.raise_for_status()

    job = response.json()
    schedule = job["settings"]["schedule"]

    print("Current Databricks schedule:")
    print(schedule)

    # Resume the existing Job schedule
    update_url = f"{host}/api/2.2/jobs/update"

    payload = {
        "job_id": job_id,
        "new_settings": {
            "schedule": {
                "quartz_cron_expression": schedule["quartz_cron_expression"],
                "timezone_id": schedule["timezone_id"],
                "pause_status": "UNPAUSED",
            }
        },
    }

    response = requests.post(
        update_url,
        headers=headers,
        json=payload,
        timeout=30,
    )

    response.raise_for_status()

    print(
        f"Databricks Job {job_id} schedule resumed successfully."
    )


with DAG(
    dag_id="payment_pipeline",
    start_date=datetime(2026, 9, 18),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    # Start all Kafka processes
    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command="""
        cd /opt/airflow/kafka

        mkdir -p /tmp/payment_pipeline_logs

        setsid bash -c '
            cd /opt/airflow/kafka
            exec python -m consumers.postgres_consumer
        ' > /tmp/payment_pipeline_logs/postgres_consumer.log 2>&1 < /dev/null &

        setsid bash -c '
            cd /opt/airflow/kafka
            exec python -m consumers.stripe_consumer
        ' > /tmp/payment_pipeline_logs/stripe_consumer.log 2>&1 < /dev/null &

        setsid bash -c '
            cd /opt/airflow/kafka
            exec python -m producers.postgres_producer
        ' > /tmp/payment_pipeline_logs/postgres_producer.log 2>&1 < /dev/null &

        setsid bash -c '
            cd /opt/airflow/kafka
            exec python -m producers.stripe_producer
        ' > /tmp/payment_pipeline_logs/stripe_producer.log 2>&1 < /dev/null &

        echo "Kafka producers and consumers started."

        sleep 15
        """,
    )

    # Resume the existing Databricks Job schedule
    resume_databricks = PythonOperator(
        task_id="resume_databricks",
        python_callable=resume_databricks_job,
    )

    start_kafka >> resume_databricks