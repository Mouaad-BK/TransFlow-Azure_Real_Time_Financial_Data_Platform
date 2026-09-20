from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
import json
import threading
import time


PORT = 9101
DAG_ID = "payment_pipeline"
TOTAL_TASKS = 5

# Cache des métriques
metrics = {
    "dag_enabled": 0,
    "tasks_running": 0,
    "tasks_total": TOTAL_TASKS,
}

metrics_lock = threading.Lock()


def run_airflow_command(command):
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=20,
        )

        if result.returncode != 0:
            return None

        return result.stdout

    except Exception:
        return None


def collect_metrics():

    # DAG enabled / paused
    output = run_airflow_command(
        [
            "airflow",
            "dags",
            "list",
            "--output",
            "json",
        ]
    )

    dag_enabled = 0

    if output:
        try:
            dags = json.loads(output)

            for dag in dags:
                if dag["dag_id"] == DAG_ID:
                    dag_enabled = (
                        0
                        if str(dag["is_paused"]).lower() == "true"
                        else 1
                    )
                    break

        except Exception:
            pass

    # Latest DAG run
    tasks_running = 0

    output = run_airflow_command(
        [
            "airflow",
            "dags",
            "list-runs",
            DAG_ID,
            "--output",
            "json",
        ]
    )

    if output:

        try:
            runs = json.loads(output)

            if runs:

                latest_run = runs[0]
                run_id = latest_run.get("run_id")

                if run_id:

                    output = run_airflow_command(
                        [
                            "airflow",
                            "tasks",
                            "states-for-dag-run",
                            DAG_ID,
                            run_id,
                            "--output",
                            "json",
                        ]
                    )

                    if output:

                        tasks = json.loads(output)

                        tasks_running = sum(
                            1
                            for task in tasks
                            if str(task.get("state", "")).lower()
                            in ["success", "running"]
                        )

        except Exception:
            pass

    # Update cache
    with metrics_lock:
        metrics["dag_enabled"] = dag_enabled
        metrics["tasks_running"] = tasks_running
        metrics["tasks_total"] = TOTAL_TASKS


def background_collector():

    while True:

        try:
            collect_metrics()
        except Exception:
            pass

        # Mise à jour toutes les 15 secondes
        time.sleep(15)


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        with metrics_lock:
            dag_enabled = metrics["dag_enabled"]
            tasks_running = metrics["tasks_running"]
            tasks_total = metrics["tasks_total"]

        response = (
            "# HELP airflow_payment_pipeline_enabled "
            "Whether the payment_pipeline DAG is enabled\n"
            "# TYPE airflow_payment_pipeline_enabled gauge\n"
            f"airflow_payment_pipeline_enabled {dag_enabled}\n"

            "# HELP airflow_payment_pipeline_tasks_running "
            "Number of payment_pipeline tasks in success or running state\n"
            "# TYPE airflow_payment_pipeline_tasks_running gauge\n"
            f"airflow_payment_pipeline_tasks_running {tasks_running}\n"

            "# HELP airflow_payment_pipeline_tasks_total "
            "Total number of tasks in payment_pipeline\n"
            "# TYPE airflow_payment_pipeline_tasks_total gauge\n"
            f"airflow_payment_pipeline_tasks_total {tasks_total}\n"
        )

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(response.encode())))
        self.end_headers()

        self.wfile.write(response.encode())

    def log_message(self, format, *args):
        pass


# Première collecte avant de démarrer le serveur
print("Collecting initial Airflow metrics...")
collect_metrics()

# Thread de collecte en arrière-plan
collector_thread = threading.Thread(
    target=background_collector,
    daemon=True,
)

collector_thread.start()

print(f"Airflow exporter running on port {PORT}")

server = HTTPServer(("0.0.0.0", PORT), MetricsHandler)

server.serve_forever()