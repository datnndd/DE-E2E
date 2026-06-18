from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="orchestration_healthcheck",
    description="Minimal DAG to verify Airflow orchestration container is working.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orchestration", "healthcheck"],
)
def orchestration_healthcheck():
    # Healthcheck task: proves scheduler, worker process, and task runtime work.
    @task(retries=1)
    def ping() -> str:
        """Return a small success message for Airflow runtime checks."""
        return "airflow orchestration ready"

    ping()


orchestration_healthcheck()
