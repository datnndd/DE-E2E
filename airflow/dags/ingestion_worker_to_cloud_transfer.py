from __future__ import annotations

import os
from datetime import datetime
from urllib import request
import json

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException


@dag(
    dag_id="ingestion_worker_to_cloud_transfer",
    description="Fetch JSON from ingestion-worker and forward it to a configured cloud ingest endpoint.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orchestration", "api", "cloud"],
)
def ingestion_worker_to_cloud_transfer():
    @task(retries=2)
    def fetch_payload() -> dict:
        api_url = os.getenv("INGESTION_WORKER_URL", "http://ingestion-worker:8000").rstrip("/")
        with request.urlopen(f"{api_url}/data", timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    @task(retries=2)
    def send_to_cloud(payload: dict) -> dict:
        cloud_ingest_url = os.getenv("CLOUD_INGEST_URL", "").strip()
        if not cloud_ingest_url:
            raise AirflowSkipException("CLOUD_INGEST_URL is not configured")

        headers = {"Content-Type": "application/json"}
        cloud_api_key = os.getenv("CLOUD_API_KEY", "").strip()
        if cloud_api_key:
            headers["Authorization"] = f"Bearer {cloud_api_key}"

        body = json.dumps(payload).encode("utf-8")
        req = request.Request(cloud_ingest_url, data=body, headers=headers, method="POST")
        with request.urlopen(req, timeout=60) as response:
            response_body = response.read().decode("utf-8")
            return {"status_code": response.status, "response": response_body}

    send_to_cloud(fetch_payload())


ingestion_worker_to_cloud_transfer()