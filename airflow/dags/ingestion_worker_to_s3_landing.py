from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from urllib import request

import boto3
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task


@dag(
    dag_id="ingestion_worker_to_s3_landing",
    description="Fetch original Douyin API JSON from ingestion-worker and write it to AWS S3 Landing layer.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orchestration", "douyin", "s3", "landing", "raw"],
)
def ingestion_worker_to_s3_landing():
    @task(retries=2)
    def fetch_payload() -> dict:
        worker_url = os.getenv("INGESTION_WORKER_URL", "http://ingestion-worker:8000").rstrip("/")
        with request.urlopen(f"{worker_url}/data", timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))

    @task(retries=2)
    def write_landing(payload: dict) -> dict:
        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        prefix = os.getenv("S3_LANDING_PREFIX", "landing/douyin/api_raw/json").strip().strip("/")
        now = datetime.now(timezone.utc)
        run_id = str(payload.get("run_id") or now.strftime("%Y%m%dT%H%M%S%f"))
        key = (
            f"{prefix}/year={now:%Y}/month={now:%m}/day={now:%d}/"
            f"{run_id}.json"
        )

        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            Metadata={"zone": "landing", "source": "douyin", "format": "json"},
        )

        return {"bucket": bucket, "key": key, "uri": f"s3://{bucket}/{key}", "bytes": len(body)}

    write_landing(fetch_payload())


ingestion_worker_to_s3_landing()