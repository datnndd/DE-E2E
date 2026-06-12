from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib import request

import boto3
import yaml
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)


@dag(
    dag_id="crawl_douyin_seed_to_s3_landing",
    description="Read Douyin food seed file, crawl each account, and write raw API JSON to S3 Landing.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["douyin", "food", "seed", "s3", "landing"],
)
def crawl_douyin_seed_to_s3_landing():
    def load_seed_accounts() -> list[dict]:
        seed_file = Path(os.getenv("DOUYIN_SEED_FILE", "/opt/airflow/seeds/douyin_food_restaurant_seeds.yml"))
        logger.info("Loading seed file: %s", seed_file)
        if not seed_file.exists():
            raise FileNotFoundError(f"Seed file not found: {seed_file}")

        seed_config = yaml.safe_load(seed_file.read_text(encoding="utf-8-sig"))
        crawl_config = seed_config.get("crawl_config") or {}
        niche = seed_config.get("niche", "unknown_niche")
        accounts = seed_config.get("seed_accounts") or []
        if not accounts:
            raise AirflowSkipException("Seed file has no seed_accounts")

        seed_accounts = [
            {
                "niche": niche,
                "account_id": account["id"],
                "account_type": account.get("type", "unknown"),
                "link": account["link"],
                "mode": crawl_config.get("mode", "post"),
                "limit": int(crawl_config.get("limit", 20)),
                "start_time": crawl_config.get("start_time", ""),
                "end_time": crawl_config.get("end_time", ""),
            }
            for account in accounts
        ]
        logger.info("Loaded %s seed accounts for niche=%s", len(seed_accounts), niche)
        return seed_accounts

    def crawl_account(account: dict) -> dict:
        worker_url = os.getenv("INGESTION_WORKER_URL", "http://ingestion-worker:8000").rstrip("/")
        payload = json.dumps(
            {
                "link": account["link"],
                "mode": account["mode"],
                "limit": account["limit"],
                "start_time": account["start_time"],
                "end_time": account["end_time"],
            }
        ).encode("utf-8")
        req = request.Request(
            f"{worker_url}/douyin/fetch",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        logger.info("Calling ingestion-worker for account_id=%s", account["account_id"])
        with request.urlopen(req, timeout=300) as response:
            return json.loads(response.read().decode("utf-8"))

    def write_landing(account: dict, raw_response: dict) -> dict:
        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S%f")
        prefix = os.getenv("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json").strip().strip("/")
        key = (
            f"{prefix}/niche={account['niche']}/account_id={account['account_id']}/"
            f"year={now:%Y}/month={now:%m}/day={now:%d}/{run_id}.json"
        )
        landing_payload = {
            "pipeline": "de-e2e",
            "source": "douyin",
            "zone": "landing",
            "niche": account["niche"],
            "account_id": account["account_id"],
            "account_type": account["account_type"],
            "link": account["link"],
            "crawl_config": {
                "mode": account["mode"],
                "limit": account["limit"],
                "start_time": account["start_time"],
                "end_time": account["end_time"],
            },
            "generated_at": now.isoformat(),
            "raw": raw_response,
        }
        body = json.dumps(landing_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        logger.info("Writing landing JSON to s3://%s/%s", bucket, key)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            Metadata={
                "zone": "landing",
                "source": "douyin",
                "niche": account["niche"],
                "account_id": account["account_id"],
            },
        )
        return {"account_id": account["account_id"], "bucket": bucket, "key": key, "uri": f"s3://{bucket}/{key}", "bytes": len(body)}

    @task(retries=0)
    def crawl_seed_accounts() -> list[dict]:
        results = []
        for account in load_seed_accounts():
            logger.info("Processing account_id=%s link=%s", account["account_id"], account["link"])
            raw_response = crawl_account(account)
            results.append(write_landing(account, raw_response))
        return results

    crawl_seed_accounts()


crawl_douyin_seed_to_s3_landing()