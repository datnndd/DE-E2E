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

MEDIA_CONTENT_TYPES = {
    "video": "video/mp4",
    "image": "image/jpeg",
    "cover": "image/jpeg",
    "avatar": "image/jpeg",
}

MEDIA_EXTENSIONS = {
    "video": "mp4",
    "image": "jpeg",
    "cover": "jpeg",
    "avatar": "jpeg",
}


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

    def first_url(url_list: list | None) -> str | None:
        if isinstance(url_list, list) and url_list:
            return url_list[0]
        return None

    def extract_aweme_list(raw_response: dict) -> list[dict]:
        raw = raw_response.get("raw") or {}
        if isinstance(raw.get("aweme_list"), list):
            return raw["aweme_list"]
        if isinstance(raw.get("aweme_detail"), dict):
            return [raw["aweme_detail"]]
        return []

    def extract_media_tasks(aweme: dict) -> list[dict]:
        tasks = []
        aweme_id = str(aweme.get("aweme_id") or aweme.get("group_id") or "unknown_aweme")

        video_url = first_url(aweme.get("video", {}).get("play_addr", {}).get("url_list", []))
        if video_url:
            tasks.append({"media_type": "video", "index": 0, "aweme_id": aweme_id, "url": video_url})

        for index, image in enumerate(aweme.get("images") or []):
            image_url = first_url(image.get("url_list", []))
            if image_url:
                tasks.append({"media_type": "image", "index": index, "aweme_id": aweme_id, "url": image_url})

        cover_url = first_url(aweme.get("video", {}).get("cover", {}).get("url_list", []))
        if cover_url:
            tasks.append({"media_type": "cover", "index": 0, "aweme_id": aweme_id, "url": cover_url})

        avatar_url = first_url(aweme.get("author", {}).get("avatar", {}).get("url_list", []))
        if avatar_url:
            tasks.append({"media_type": "avatar", "index": 0, "aweme_id": aweme_id, "url": avatar_url})

        return tasks

    def download_bytes(url: str) -> tuple[bytes, str | None]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
        }
        req = request.Request(url, headers=headers, method="GET")
        with request.urlopen(req, timeout=180) as response:
            return response.read(), response.headers.get("Content-Type")

    def attach_media_s3_url(aweme: dict, media: dict, s3_url: str, key: str) -> None:
        media_type = media["media_type"]
        record = {
            "media_type": media_type,
            "index": media["index"],
            "s3_url": s3_url,
            "s3_key": key,
            "source_url": media["url"],
        }
        aweme.setdefault("s3_media", []).append(record)

        if media_type == "video":
            aweme.setdefault("video", {}).setdefault("play_addr", {})["s3_url"] = s3_url
        elif media_type == "image":
            images = aweme.get("images") or []
            if media["index"] < len(images):
                images[media["index"]]["s3_url"] = s3_url
        elif media_type == "cover":
            aweme.setdefault("video", {}).setdefault("cover", {})["s3_url"] = s3_url
        elif media_type == "avatar":
            aweme.setdefault("author", {}).setdefault("avatar", {})["s3_url"] = s3_url

    def upload_aweme_media(client, bucket: str, account: dict, raw_response: dict, now: datetime) -> list[dict]:
        if os.getenv("DOUYIN_DOWNLOAD_MEDIA", "true").lower() not in {"1", "true", "yes"}:
            logger.info("Media download disabled by DOUYIN_DOWNLOAD_MEDIA")
            return []

        media_prefix = os.getenv("S3_MEDIA_PREFIX", "lakehouse/landing/douyin/media_raw").strip().strip("/")
        uploaded = []
        awemes = extract_aweme_list(raw_response)
        logger.info("Preparing media download for %s awemes", len(awemes))

        for aweme in awemes:
            for media in extract_media_tasks(aweme):
                media_type = media["media_type"]
                extension = MEDIA_EXTENSIONS.get(media_type, "bin")
                key = (
                    f"{media_prefix}/niche={account['niche']}/account_id={account['account_id']}/"
                    f"aweme_id={media['aweme_id']}/media_type={media_type}/"
                    f"year={now:%Y}/month={now:%m}/day={now:%d}/"
                    f"{media_type}_{media['index']}.{extension}"
                )
                try:
                    logger.info("Downloading %s for aweme_id=%s", media_type, media["aweme_id"])
                    body, response_content_type = download_bytes(media["url"])
                    content_type = response_content_type or MEDIA_CONTENT_TYPES.get(media_type, "application/octet-stream")
                    logger.info("Uploading media to s3://%s/%s bytes=%s", bucket, key, len(body))
                    client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=body,
                        ContentType=content_type,
                        Metadata={
                            "zone": "landing",
                            "source": "douyin",
                            "niche": account["niche"],
                            "account_id": account["account_id"],
                            "aweme_id": media["aweme_id"],
                            "media_type": media_type,
                        },
                    )
                    s3_url = f"s3://{bucket}/{key}"
                    attach_media_s3_url(aweme, media, s3_url, key)
                    uploaded.append({"aweme_id": media["aweme_id"], "media_type": media_type, "s3_url": s3_url, "bytes": len(body)})
                except Exception as exc:
                    logger.warning("Media upload failed media_type=%s aweme_id=%s error=%s", media_type, media["aweme_id"], exc)
                    aweme.setdefault("s3_media_errors", []).append(
                        {"media_type": media_type, "index": media["index"], "source_url": media["url"], "error": str(exc)}
                    )

        logger.info("Uploaded %s media objects", len(uploaded))
        return uploaded

    def write_landing(account: dict, raw_response: dict) -> dict:
        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S%f")
        prefix = os.getenv("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json").strip().strip("/")
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        media_uploads = upload_aweme_media(client, bucket, account, raw_response, now)
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
            "media_uploads": media_uploads,
            "raw": raw_response,
        }
        body = json.dumps(landing_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
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
        return {"account_id": account["account_id"], "bucket": bucket, "key": key, "uri": f"s3://{bucket}/{key}", "bytes": len(body), "media_count": len(media_uploads)}

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