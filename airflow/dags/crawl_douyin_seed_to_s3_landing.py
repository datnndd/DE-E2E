from __future__ import annotations

import csv
import hashlib
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
    def load_seed_targets() -> list[dict]:
        csv_file = discover_seed_csv_file()
        yaml_file = Path(os.getenv("DOUYIN_SEED_FILE", "/opt/airflow/seeds/douyin_food_restaurant_seeds.yml"))
        if csv_file:
            return load_seed_targets_from_csv(csv_file)
        return load_seed_targets_from_yaml(yaml_file)

    def discover_seed_csv_file() -> Path | None:
        explicit_file = os.getenv("DOUYIN_SEED_CSV_FILE", "").strip()
        if explicit_file:
            path = Path(explicit_file)
            if path.exists():
                logger.info("Using explicit seed CSV file: %s", path)
                return path
            logger.warning("Explicit seed CSV file does not exist, auto-discovery will be used: %s", path)

        seed_dir = Path(os.getenv("DOUYIN_SEED_CSV_DIR", "/opt/airflow/seeds"))
        pattern = os.getenv("DOUYIN_SEED_CSV_PATTERN", "*.csv").strip() or "*.csv"
        if not seed_dir.exists():
            logger.warning("Seed CSV directory does not exist: %s", seed_dir)
            return None

        candidates = [path for path in seed_dir.glob(pattern) if path.is_file() and not path.name.startswith(".")]
        if not candidates:
            logger.info("No seed CSV files found in %s with pattern %s", seed_dir, pattern)
            return None

        for candidate in sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True):
            if not seed_csv_was_processed(candidate):
                logger.info("Auto-discovered latest unprocessed seed CSV file: %s", candidate)
                return candidate

        raise AirflowSkipException("All discovered seed CSV files were already processed successfully")

    def seed_file_hash(seed_file: Path) -> str:
        digest = hashlib.sha256()
        with seed_file.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def seed_manifest_key(file_hash: str) -> str:
        prefix = os.getenv("S3_CONTROL_PREFIX", "lakehouse/control/douyin/processed_seed_files").strip().strip("/")
        return f"{prefix}/{file_hash}.json"

    def seed_csv_was_processed(seed_file: Path) -> bool:
        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            logger.warning("S3_BUCKET is not configured; seed CSV processed-state check is disabled")
            return False

        file_hash = seed_file_hash(seed_file)
        key = seed_manifest_key(file_hash)
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        try:
            client.head_object(Bucket=bucket, Key=key)
            logger.info("Seed CSV already processed successfully: file=%s hash=%s", seed_file.name, file_hash)
            return True
        except client.exceptions.ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise


    def slugify(value: str) -> str:
        slug = "".join(char.lower() if char.isalnum() else "_" for char in value.strip())
        return "_".join(part for part in slug.split("_") if part)

    def default_account_type(niche: str) -> str:
        slug = slugify(niche)
        if "food" in slug or "restaurant" in slug:
            return "food_creator"
        base = slug.removeprefix("douyin_").split("_")[0] or "creator"
        return f"{base}_creator"

    def load_seed_targets_from_csv(seed_file: Path) -> list[dict]:
        logger.info("Loading seed CSV file: %s", seed_file)
        file_hash = seed_file_hash(seed_file)
        targets = []
        with seed_file.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            missing_columns = {"niche", "link", "limit", "start_date"} - set(reader.fieldnames or [])
            if missing_columns:
                raise ValueError(f"Seed CSV missing required columns: {', '.join(sorted(missing_columns))}")
            enabled_index = 0
            for row_index, row in enumerate(reader, start=1):
                enabled = str(row.get("enabled", "true")).strip().lower()
                if enabled in {"0", "false", "no", "n"}:
                    continue
                enabled_index += 1
                niche = (row.get("niche") or "douyin_food_restaurant").strip()
                account_type = (row.get("account_type") or row.get("type") or default_account_type(niche)).strip()
                account_id = (row.get("account_id") or row.get("id") or f"{account_type}_{enabled_index:03d}").strip()
                link = (row.get("link") or "").strip()
                if not link:
                    logger.warning("Skipping CSV row %s because link is empty", row_index)
                    continue
                targets.append(
                    {
                        "niche": niche,
                        "account_id": account_id,
                        "account_type": account_type,
                        "link": link,
                        "mode": (row.get("mode") or "post").strip() or "post",
                        "limit": int((row.get("limit") or "20").strip()),
                        "start_time": (row.get("start_time") or row.get("start_date") or "").strip(),
                        "end_time": (row.get("end_time") or row.get("end_date") or "").strip(),
                        "seed_file_name": seed_file.name,
                        "seed_file_path": str(seed_file),
                        "seed_file_hash": file_hash,
                    }
                )
        if not targets:
            raise AirflowSkipException("Seed CSV file has no enabled rows with link")
        logger.info("Loaded %s seed targets from CSV", len(targets))
        return targets

    def load_seed_targets_from_yaml(seed_file: Path) -> list[dict]:
        logger.info("Loading seed YAML file: %s", seed_file)
        if not seed_file.exists():
            raise FileNotFoundError(f"Seed file not found: {seed_file}")

        seed_config = yaml.safe_load(seed_file.read_text(encoding="utf-8-sig"))
        crawl_config = seed_config.get("crawl_config") or {}
        niche = seed_config.get("niche", "unknown_niche")
        limit = int(crawl_config.get("limit", 20))
        start_time = crawl_config.get("start_time", "")
        end_time = crawl_config.get("end_time", "")

        targets = [
            {
                "niche": niche,
                "account_id": account["id"],
                "account_type": account.get("type", "unknown"),
                "link": account["link"],
                "mode": crawl_config.get("mode", "post"),
                "limit": limit,
                "start_time": start_time,
                "end_time": end_time,
            }
            for account in seed_config.get("seed_accounts") or []
        ]
        if not targets:
            raise AirflowSkipException("Seed YAML file has no seed_accounts")
        logger.info("Loaded %s seed targets for niche=%s", len(targets), niche)
        return targets

    def crawl_target(target: dict) -> dict:
        worker_url = os.getenv("INGESTION_WORKER_URL", "http://ingestion-worker:8000").rstrip("/")
        endpoint = f"{worker_url}/douyin/fetch"
        payload_dict = {
            "link": target["link"],
            "mode": target["mode"],
            "limit": target["limit"],
            "start_time": target["start_time"],
            "end_time": target["end_time"],
        }
        payload = json.dumps(payload_dict).encode("utf-8")
        req = request.Request(endpoint, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        logger.info("Calling ingestion-worker account_id=%s", target["account_id"])
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

    def build_media_key(media_prefix: str, account: dict, media: dict) -> str:
        media_type = media["media_type"]
        extension = MEDIA_EXTENSIONS.get(media_type, "bin")
        account_prefix = f"{media_prefix}/niche={account['niche']}/account_id={account['account_id']}/files"
        if media_type == "avatar":
            return f"{account_prefix}/user_avatar_{media['index']}.{extension}"
        return f"{account_prefix}/{media['aweme_id']}_{media_type}_{media['index']}.{extension}"

    def upload_aweme_media(client, bucket: str, account: dict, raw_response: dict) -> list[dict]:
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
                key = build_media_key(media_prefix, account, media)
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
                    uploaded.append(
                        {
                            "status": "uploaded",
                            "account_id": account["account_id"],
                            "aweme_id": media["aweme_id"],
                            "media_type": media_type,
                            "index": media["index"],
                            "s3_key": key,
                            "s3_url": s3_url,
                            "bytes": len(body),
                            "content_type": content_type,
                            "source_url": media["url"],
                        }
                    )
                except Exception as exc:
                    logger.warning("Media upload failed media_type=%s aweme_id=%s error=%s", media_type, media["aweme_id"], exc)
                    uploaded.append(
                        {
                            "status": "failed",
                            "account_id": account["account_id"],
                            "aweme_id": media["aweme_id"],
                            "media_type": media_type,
                            "index": media["index"],
                            "source_url": media["url"],
                            "error": str(exc),
                        }
                    )

        logger.info("Processed %s media objects", len(uploaded))
        return uploaded

    def write_media_manifest(client, bucket: str, account: dict, media_uploads: list[dict], now: datetime, run_id: str) -> dict | None:
        if not media_uploads:
            return None

        manifest_prefix = os.getenv("S3_MEDIA_MANIFEST_PREFIX", "lakehouse/landing/douyin/media_manifest/json").strip().strip("/")
        key = (
            f"{manifest_prefix}/niche={account['niche']}/account_id={account['account_id']}/"
            f"year={now:%Y}/month={now:%m}/day={now:%d}/{run_id}.json"
        )
        manifest_payload = {
            "pipeline": "de-e2e",
            "source": "douyin",
            "zone": "landing",
            "artifact": "media_manifest",
            "niche": account["niche"],
            "account_id": account["account_id"],
            "generated_at": now.isoformat(),
            "items": media_uploads,
        }
        body = json.dumps(manifest_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        logger.info("Writing media manifest to s3://%s/%s", bucket, key)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            Metadata={
                "zone": "landing",
                "source": "douyin",
                "artifact": "media_manifest",
                "niche": account["niche"],
                "account_id": account["account_id"],
            },
        )
        return {"bucket": bucket, "key": key, "uri": f"s3://{bucket}/{key}", "bytes": len(body), "media_count": len(media_uploads)}

    def write_landing(account: dict, raw_response: dict) -> dict:
        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S%f")
        prefix = os.getenv("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json").strip().strip("/")
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        media_uploads = upload_aweme_media(client, bucket, account, raw_response)
        media_manifest = write_media_manifest(client, bucket, account, media_uploads, now, run_id)
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
            "link": account.get("link", ""),
            "crawl_config": {
                "mode": account["mode"],
                "limit": account["limit"],
                "start_time": account["start_time"],
                "end_time": account["end_time"],
            },
            "generated_at": now.isoformat(),
            "media_manifest": media_manifest,
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
        return {
            "account_id": account["account_id"],
            "bucket": bucket,
            "key": key,
            "uri": f"s3://{bucket}/{key}",
            "bytes": len(body),
            "media_count": len(media_uploads),
            "seed_file_name": account.get("seed_file_name", ""),
            "seed_file_path": account.get("seed_file_path", ""),
            "seed_file_hash": account.get("seed_file_hash", ""),
        }

    @task(retries=0)
    def write_seed_processing_manifest(landing_results: list[dict]) -> list[dict]:
        if not landing_results:
            raise AirflowSkipException("No landing results to mark as processed")

        first = landing_results[0]
        file_hash = str(first.get("seed_file_hash") or "").strip()
        if not file_hash:
            logger.info("Landing results did not come from CSV; skipping seed processing manifest")
            return landing_results

        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        now = datetime.now(timezone.utc)
        key = seed_manifest_key(file_hash)
        manifest_payload = {
            "pipeline": "de-e2e",
            "source": "douyin",
            "artifact": "processed_seed_file",
            "status": "success",
            "seed_file_name": first.get("seed_file_name", ""),
            "seed_file_path": first.get("seed_file_path", ""),
            "seed_file_hash": file_hash,
            "processed_at": now.isoformat(),
            "landing_result_count": len(landing_results),
            "landing_uris": [result.get("uri") for result in landing_results if result.get("uri")],
        }
        body = json.dumps(manifest_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION") or None)
        logger.info("Writing seed processing manifest to s3://%s/%s", bucket, key)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
            Metadata={
                "artifact": "processed_seed_file",
                "status": "success",
                "seed_file_hash": file_hash,
            },
        )
        return landing_results

    @task(retries=0)
    def crawl_seed_accounts() -> list[dict]:
        results = []
        for account in load_seed_targets():
            logger.info("Processing account_id=%s", account["account_id"])
            raw_response = crawl_target(account)
            results.append(write_landing(account, raw_response))
        return results

    @task(retries=0)
    def trigger_databricks_job(landing_results: list[dict]) -> dict:
        host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
        token = os.getenv("DATABRICKS_TOKEN", "").strip()
        job_id = os.getenv("DATABRICKS_JOB_ID", "").strip()
        if not host or not token or not job_id:
            raise AirflowSkipException("DATABRICKS_HOST, DATABRICKS_TOKEN, or DATABRICKS_JOB_ID is not configured")

        payload = {
            "job_id": int(job_id) if job_id.isdigit() else job_id,
            "idempotency_token": datetime.now(timezone.utc).strftime("douyin-%Y%m%dT%H%M%S%f"),
            "job_parameters": {
                "s3_bucket": os.getenv("S3_BUCKET", "").strip(),
                "s3_landing_prefix": os.getenv("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json").strip().strip("/"),
                "s3_media_manifest_prefix": os.getenv("S3_MEDIA_MANIFEST_PREFIX", "lakehouse/landing/douyin/media_manifest/json").strip().strip("/"),
                "landing_result_count": str(len(landing_results)),
            },
        }
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{host}/api/2.1/jobs/run-now",
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        logger.info("Triggering Databricks job_id=%s after %s landing writes", job_id, len(landing_results))
        with request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))
        return {
            "job_id": job_id,
            "run_id": result.get("run_id"),
            "number_in_job": result.get("number_in_job"),
            "run_page_url": result.get("run_page_url"),
        }

    landing_results = crawl_seed_accounts()
    processed_results = write_seed_processing_manifest(landing_results)
    trigger_databricks_job(processed_results)

crawl_douyin_seed_to_s3_landing()
