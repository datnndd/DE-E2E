from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

# Media upload rules: map extracted media types to S3 metadata and file extensions.
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
    description="Read Douyin seed CSV/YAML, crawl accounts, write raw JSON/media to S3, then trigger Databricks.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["douyin", "food", "seed", "s3", "landing"],
)
def crawl_douyin_seed_to_s3_landing():
    # -------------------------------------------------------------------------
    # Shared configuration helpers
    # -------------------------------------------------------------------------
    def env_str(name: str, default: str = "") -> str:
        """Read an environment variable and trim whitespace."""
        return os.getenv(name, default).strip()

    def env_int(name: str, default: int) -> int:
        """Read a positive integer environment variable."""
        value = env_str(name, str(default))
        try:
            return int(value)
        except ValueError:
            logger.warning("Invalid integer env %s=%s; using default=%s", name, value, default)
            return default

    def s3_client():
        """Create an S3 client using the configured AWS region and connection pool."""
        return boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION") or None,
            config=Config(max_pool_connections=max(10, env_int("S3_MAX_POOL_CONNECTIONS", 64))),
        )

    def s3_transfer_config() -> TransferConfig:
        """Limit per-file multipart concurrency because media downloads already run in parallel."""
        return TransferConfig(
            max_concurrency=max(1, env_int("S3_UPLOAD_MAX_CONCURRENCY", 2)),
            multipart_threshold=max(8 * 1024 * 1024, env_int("S3_MULTIPART_THRESHOLD", 32 * 1024 * 1024)),
            multipart_chunksize=max(8 * 1024 * 1024, env_int("S3_MULTIPART_CHUNKSIZE", 32 * 1024 * 1024)),
            use_threads=True,
        )

    def s3_prefix(name: str, default: str) -> str:
        """Read an S3 prefix and normalize leading/trailing slashes."""
        return env_str(name, default).strip("/")

    def request_json(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None, timeout: int = 60) -> dict:
        """Send an HTTP request and parse a JSON response."""
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request_headers = {"Content-Type": "application/json", **(headers or {})}
        req = request.Request(url, data=body, headers=request_headers, method=method)
        with request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def request_json_with_retry(
        url: str,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict | None = None,
        timeout: int = 60,
        retry_times: int = 3,
        retry_backoff_seconds: int = 10,
    ) -> dict:
        """Retry transient HTTP 5xx JSON requests."""
        last_error = None
        for attempt in range(1, retry_times + 1):
            try:
                return request_json(url, method=method, payload=payload, headers=headers, timeout=timeout)
            except error.HTTPError as exc:
                last_error = exc
                if exc.code < 500 or attempt == retry_times:
                    raise
                logger.warning("HTTP request failed attempt=%s status=%s url=%s", attempt, exc.code, url)
            except Exception as exc:
                last_error = exc
                if attempt == retry_times:
                    raise
                logger.warning("HTTP request failed attempt=%s url=%s error=%s", attempt, url, exc)
            time.sleep(retry_backoff_seconds * attempt)
        raise RuntimeError(f"HTTP request failed after {retry_times} attempts: {last_error}")

    # -------------------------------------------------------------------------
    # Seed file discovery and processed-state control
    # -------------------------------------------------------------------------
    def load_seed_targets() -> list[dict]:
        """Load crawl targets from the latest unprocessed CSV file."""
        csv_file = discover_seed_csv_file()
        if not csv_file:
            raise AirflowSkipException("No seed CSV file found to process")
        return load_seed_targets_from_csv(csv_file)

    def discover_seed_csv_file() -> Path | None:
        """Find the explicit CSV file or latest unprocessed CSV in the seed folder."""
        explicit_file = env_str("DOUYIN_SEED_CSV_FILE")
        if explicit_file:
            path = Path(explicit_file)
            if path.exists():
                logger.info("Using explicit seed CSV file: %s", path)
                return path
            logger.warning("Explicit seed CSV file does not exist; auto-discovery will be used: %s", path)

        seed_dir = Path(env_str("DOUYIN_SEED_CSV_DIR", "/opt/airflow/seeds"))
        pattern = env_str("DOUYIN_SEED_CSV_PATTERN", "*.csv") or "*.csv"
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
        """Calculate SHA-256 hash for CSV content to identify already processed files."""
        digest = hashlib.sha256()
        with seed_file.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def seed_manifest_key(file_hash: str) -> str:
        """Build S3 control-manifest key for a processed seed CSV hash."""
        prefix = s3_prefix("S3_CONTROL_PREFIX", "lakehouse/control/douyin/processed_seed_files")
        return f"{prefix}/{file_hash}.json"

    def seed_csv_was_processed(seed_file: Path) -> bool:
        """Check S3 control manifest to skip CSV files already processed successfully."""
        bucket = env_str("S3_BUCKET")
        if not bucket:
            logger.warning("S3_BUCKET is not configured; seed CSV processed-state check is disabled")
            return False

        file_hash = seed_file_hash(seed_file)
        key = seed_manifest_key(file_hash)
        client = s3_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
            logger.info("Seed CSV already processed successfully: file=%s hash=%s", seed_file.name, file_hash)
            return True
        except client.exceptions.ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    # -------------------------------------------------------------------------
    # Seed parsing and target normalization
    # -------------------------------------------------------------------------
    def slugify(value: str) -> str:
        """Convert niche text into a stable snake-like slug."""
        slug = "".join(char.lower() if char.isalnum() else "_" for char in value.strip())
        return "_".join(part for part in slug.split("_") if part)

    def default_account_type(niche: str) -> str:
        """Generate account_type from niche when CSV does not provide one."""
        slug = slugify(niche)
        if "food" in slug or "restaurant" in slug:
            return "food_creator"
        base = slug.removeprefix("douyin_").split("_")[0] or "creator"
        return f"{base}_creator"

    def load_seed_targets_from_csv(seed_file: Path) -> list[dict]:
        """Read CSV rows and normalize them into account crawl targets."""
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

    # -------------------------------------------------------------------------
    # Ingestion-worker API calls
    # -------------------------------------------------------------------------
    def crawl_target(target: dict) -> dict:
        """Call ingestion-worker /douyin/fetch for one normalized target."""
        worker_url = env_str("INGESTION_WORKER_URL", "http://ingestion-worker:8000").rstrip("/")
        endpoint = f"{worker_url}/douyin/fetch"
        payload_dict = {
            "link": target["link"],
            "mode": target["mode"],
            "limit": target["limit"],
            "start_time": target["start_time"],
            "end_time": target["end_time"],
        }
        logger.info("Calling ingestion-worker account_id=%s", target["account_id"])
        return request_json_with_retry(
            endpoint,
            method="POST",
            payload=payload_dict,
            timeout=300,
            retry_times=max(1, env_int("INGESTION_WORKER_FETCH_RETRY_TIMES", 3)),
            retry_backoff_seconds=max(1, env_int("INGESTION_WORKER_FETCH_RETRY_BACKOFF_SECONDS", 30)),
        )

    # -------------------------------------------------------------------------
    # Media extraction from raw Douyin response
    # -------------------------------------------------------------------------
    def media_urls(url_list: list | None) -> list[str]:
        """Return only the primary Douyin media URL."""
        if not isinstance(url_list, list):
            return []
        for url in url_list:
            if isinstance(url, str) and url:
                return [url]
        return []

    def extract_aweme_list(raw_response: dict) -> list[dict]:
        """Normalize detail/list responses into a list of aweme objects."""
        raw = raw_response.get("raw") or {}
        if isinstance(raw.get("aweme_list"), list):
            return raw["aweme_list"]
        if isinstance(raw.get("aweme_detail"), dict):
            return [raw["aweme_detail"]]
        return []

    def extract_media_tasks(aweme: dict) -> list[dict]:
        """Extract downloadable video/image/cover tasks from one aweme."""
        tasks = []
        aweme_id = str(aweme.get("aweme_id") or aweme.get("group_id") or "unknown_aweme")

        video_urls = media_urls(aweme.get("video", {}).get("play_addr", {}).get("url_list", []))
        if video_urls:
            tasks.append({"aweme_id": aweme_id, "media_type": "video", "index": 0, "urls": video_urls})

        cover_urls = media_urls(aweme.get("video", {}).get("cover", {}).get("url_list", []))
        if cover_urls:
            tasks.append({"aweme_id": aweme_id, "media_type": "cover", "index": 0, "urls": cover_urls})

        for index, image in enumerate(aweme.get("images") or []):
            image_urls = media_urls(image.get("url_list") or image.get("download_url_list") or [])
            if image_urls:
                tasks.append({"aweme_id": aweme_id, "media_type": "image", "index": index, "urls": image_urls})

        avatar_urls = media_urls(aweme.get("author", {}).get("avatar_thumb", {}).get("url_list", []))
        if avatar_urls:
            tasks.append({"aweme_id": aweme_id, "media_type": "avatar", "index": 0, "urls": avatar_urls})

        return tasks

    def media_headers(extra_headers: dict | None = None) -> dict:
        """Build headers for Douyin media download."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.douyin.com/",
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def download_url_to_file(url: str, file_path: Path) -> tuple[str | None, str]:
        """Stream one URL to a temp file, resuming partial bytes when possible."""
        timeout = max(1, env_int("DOUYIN_MEDIA_DOWNLOAD_TIMEOUT", 180))
        chunk_size = max(8192, env_int("DOUYIN_MEDIA_CHUNK_SIZE", 1024 * 1024))
        existing_size = file_path.stat().st_size if file_path.exists() else 0
        extra_headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else None
        req = request.Request(url, headers=media_headers(extra_headers), method="GET")

        with request.urlopen(req, timeout=timeout) as response:
            status_code = getattr(response, "status", response.getcode())
            if status_code not in {200, 206}:
                raise RuntimeError(f"HTTP {status_code}")

            mode = "ab" if existing_size > 0 and status_code == 206 else "wb"
            expected_remaining = int(response.headers.get("Content-Length") or 0)
            bytes_written = 0
            content_type = response.headers.get("Content-Type")

            with file_path.open(mode) as file:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    file.write(chunk)
                    bytes_written += len(chunk)

            if expected_remaining and bytes_written < expected_remaining:
                raise RuntimeError(f"IncompleteRead({bytes_written} bytes read, {expected_remaining - bytes_written} more expected)")

            return content_type, url

    def download_media_file(urls: list[str], file_path: Path) -> tuple[str | None, str]:
        """Download media with retries, fallback URLs, and resume support."""
        retry_times = max(1, env_int("DOUYIN_MEDIA_RETRY_TIMES", 3))
        backoff_seconds = max(0, env_int("DOUYIN_MEDIA_RETRY_BACKOFF_SECONDS", 2))
        last_error = None

        for attempt in range(1, retry_times + 1):
            for url in urls:
                try:
                    return download_url_to_file(url, file_path)
                except Exception as exc:
                    last_error = exc
                    current_size = file_path.stat().st_size if file_path.exists() else 0
                    logger.warning(
                        "Media download attempt=%s failed partial_bytes=%s url=%s error=%s",
                        attempt,
                        current_size,
                        url,
                        exc,
                    )
            if attempt < retry_times and backoff_seconds > 0:
                time.sleep(backoff_seconds * attempt)

        raise RuntimeError(f"Media download failed after {retry_times} attempts: {last_error}")

    def build_media_key(media_prefix: str, account: dict, media: dict) -> str:
        """Build deterministic S3 key for a downloaded media object."""
        media_type = media["media_type"]
        extension = MEDIA_EXTENSIONS.get(media_type, "bin")
        account_prefix = f"{media_prefix}/niche={account['niche']}/account_id={account['account_id']}/files"
        if media_type == "avatar":
            return f"{account_prefix}/user_avatar_{media['index']}.{extension}"
        return f"{account_prefix}/{media['aweme_id']}_{media_type}_{media['index']}.{extension}"

    def media_task_id(media: dict) -> str:
        """Build stable ID for one aweme media task."""
        return f"{media['aweme_id']}::{media['media_type']}::{media['index']}"

    def upload_single_media(client, bucket: str, account: dict, media_prefix: str, media: dict, retry_round: int = 0) -> dict:
        """Download one media object to temp file, upload to S3, return manifest row."""
        media_type = media["media_type"]
        source_url = (media.get("urls") or [None])[0]
        key = build_media_key(media_prefix, account, media)
        task_id = media_task_id(media)
        temp_path = None
        try:
            extension = MEDIA_EXTENSIONS.get(media_type, "bin")
            with tempfile.NamedTemporaryFile(prefix="douyin_media_", suffix=f".{extension}", delete=False) as temp_file:
                temp_path = Path(temp_file.name)

            detected_content_type, source_url = download_media_file(media["urls"], temp_path)
            content_type = detected_content_type or MEDIA_CONTENT_TYPES.get(media_type, "application/octet-stream")
            client.upload_file(
                str(temp_path),
                bucket,
                key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": {
                        "source": "douyin",
                        "niche": account["niche"],
                        "account_id": account["account_id"],
                        "aweme_id": media["aweme_id"],
                        "media_type": media_type,
                    },
                },
                Config=s3_transfer_config(),
            )
            file_size = temp_path.stat().st_size
            s3_url = f"s3://{bucket}/{key}"
            return {
                "status": "uploaded",
                "account_id": account["account_id"],
                "aweme_id": media["aweme_id"],
                "media_type": media_type,
                "index": media["index"],
                "s3_key": key,
                "s3_url": s3_url,
                "bytes": file_size,
                "content_type": content_type,
                "source_url": source_url,
                "candidate_url_count": len(media.get("urls") or []),
                "media_task_id": task_id,
                "retry_round": retry_round,
            }
        except Exception as exc:
            logger.warning("Media upload failed media_type=%s aweme_id=%s error=%s", media_type, media["aweme_id"], exc)
            return {
                "status": "failed",
                "account_id": account["account_id"],
                "aweme_id": media["aweme_id"],
                "media_type": media_type,
                "index": media["index"],
                "source_url": source_url,
                "candidate_url_count": len(media.get("urls") or []),
                "partial_bytes": temp_path.stat().st_size if temp_path and temp_path.exists() else 0,
                "media_task_id": task_id,
                "retry_round": retry_round,
                "error": str(exc),
            }
        finally:
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)

    def upload_aweme_media(client, bucket: str, account: dict, raw_response: dict) -> list[dict]:
        """Download media from raw response and upload files to S3 in parallel."""
        if env_str("DOUYIN_DOWNLOAD_MEDIA", "true").lower() not in {"1", "true", "yes"}:
            logger.info("Media download disabled by DOUYIN_DOWNLOAD_MEDIA")
            return []

        media_prefix = s3_prefix("S3_MEDIA_PREFIX", "lakehouse/landing/douyin/media_raw")
        awemes = extract_aweme_list(raw_response)
        media_tasks = [media for aweme in awemes for media in extract_media_tasks(aweme)]

        workers = max(1, env_int("DOUYIN_MEDIA_DOWNLOAD_WORKERS", 4))
        workers = min(workers, len(media_tasks) or 1)
        logger.info("Preparing parallel media download for %s awemes, %s media, workers=%s", len(awemes), len(media_tasks), workers)

        uploaded = upload_media_batch(client, bucket, account, media_prefix, media_tasks, workers, retry_round=0)
        uploaded = retry_failed_media(client, bucket, account, media_prefix, media_tasks, uploaded, workers)

        status_counts = {}
        for item in uploaded:
            status_counts[item.get("status", "unknown")] = status_counts.get(item.get("status", "unknown"), 0) + 1
        logger.info("Processed %s media objects status_counts=%s", len(uploaded), status_counts)
        return uploaded

    def upload_media_batch(
        client,
        bucket: str,
        account: dict,
        media_prefix: str,
        media_tasks: list[dict],
        workers: int,
        retry_round: int,
    ) -> list[dict]:
        """Upload a batch of media tasks in parallel."""
        if not media_tasks:
            return []
        results = []
        with ThreadPoolExecutor(max_workers=min(workers, len(media_tasks))) as executor:
            futures = [
                executor.submit(upload_single_media, client, bucket, account, media_prefix, media, retry_round)
                for media in media_tasks
            ]
            for future in as_completed(futures):
                results.append(future.result())
        return results

    def retry_failed_media(
        client,
        bucket: str,
        account: dict,
        media_prefix: str,
        original_tasks: list[dict],
        first_results: list[dict],
        workers: int,
    ) -> list[dict]:
        """Retry failed media immediately before writing the account manifest."""
        retry_rounds = max(0, env_int("DOUYIN_MEDIA_FAILED_RETRY_ROUNDS", 1))
        latest_results = {result["media_task_id"]: result for result in first_results}
        original_by_id = {media_task_id(media): media for media in original_tasks}

        for retry_round in range(1, retry_rounds + 1):
            failed_ids = [task_id for task_id, result in latest_results.items() if result.get("status") != "uploaded"]
            if not failed_ids:
                break

            failed_tasks = [original_by_id[task_id] for task_id in failed_ids if task_id in original_by_id]
            logger.info("Retrying %s failed media objects retry_round=%s", len(failed_tasks), retry_round)
            retry_results = upload_media_batch(client, bucket, account, media_prefix, failed_tasks, workers, retry_round)
            for result in retry_results:
                latest_results[result["media_task_id"]] = result

        return list(latest_results.values())

    # -------------------------------------------------------------------------
    # S3 Landing writers
    # -------------------------------------------------------------------------
    def write_media_manifest(client, bucket: str, account: dict, media_uploads: list[dict], now: datetime, run_id: str) -> dict | None:
        """Write media-upload manifest JSON to S3 Landing."""
        if not media_uploads:
            return None

        manifest_prefix = s3_prefix("S3_MEDIA_MANIFEST_PREFIX", "lakehouse/landing/douyin/media_manifest/json")
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
        """Write one account raw API response and its media manifest to S3."""
        bucket = env_str("S3_BUCKET")
        if not bucket:
            raise AirflowSkipException("S3_BUCKET is not configured")

        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S%f")
        prefix = s3_prefix("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json")
        client = s3_client()
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
            "media_manifest_uri": media_manifest.get("uri") if isinstance(media_manifest, dict) else "",
            "seed_file_name": account.get("seed_file_name", ""),
            "seed_file_path": account.get("seed_file_path", ""),
            "seed_file_hash": account.get("seed_file_hash", ""),
        }

    def write_processed_seed_manifest(landing_results: list[dict]) -> None:
        """Mark a CSV seed as processed after landing writes succeed."""
        first = landing_results[0]
        file_hash = str(first.get("seed_file_hash") or "").strip()
        if not file_hash:
            logger.info("Landing results did not come from CSV; skipping seed processing manifest")
            return

        bucket = env_str("S3_BUCKET")
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
        logger.info("Writing seed processing manifest to s3://%s/%s", bucket, key)
        s3_client().put_object(
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

    # -------------------------------------------------------------------------
    # Downstream Databricks trigger
    # -------------------------------------------------------------------------
    def build_databricks_run_payload(job_id: str, landing_results: list[dict]) -> dict:
        """Build Databricks Jobs API payload with only newly written S3 URIs."""
        landing_uris = [result["uri"] for result in landing_results if result.get("uri")]
        media_manifest_uris = [result["media_manifest_uri"] for result in landing_results if result.get("media_manifest_uri")]
        seed_file_hash_value = str(landing_results[0].get("seed_file_hash") or "") if landing_results else ""
        return {
            "job_id": int(job_id) if job_id.isdigit() else job_id,
            "idempotency_token": datetime.now(timezone.utc).strftime("douyin-%Y%m%dT%H%M%S%f"),
            "job_parameters": {
                "s3_bucket": env_str("S3_BUCKET"),
                "s3_landing_prefix": s3_prefix("S3_LANDING_PREFIX", "lakehouse/landing/douyin/api_raw/json"),
                "s3_media_manifest_prefix": s3_prefix("S3_MEDIA_MANIFEST_PREFIX", "lakehouse/landing/douyin/media_manifest/json"),
                "landing_result_count": str(len(landing_results)),
                "landing_uris_json": json.dumps(landing_uris, separators=(",", ":")),
                "media_manifest_uris_json": json.dumps(media_manifest_uris, separators=(",", ":")),
                "seed_file_hash": seed_file_hash_value,
            },
        }

    def run_databricks_job(landing_results: list[dict]) -> dict:
        """Call Databricks Jobs run-now API after S3 landing is complete."""
        host = env_str("DATABRICKS_HOST").rstrip("/")
        token = env_str("DATABRICKS_TOKEN")
        job_id = env_str("DATABRICKS_JOB_ID")
        if not host or not token or not job_id:
            raise AirflowSkipException("DATABRICKS_HOST, DATABRICKS_TOKEN, or DATABRICKS_JOB_ID is not configured")

        payload = build_databricks_run_payload(job_id, landing_results)
        logger.info("Triggering Databricks job_id=%s after %s landing writes", job_id, len(landing_results))
        result = request_json(
            f"{host}/api/2.1/jobs/run-now",
            method="POST",
            payload=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        run_info = {
            "job_id": job_id,
            "run_id": result.get("run_id"),
            "number_in_job": result.get("number_in_job"),
            "run_page_url": result.get("run_page_url"),
        }
        wait_for_databricks_run(host, token, run_info["run_id"])
        return run_info

    def get_databricks_run(host: str, token: str, run_id: int | str) -> dict:
        """Read one Databricks run state from Jobs API."""
        return request_json(
            f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )

    def wait_for_databricks_run(host: str, token: str, run_id: int | str | None) -> None:
        """Poll Databricks run until success or fail Airflow on terminal failure."""
        if not run_id:
            raise RuntimeError("Databricks run-now response did not include run_id")

        poll_seconds = max(5, env_int("DATABRICKS_RUN_POLL_SECONDS", 30))
        timeout_minutes = max(1, env_int("DATABRICKS_RUN_TIMEOUT_MINUTES", 120))
        deadline = time.monotonic() + timeout_minutes * 60

        while True:
            run = get_databricks_run(host, token, run_id)
            state = run.get("state") or {}
            life_cycle_state = state.get("life_cycle_state")
            result_state = state.get("result_state")
            state_message = state.get("state_message") or ""
            run_page_url = run.get("run_page_url") or ""
            logger.info(
                "Databricks run_id=%s life_cycle_state=%s result_state=%s message=%s url=%s",
                run_id,
                life_cycle_state,
                result_state,
                state_message,
                run_page_url,
            )

            if life_cycle_state == "TERMINATED" and result_state == "SUCCESS":
                return
            if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
                raise RuntimeError(
                    f"Databricks run failed run_id={run_id} "
                    f"life_cycle_state={life_cycle_state} result_state={result_state} "
                    f"message={state_message} url={run_page_url}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Databricks run timeout run_id={run_id} after {timeout_minutes} minutes")

            time.sleep(poll_seconds)

    # -------------------------------------------------------------------------
    # Airflow tasks and dependencies
    # -------------------------------------------------------------------------
    @task(retries=0)
    def crawl_seed_accounts() -> list[dict]:
        """Crawl all targets from the selected seed file and write S3 landing outputs."""
        results = []
        for account in load_seed_targets():
            logger.info("Processing account_id=%s", account["account_id"])
            raw_response = crawl_target(account)
            results.append(write_landing(account, raw_response))
        return results

    @task(retries=0)
    def write_seed_processing_manifest(landing_results: list[dict], databricks_result: dict) -> list[dict]:
        """Persist S3 control manifest only after Databricks succeeds."""
        if not landing_results:
            raise AirflowSkipException("No landing results to mark as processed")
        logger.info("Databricks succeeded; marking seed processed: %s", databricks_result)
        write_processed_seed_manifest(landing_results)
        return landing_results

    @task(retries=0)
    def trigger_databricks_job(landing_results: list[dict]) -> dict:
        """Trigger Bronze/Silver/Gold processing in Databricks."""
        return run_databricks_job(landing_results)

    landing_results = crawl_seed_accounts()
    databricks_result = trigger_databricks_job(landing_results)
    write_seed_processing_manifest(landing_results, databricks_result)


crawl_douyin_seed_to_s3_landing()
