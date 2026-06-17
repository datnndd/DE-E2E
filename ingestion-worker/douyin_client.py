from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from douyin.abogus import ABogus
from douyin.urls import Urls
from douyin.xbogus import USER_AGENT, XBogus, generate_random_str

COMMON_PARAMS: dict[str, str | int] = {
    "device_platform": "webapp",
    "aid": "6383",
    "channel": "channel_pc_web",
    "pc_client_type": 1,
    "version_code": "170400",
    "version_name": "17.4.0",
    "cookie_enabled": "true",
    "screen_width": "1920",
    "screen_height": "1080",
    "browser_language": "zh-CN",
    "browser_platform": "Win32",
    "browser_name": "Chrome",
    "browser_version": "122.0.0.0",
    "browser_online": "true",
    "engine_name": "Blink",
    "engine_version": "122.0.0.0",
    "os_name": "Windows",
    "os_version": "10",
    "cpu_core_num": "8",
    "device_memory": "8",
    "platform": "PC",
    "downlink": "10",
    "effective_type": "4g",
    "round_trip_time": "50",
}

@dataclass(frozen=True)
class DouyinTarget:
    target_type: str
    target_id: str
    resolved_url: str


class DouyinClient:
    def __init__(self, cookie: str | None = None, timeout: int = 30) -> None:
        self.timeout = timeout
        self.urls = Urls()
        self.abogus = ABogus()
        self.xbogus = XBogus()
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(self._headers(cookie))

    def fetch(
        self,
        link: str,
        mode: str = "post",
        limit: int = 20,
        start_time: str = "",
        end_time: str = "",
    ) -> dict[str, Any]:
        start_date, end_date = self._date_window(start_time, end_time)
        target = self.resolve_target(link)
        if target.target_type == "aweme":
            data = self.fetch_aweme_detail(target.target_id)
        elif target.target_type == "user":
            data = self.fetch_user_awemes(target.target_id, mode=mode, limit=limit, start_date=start_date, end_date=end_date)
        elif target.target_type == "mix":
            data = self.fetch_mix_awemes(target.target_id, limit=limit, start_date=start_date, end_date=end_date)
        elif target.target_type == "music":
            data = self.fetch_music_awemes(target.target_id, limit=limit, start_date=start_date, end_date=end_date)
        else:
            raise ValueError(f"Unsupported Douyin target type: {target.target_type}")

        return {
            "source": "douyin",
            "target": target.__dict__,
            "mode": mode,
            "limit": limit,
            "start_time": start_date,
            "end_time": end_date,
            "raw": data,
        }

    def resolve_target(self, link: str) -> DouyinTarget:
        share_url = self.extract_share_url(link)
        response = self.session.get(share_url, allow_redirects=True, timeout=self.timeout)
        resolved_url = response.url
        path_url = response.request.path_url
        combined = f"{resolved_url} {path_url}"

        patterns = [
            ("user", r"/user/([^?\s]+)"),
            ("aweme", r"/(?:video|note)/(\d+)"),
            ("mix", r"/(?:mix/detail|collection)/(\d+)"),
            ("music", r"/music/(\d+)"),
        ]
        for target_type, pattern in patterns:
            match = re.search(pattern, combined)
            if match:
                return DouyinTarget(target_type, match.group(1), resolved_url)

        raise ValueError("Cannot resolve Douyin link to supported target")

    def fetch_aweme_detail(self, aweme_id: str) -> dict[str, Any]:
        params = {**COMMON_PARAMS, "aweme_id": aweme_id}
        return self._signed_get_json(self.urls.POST_DETAIL, params, signer="abogus")

    def fetch_user_awemes(self, sec_user_id: str, mode: str, limit: int, start_date: str, end_date: str) -> dict[str, Any]:
        if mode not in {"post", "like"}:
            raise ValueError("mode must be post or like")

        endpoint = self.urls.USER_POST if mode == "post" else self.urls.USER_FAVORITE_A
        max_cursor = "0"
        awemes: list[dict[str, Any]] = []
        has_more = True

        while has_more and not self._reached_limit(awemes, limit):
            count = self._page_count(limit, len(awemes), default=18)
            params = {**COMMON_PARAMS, "sec_user_id": sec_user_id, "count": count, "max_cursor": max_cursor}
            data = self._signed_get_json(endpoint, params, signer="xbogus")
            batch = data.get("aweme_list") or []
            self._append_filtered(awemes, batch, limit, start_date, end_date)
            has_more = bool(data.get("has_more")) and bool(batch)
            max_cursor = str(data.get("max_cursor") or data.get("cursor") or "0")

        return {"aweme_list": awemes, "count": len(awemes), "has_more": has_more, "max_cursor": max_cursor}

    def fetch_mix_awemes(self, mix_id: str, limit: int, start_date: str, end_date: str) -> dict[str, Any]:
        return self._fetch_cursor_awemes(self.urls.USER_MIX, {"mix_id": mix_id}, limit, start_date, end_date)

    def fetch_music_awemes(self, music_id: str, limit: int, start_date: str, end_date: str) -> dict[str, Any]:
        return self._fetch_cursor_awemes(self.urls.MUSIC, {"music_id": music_id}, limit, start_date, end_date)

    def _fetch_cursor_awemes(self, endpoint: str, extra_params: dict[str, Any], limit: int, start_date: str, end_date: str) -> dict[str, Any]:
        cursor = "0"
        awemes: list[dict[str, Any]] = []
        has_more = True

        while has_more and not self._reached_limit(awemes, limit):
            count = self._page_count(limit, len(awemes), default=20)
            params = {**COMMON_PARAMS, **extra_params, "count": count, "cursor": cursor}
            data = self._signed_get_json(endpoint, params, signer="xbogus")
            batch = data.get("aweme_list") or []
            self._append_filtered(awemes, batch, limit, start_date, end_date)
            has_more = bool(data.get("has_more")) and bool(batch)
            cursor = str(data.get("cursor") or data.get("max_cursor") or "0")

        return {"aweme_list": awemes, "count": len(awemes), "has_more": has_more, "cursor": cursor}

    def _signed_get_json(self, endpoint: str, params: dict[str, Any], signer: str) -> dict[str, Any]:
        payload = urlencode(params)
        if signer == "abogus":
            a_bogus = quote(self.abogus.get_value(payload), safe="")
            query = f"{payload}&a_bogus={a_bogus}"
        elif signer == "xbogus":
            query = self.xbogus.sign(payload, user_agent=USER_AGENT)
        else:
            query = payload

        response = self.session.get(f"{endpoint}{query}", timeout=self.timeout)
        response.raise_for_status()
        return response.json()


    @staticmethod
    def _append_filtered(awemes: list[dict[str, Any]], batch: list[dict[str, Any]], limit: int, start_date: str, end_date: str) -> None:
        for aweme in batch:
            create_date = DouyinClient._aweme_create_date(aweme)
            if start_date <= create_date <= end_date:
                awemes.append(aweme)
            if DouyinClient._reached_limit(awemes, limit):
                return

    @staticmethod
    def _aweme_create_date(aweme: dict[str, Any]) -> str:
        create_time = int(aweme.get("create_time") or 0)
        if create_time <= 0:
            return "1970-01-01"
        return time.strftime("%Y-%m-%d", time.localtime(create_time))

    @staticmethod
    def _date_window(start_time: str, end_time: str) -> tuple[str, str]:
        start_date = start_time.strip() or "1970-01-01"
        end_date = end_time.strip() or datetime.now().strftime("%Y-%m-%d")
        for value, name in [(start_date, "start_time"), (end_date, "end_time")]:
            datetime.strptime(value, "%Y-%m-%d")
        return start_date, end_date

    @staticmethod
    def _reached_limit(awemes: list[dict[str, Any]], limit: int) -> bool:
        return limit > 0 and len(awemes) >= limit

    @staticmethod
    def _page_count(limit: int, current_count: int, default: int) -> int:
        if limit <= 0:
            return default
        return min(default, max(limit - current_count, 1))

    @staticmethod
    def extract_share_url(text: str) -> str:
        match = re.search(r"https?://[^\s]+", text)
        return match.group(0) if match else text.strip()

    @staticmethod
    def _headers(cookie: str | None) -> dict[str, str]:
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://www.douyin.com/?recommend=1",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        headers["Cookie"] = cookie or f"msToken={generate_random_str(107)}"
        return headers