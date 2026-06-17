from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from douyin_client import DouyinClient


class FetchRequest(BaseModel):
    link: str | None = None
    mode: str = Field(default="post", pattern="^(post|like)$")
    limit: int = Field(default=20, ge=0, le=10000)
    start_time: str = ""
    end_time: str = ""

class TransferPayload(BaseModel):
    pipeline: str = Field(default="de-e2e")
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    records: list[dict[str, Any]] = Field(default_factory=list)



def build_douyin_cookie() -> str | None:
    parts = []
    for env_name, cookie_name in [
        ("DOUYIN_MSTOKEN", "msToken"),
        ("DOUYIN_TTWID", "ttwid"),
        ("DOUYIN_ODIN_TT", "odin_tt"),
        ("DOUYIN_PASSPORT_CSRF_TOKEN", "passport_csrf_token"),
        ("DOUYIN_SID_GUARD", "sid_guard"),
        ("DOUYIN_SESSIONID", "sessionid"),
        ("DOUYIN_SID_TT", "sid_tt"),
    ]:
        value = os.getenv(env_name, "").strip()
        if value:
            parts.append(f"{cookie_name}={value}")
    return "; ".join(parts) or None

app = FastAPI(title="DE-E2E Ingestion Worker", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "ingestion-worker"}


@app.post("/douyin/fetch")
def fetch_douyin(request: FetchRequest) -> dict[str, Any]:
    link = request.link or os.getenv("DOUYIN_LINK", "").strip()
    if not link:
        raise HTTPException(status_code=400, detail="Missing link or DOUYIN_LINK")

    try:
        client = DouyinClient(cookie=build_douyin_cookie())
        return client.fetch(
            link=link,
            mode=request.mode,
            limit=request.limit,
            start_time=request.start_time,
            end_time=request.end_time,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/data")
def get_data() -> TransferPayload:
    link = os.getenv("DOUYIN_LINK", "").strip()
    if not link:
        return TransferPayload(records=[])

    mode = os.getenv("DOUYIN_MODE", "post").strip() or "post"
    limit = int(os.getenv("DOUYIN_LIMIT", "20"))
    start_time = os.getenv("DOUYIN_START_TIME", "").strip()
    end_time = os.getenv("DOUYIN_END_TIME", "").strip()
    client = DouyinClient(cookie=build_douyin_cookie())
    data = client.fetch(link=link, mode=mode, limit=limit, start_time=start_time, end_time=end_time)
    return TransferPayload(records=[data])