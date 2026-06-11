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
    limit: int = Field(default=20, ge=1, le=200)


class TransferPayload(BaseModel):
    pipeline: str = Field(default="de-e2e")
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    records: list[dict[str, Any]] = Field(default_factory=list)


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
        client = DouyinClient(cookie=os.getenv("DOUYIN_COOKIE", "").strip() or None)
        return client.fetch(link=link, mode=request.mode, limit=request.limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/data")
def get_data() -> TransferPayload:
    link = os.getenv("DOUYIN_LINK", "").strip()
    if not link:
        return TransferPayload(records=[])

    mode = os.getenv("DOUYIN_MODE", "post").strip() or "post"
    limit = int(os.getenv("DOUYIN_LIMIT", "20"))
    client = DouyinClient(cookie=os.getenv("DOUYIN_COOKIE", "").strip() or None)
    data = client.fetch(link=link, mode=mode, limit=limit)
    return TransferPayload(records=[data])