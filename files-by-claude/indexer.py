from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from opensearchpy import OpenSearch

import config


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts(event_ts: Optional[int]) -> int:
    return event_ts if event_ts is not None else int(time.time() * 1000)


def index_basic(
    client: OpenSearch,
    doc_id: str,
    *,
    image_key: str,
    image_url: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    status: str = "stored",
    event_ts: Optional[int] = None,
) -> None:
    """1차 색인: 이미지 URL + 기본 메타데이터.
    - doc_as_upsert 로 멱등(같은 doc_id 재수신 안전).
    - description / caption_vector 는 건드리지 않음 → 2차가 먼저 와도 덮어쓰지 않음.
    이 시점부터 키워드(BM25)/시맨틱은 아직 불가, 문서만 존재.
    """
    doc = {
        "image_key": image_key,
        "image_url": image_url,
        "status": status,
        "phase": "basic",
        "event_ts": _ts(event_ts),
        "updated_at": _now_iso(),
    }
    if width is not None:
        doc["width"] = width
    if height is not None:
        doc["height"] = height
    client.update(
        index=config.INDEX_NAME, id=doc_id,
        body={"doc": doc, "doc_as_upsert": True},
    )


def index_enrichment(
    client: OpenSearch,
    doc_id: str,
    *,
    description: str,
    vector: List[float],
    event_ts: Optional[int] = None,
) -> None:
    """2차 색인: 캡션(BM25) + 임베딩(kNN) 추가. 기존 기본 메타와 병합.
    이 시점 이후 키워드 + 시맨틱 하이브리드 검색 가능.
    """
    doc = {
        "description": description,
        "caption_vector": vector,
        "phase": "enriched",
        "event_ts": _ts(event_ts),
        "updated_at": _now_iso(),
    }
    client.update(
        index=config.INDEX_NAME, id=doc_id,
        body={"doc": doc, "doc_as_upsert": True},
    )


# 참고: 같은 필드를 두 이벤트가 경쟁하면(예: 둘 다 status 수정) 순서 역전 시
# stale overwrite 가 날 수 있다. 엄격히 막으려면 painless 스크립트로
# event_ts 비교 후 갱신하거나 external versioning 을 쓴다. 지금 구조는
# 1차/2차가 서로 다른 필드만 쓰므로 doc_as_upsert 병합으로 충분하다.
