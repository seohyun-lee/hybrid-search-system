"""Two-phase indexing into OpenSearch.

Models the real system's two Kafka events as two functions:

  * index_basic      -> 1st event: image_url + basic metadata. Document exists,
                        but keyword (BM25) / semantic (kNN) search not yet possible.
  * index_enrichment -> 2nd event: description (BM25) + caption_vector (kNN).
                        After this, hybrid search works.

Both use doc_as_upsert, so they are idempotent and order-independent: whichever
event arrives first creates the doc, the other merges into it. They write
disjoint field sets, so the merge never clobbers the other phase's data.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from opensearchpy import OpenSearch

from .. import config


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts(event_ts: Optional[int]) -> int:
    return event_ts if event_ts is not None else int(time.time() * 1000)


def index_basic(
    client: OpenSearch,
    doc_id: str,
    *,
    image_url: str,
    image_id: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    source: Optional[str] = None,
    status: str = "stored",
    event_ts: Optional[int] = None,
) -> None:
    """1st-phase index: image URL + basic metadata. Leaves description /
    caption_vector untouched so a 2nd event that arrives first is not overwritten."""
    doc = {
        "image_id": image_id or doc_id,
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
    if source is not None:
        doc["source"] = source
    client.update(
        index=config.INDEX_NAME,
        id=doc_id,
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
    """2nd-phase index: caption text (BM25) + embedding (kNN), merged onto the
    basic doc. Hybrid keyword + semantic search becomes possible after this."""
    doc = {
        "description": description,
        "caption_vector": vector,
        "phase": "enriched",
        "event_ts": _ts(event_ts),
        "updated_at": _now_iso(),
    }
    client.update(
        index=config.INDEX_NAME,
        id=doc_id,
        body={"doc": doc, "doc_as_upsert": True},
    )
