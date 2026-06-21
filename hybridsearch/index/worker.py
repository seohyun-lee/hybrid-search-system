"""Two-phase indexing into OpenSearch with a per-phase stale-overwrite guard.

Models the real system's two events as two functions:

  * index_basic      -> ImageCreated : image_url + basic metadata (1st event)
  * index_enrichment -> ImageEnriched: description (BM25) + caption_vector (kNN)

Each writes only its own disjoint field set via a scripted upsert, guarded on a
phase-local timestamp (ts_basic / ts_enriched = the event's occurred_at). So:

  * order-independent across phases — a late ImageCreated only touches ts_basic +
    its own fields, never the description/vector an earlier ImageEnriched wrote.
  * idempotent under at-least-once — a duplicate/stale redelivery of the *same*
    phase whose ts is older than the stored one is dropped (ctx.op = 'noop').

`phase` is a single shared field, so it's advanced without downgrading: once a
doc is enriched, a later basic event won't revert it to "basic".
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from opensearchpy import OpenSearch

from .. import config
from ..events.schema import EVENT_IMAGE_CREATED, EVENT_IMAGE_ENRICHED

# Apply params.fields only if this phase's stored ts is older than the incoming
# event ts; always advance the phase ts. Then set `phase` from whichever phases
# have a ts (never downgrade enriched -> basic).
_GUARD_SCRIPT = (
    "long ts = params.ts;"
    "String tf = params.ts_field;"
    "def cur = ctx._source[tf];"
    "if (cur != null && ts < cur) { ctx.op = 'noop'; return; }"
    "for (entry in params.fields.entrySet()) { ctx._source[entry.getKey()] = entry.getValue(); }"
    "ctx._source[tf] = ts;"
    "if (ctx._source.ts_enriched != null) { ctx._source.phase = 'enriched'; }"
    "else if (ctx._source.ts_basic != null) { ctx._source.phase = 'basic'; }"
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _guarded_upsert(
    client: OpenSearch,
    doc_id: str,
    *,
    fields: Dict[str, Any],
    ts_field: str,
    event_ts: Optional[int],
) -> None:
    """Scripted upsert that writes `fields` + advances `ts_field`, but only if the
    incoming event_ts is newer than the stored one for this phase."""
    ts = event_ts if event_ts is not None else _now_ms()
    client.update(
        index=config.INDEX_NAME,
        id=doc_id,
        body={
            "scripted_upsert": True,
            "upsert": {},
            "script": {
                "lang": "painless",
                "source": _GUARD_SCRIPT,
                "params": {"ts": ts, "ts_field": ts_field, "fields": fields},
            },
        },
        retry_on_conflict=3,
    )


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
    """1st-phase (ImageCreated): image URL + basic metadata. Guarded on ts_basic."""
    fields: Dict[str, Any] = {
        "image_id": image_id or doc_id,
        "image_url": image_url,
        "status": status,
        "updated_at": _now_iso(),
    }
    if width is not None:
        fields["width"] = width
    if height is not None:
        fields["height"] = height
    if source is not None:
        fields["source"] = source
    _guarded_upsert(client, doc_id, fields=fields, ts_field="ts_basic", event_ts=event_ts)


def index_enrichment(
    client: OpenSearch,
    doc_id: str,
    *,
    description: str,
    vector: List[float],
    event_ts: Optional[int] = None,
) -> None:
    """2nd-phase (ImageEnriched): caption text (BM25) + embedding (kNN). Guarded on
    ts_enriched. Hybrid keyword + semantic search becomes possible after this."""
    fields: Dict[str, Any] = {
        "description": description,
        "caption_vector": vector,
        "updated_at": _now_iso(),
    }
    _guarded_upsert(client, doc_id, fields=fields, ts_field="ts_enriched", event_ts=event_ts)


def handle_event(client: OpenSearch, storage, envelope: Dict[str, Any]) -> None:
    """Route a parsed event envelope to the right phase. Raises on bad payloads /
    unknown types (caller treats as permanent -> DLQ). occurred_at flows in as the
    guard's event_ts.

    ImageEnriched embeds the caption here (the worker owns enrichment). The vector
    is cached in the storage sidecar so a redelivery / reindex skips the model.
    """
    event_type = envelope["event_type"]
    image_id = envelope["image_id"]
    occurred_at = envelope.get("occurred_at")
    payload = envelope.get("payload") or {}

    if event_type == EVENT_IMAGE_CREATED:
        index_basic(
            client,
            image_id,
            image_url=payload["image_url"],
            image_id=image_id,
            width=payload.get("width"),
            height=payload.get("height"),
            source=payload.get("source"),
            status=payload.get("status", "stored"),
            event_ts=occurred_at,
        )
    elif event_type == EVENT_IMAGE_ENRICHED:
        description = payload["description"]
        sidecar = (storage.get_metadata(image_id) or {}) if storage is not None else {}
        vector = sidecar.get("caption_vector")
        if vector is None:
            from ..embedding import embed

            vector = embed(description)
            if storage is not None:
                storage.put_metadata(
                    image_id, {**sidecar, "description": description, "caption_vector": vector}
                )
        index_enrichment(client, image_id, description=description, vector=vector, event_ts=occurred_at)
    else:
        raise ValueError(f"unknown event_type: {event_type!r}")
