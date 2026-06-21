"""Event envelope + types for the `image-events` topic.

Common envelope: {image_id, event_type, occurred_at, payload}
  occurred_at: epoch milliseconds. Flows straight into the worker's per-phase
  stale-overwrite guard (ts_basic / ts_enriched).

Two event types, published by different services in the real system (upload vs
captioning), so order is NOT guaranteed across them. Kafka key=image_id keeps
each image's events on one partition; the worker's guard handles the rest.

  ImageCreated  (1st): payload = image_key, image_url, width, height, status [, source]
  ImageEnriched (2nd): payload = description [, tags]
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

EVENT_IMAGE_CREATED = "ImageCreated"
EVENT_IMAGE_ENRICHED = "ImageEnriched"
EVENT_TYPES = (EVENT_IMAGE_CREATED, EVENT_IMAGE_ENRICHED)


def now_ms() -> int:
    return int(time.time() * 1000)


def envelope(
    image_id: str,
    event_type: str,
    payload: Dict[str, Any],
    occurred_at: Optional[int] = None,
) -> Dict[str, Any]:
    if not image_id:
        raise ValueError("image_id is required")
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event_type: {event_type!r}")
    return {
        "image_id": image_id,
        "event_type": event_type,
        "occurred_at": occurred_at if occurred_at is not None else now_ms(),
        "payload": payload or {},
    }


def image_created(
    image_id: str,
    *,
    image_key: str,
    image_url: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    status: str = "stored",
    source: Optional[str] = None,
    occurred_at: Optional[int] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "image_key": image_key,
        "image_url": image_url,
        "width": width,
        "height": height,
        "status": status,
    }
    if source is not None:
        payload["source"] = source
    return envelope(image_id, EVENT_IMAGE_CREATED, payload, occurred_at)


def image_enriched(
    image_id: str,
    *,
    description: str,
    tags: Optional[list] = None,
    occurred_at: Optional[int] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"description": description}
    if tags is not None:
        payload["tags"] = tags
    return envelope(image_id, EVENT_IMAGE_ENRICHED, payload, occurred_at)


def serialize(env: Dict[str, Any]) -> bytes:
    return json.dumps(env, ensure_ascii=False).encode("utf-8")


def parse(raw: bytes) -> Dict[str, Any]:
    """Decode + validate a wire message. Raises ValueError on any schema problem;
    the consumer treats that as a permanent error (-> DLQ, never retried)."""
    try:
        env = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"malformed JSON: {e}") from e
    if not isinstance(env, dict):
        raise ValueError("envelope is not a JSON object")
    for field in ("image_id", "event_type", "payload"):
        if field not in env:
            raise ValueError(f"missing field: {field}")
    if env["event_type"] not in EVENT_TYPES:
        raise ValueError(f"unknown event_type: {env['event_type']!r}")
    if not isinstance(env["payload"], dict):
        raise ValueError("payload is not a JSON object")
    env.setdefault("occurred_at", now_ms())
    return env
