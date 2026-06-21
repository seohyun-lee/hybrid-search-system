"""Unit tests for the per-phase stale-overwrite guard in index/worker.py.

The real guard runs as a painless script inside OpenSearch, so it can't be hit
without a live cluster. A FakeClient mirrors `_GUARD_SCRIPT` against an in-memory
doc, which lets us pin both:

  1. the worker's *wiring* — each phase targets its own ts_field and writes only
     its disjoint fields (the params the worker hands to OpenSearch), and
  2. the guard *semantics* — order-independence + stale-drop — over the scenarios
     it's designed for (in-order, out-of-order, duplicate/stale redelivery).

If you change `_GUARD_SCRIPT` in worker.py, mirror it in FakeClient.update below.
"""
from __future__ import annotations

import pytest

from hybridsearch.events.schema import image_created, image_enriched
from hybridsearch.index import worker


class FakeClient:
    """Stands in for OpenSearch: a Python port of _GUARD_SCRIPT over self.store."""

    def __init__(self) -> None:
        self.store: dict = {}
        self.calls: list = []  # recorded params per update, for wiring assertions

    def update(self, *, index, id, body, **kwargs):
        params = body["script"]["params"]
        ts, ts_field, fields = params["ts"], params["ts_field"], params["fields"]
        self.calls.append({"id": id, "ts_field": ts_field, "ts": ts, "fields": dict(fields)})
        # --- mirror of _GUARD_SCRIPT (worker.py) ---
        doc = self.store.setdefault(id, {})
        cur = doc.get(ts_field)
        if cur is not None and ts < cur:
            return {"result": "noop"}  # stale for this phase -> dropped
        doc.update(fields)
        doc[ts_field] = ts
        if doc.get("ts_enriched") is not None:
            doc["phase"] = "enriched"
        elif doc.get("ts_basic") is not None:
            doc["phase"] = "basic"
        return {"result": "updated"}


class FakeStorage:
    """Sidecar already holds a vector, so handle_event reuses it and never loads
    the embedding model during tests."""

    def __init__(self, vector=None) -> None:
        self._vector = vector if vector is not None else [0.1, 0.2, 0.3, 0.4]
        self.put_calls: list = []

    def get_metadata(self, image_id):
        return {"caption_vector": self._vector}

    def put_metadata(self, image_id, metadata):
        self.put_calls.append((image_id, metadata))


# --- wiring: each phase targets its own ts_field + disjoint fields ---

def test_index_basic_targets_ts_basic_and_only_basic_fields():
    c = FakeClient()
    worker.index_basic(c, "i", image_url="https://x/i.jpg", width=4, height=3, source="ds", event_ts=100)
    call = c.calls[-1]
    assert call["ts_field"] == "ts_basic"
    assert call["fields"]["image_url"] == "https://x/i.jpg"
    assert "description" not in call["fields"] and "caption_vector" not in call["fields"]


def test_index_enrichment_targets_ts_enriched_and_only_enrich_fields():
    c = FakeClient()
    worker.index_enrichment(c, "i", description="a cat", vector=[0.1, 0.2], event_ts=200)
    call = c.calls[-1]
    assert call["ts_field"] == "ts_enriched"
    assert call["fields"]["description"] == "a cat"
    assert call["fields"]["caption_vector"] == [0.1, 0.2]
    assert "image_url" not in call["fields"]


# --- guard semantics ---

def test_in_order_created_then_enriched():
    c = FakeClient()
    worker.index_basic(c, "i", image_url="u", event_ts=100)
    worker.index_enrichment(c, "i", description="d", vector=[1.0], event_ts=200)
    doc = c.store["i"]
    assert doc["image_url"] == "u" and doc["description"] == "d"
    assert doc["ts_basic"] == 100 and doc["ts_enriched"] == 200
    assert doc["phase"] == "enriched"


def test_out_of_order_enriched_first_keeps_both_and_no_downgrade():
    c = FakeClient()
    worker.index_enrichment(c, "i", description="d", vector=[1.0], event_ts=200)
    worker.index_basic(c, "i", image_url="u", event_ts=100)  # later arrival, earlier ts
    doc = c.store["i"]
    assert doc["image_url"] == "u"      # 1st-phase field NOT lost
    assert doc["description"] == "d"    # 2nd-phase field intact
    assert doc["phase"] == "enriched"   # phase not downgraded to basic


def test_stale_duplicate_basic_is_dropped():
    c = FakeClient()
    worker.index_basic(c, "i", image_url="new", event_ts=200)
    worker.index_basic(c, "i", image_url="old", event_ts=100)  # stale redelivery -> noop
    assert c.store["i"]["image_url"] == "new"
    assert c.store["i"]["ts_basic"] == 200


def test_stale_duplicate_enriched_is_dropped():
    c = FakeClient()
    worker.index_enrichment(c, "i", description="new", vector=[1.0], event_ts=200)
    worker.index_enrichment(c, "i", description="old", vector=[0.0], event_ts=100)
    assert c.store["i"]["description"] == "new"
    assert c.store["i"]["ts_enriched"] == 200


def test_equal_ts_reapplies_not_dropped():
    # ts == stored is NOT stale (>=), so an at-least-once duplicate at the same ts
    # is allowed to re-apply idempotently.
    c = FakeClient()
    worker.index_basic(c, "i", image_url="u", event_ts=100)
    worker.index_basic(c, "i", image_url="u", event_ts=100)
    assert c.store["i"]["image_url"] == "u" and c.store["i"]["ts_basic"] == 100


# --- handle_event routing ---

def test_handle_event_routes_image_created():
    c, s = FakeClient(), FakeStorage()
    env = image_created("i", image_key="images/i.jpg", image_url="u", width=4, height=3, source="ds", occurred_at=100)
    worker.handle_event(c, s, env)
    assert c.calls[-1]["ts_field"] == "ts_basic"
    assert c.store["i"]["image_url"] == "u"


def test_handle_event_routes_image_enriched_reuses_sidecar_vector():
    c, s = FakeClient(), FakeStorage(vector=[9.0])
    worker.handle_event(c, s, image_enriched("i", description="d", occurred_at=200))
    assert c.calls[-1]["ts_field"] == "ts_enriched"
    assert c.store["i"]["caption_vector"] == [9.0]  # reused from sidecar, no embed
    assert s.put_calls == []  # vector already present -> nothing written back


def test_handle_event_unknown_type_raises_valueerror():
    c, s = FakeClient(), FakeStorage()
    with pytest.raises(ValueError):
        worker.handle_event(c, s, {"image_id": "i", "event_type": "Nope", "payload": {}, "occurred_at": 1})


def test_handle_event_missing_payload_field_raises_keyerror():
    # Permanent error (-> DLQ in the consumer), not a transient retry.
    c, s = FakeClient(), FakeStorage()
    with pytest.raises(KeyError):
        worker.handle_event(c, s, {"image_id": "i", "event_type": "ImageCreated", "payload": {}, "occurred_at": 1})
