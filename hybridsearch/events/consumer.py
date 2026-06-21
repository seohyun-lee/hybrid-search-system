"""Kafka consumer worker: `image-events` -> OpenSearch.

    uv run python -m hybridsearch.events.consumer

Consumer group, scales to #partitions. Delivery = at-least-once: the offset is
committed *after* the OpenSearch upsert succeeds, so a crash re-processes the
message — the worker's idempotent per-phase guarded upsert absorbs the duplicate.

Failure handling (the part that bites in real systems):
  * transient (OpenSearch down / 5xx / timeout) -> exponential backoff retry.
  * permanent (bad schema, unknown type, 4xx mapping error) -> DLQ + commit, so a
    single poison message can't wedge the partition forever.

The key mistake to avoid (cf. PubSub infinite-retry incidents): do NOT classify a
permanent error as transient and retry it forever. Unknown classification here
defaults to permanent -> DLQ, not endless retry.
"""
from __future__ import annotations

import logging
import time

from kafka import KafkaConsumer
from opensearchpy.exceptions import (
    ConnectionError as OSConnectionError,
    ConnectionTimeout,
    TransportError,
)

from .. import config
from ..index.worker import handle_event
from ..search.client import get_client
from ..storage import get_storage
from .producer import EventProducer
from .schema import parse

log = logging.getLogger("hybridsearch.events.consumer")


def _is_transient(exc: Exception) -> bool:
    """Retry-worthy: connection blips and 5xx. Everything else is permanent."""
    if isinstance(exc, (OSConnectionError, ConnectionTimeout)):
        return True
    if isinstance(exc, TransportError):
        status = getattr(exc, "status_code", None)
        return isinstance(status, int) and status >= 500
    return False


def _process_with_retry(client, storage, env, max_retries: int) -> None:
    """Run handle_event, retrying transient errors with backoff. Raises on a
    permanent error or once retries are exhausted (caller routes to DLQ)."""
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            handle_event(client, storage, env)
            return
        except Exception as exc:  # noqa: BLE001
            if _is_transient(exc) and attempt < max_retries:
                log.warning(
                    "transient error (attempt %d/%d): %s; backing off %.1fs",
                    attempt, max_retries, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            raise


def _to_dlq(dlq: EventProducer, msg, reason: str) -> None:
    key = msg.key.decode("utf-8", "replace") if isinstance(msg.key, bytes) else (msg.key or "unknown")
    log.error("-> DLQ (%s): key=%s offset=%d reason=%s", config.KAFKA_DLQ_TOPIC, key, msg.offset, reason)
    dlq.send_raw(
        config.KAFKA_DLQ_TOPIC,
        key=key,
        value={
            "reason": reason,
            "raw": msg.value.decode("utf-8", "replace") if msg.value else None,
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
        },
    )
    dlq.flush()


def run() -> None:
    if not config.KAFKA_BOOTSTRAP:
        raise SystemExit("HS_KAFKA_BOOTSTRAP is not set")
    client = get_client()
    storage = get_storage()
    dlq = EventProducer()
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
        security_protocol=config.KAFKA_SECURITY_PROTOCOL,
        group_id=config.KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,  # manual commit after upsert success / DLQ
        auto_offset_reset="earliest",
        max_poll_records=100,
    )
    log.info(
        "consuming '%s' as group '%s' (DLQ '%s')",
        config.KAFKA_TOPIC, config.KAFKA_CONSUMER_GROUP, config.KAFKA_DLQ_TOPIC,
    )
    try:
        for msg in consumer:
            try:
                env = parse(msg.value)
            except ValueError as e:
                _to_dlq(dlq, msg, f"parse: {e}")  # malformed = permanent
                consumer.commit()
                continue
            try:
                _process_with_retry(client, storage, env, config.KAFKA_MAX_RETRIES)
            except Exception as e:  # noqa: BLE001 - permanent or retries exhausted
                _to_dlq(dlq, msg, f"process: {type(e).__name__}: {e}")
            consumer.commit()
    finally:
        consumer.close()
        dlq.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()


if __name__ == "__main__":
    main()
