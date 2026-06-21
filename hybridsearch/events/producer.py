"""Kafka producer for `image-events`. Keyed by image_id so an image's 1st/2nd
events land on the same partition (per-image ordering)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Union

from kafka import KafkaProducer

from .. import config
from .schema import serialize

log = logging.getLogger("hybridsearch.events.producer")


class EventProducer:
    """Thin wrapper: JSON value, image_id key, acks=all. Also used by the consumer
    to publish to the DLQ (`send_raw`)."""

    def __init__(self) -> None:
        if not config.KAFKA_BOOTSTRAP:
            raise RuntimeError("HS_KAFKA_BOOTSTRAP is not set")
        self._p = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
            security_protocol=config.KAFKA_SECURITY_PROTOCOL,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else serialize(v),
            acks="all",
            retries=5,
            linger_ms=20,
        )

    def send(self, env: Dict[str, Any]) -> None:
        # key=image_id -> same partition -> per-image ordering.
        self._p.send(config.KAFKA_TOPIC, key=env["image_id"], value=env)

    def send_raw(self, topic: str, key: str, value: Union[bytes, Dict[str, Any]]) -> None:
        self._p.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._p.flush()

    def close(self) -> None:
        try:
            self._p.flush()
        finally:
            self._p.close()

    def __enter__(self) -> "EventProducer":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
