"""Replay the manifest onto Kafka as events: one ImageCreated + one
ImageEnriched per record, keyed by image_id. Stands in for the real producers
(upload service emits ImageCreated; captioning job emits ImageEnriched).

    uv run python -m hybridsearch.events.publish_from_manifest [--limit N]

The consumer worker (hybridsearch.events.consumer) then indexes them.
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterator, Optional

from .. import config
from .admin import ensure_topics
from .producer import EventProducer
from .schema import image_created, image_enriched

log = logging.getLogger("publish_from_manifest")


def _iter_manifest(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("skipping malformed manifest line")


def _image_key(image_id: str) -> str:
    prefix = config.S3_IMAGE_PREFIX.strip("/")
    return f"{prefix}/{image_id}.jpg" if prefix else f"{image_id}.jpg"


def run(limit: Optional[int] = None) -> int:
    if not config.MANIFEST_PATH.exists():
        raise SystemExit(
            f"manifest not found: {config.MANIFEST_PATH}\n"
            "run `python -m hybridsearch.ingest.prepare_dataset` first."
        )
    ensure_topics()
    n = 0
    with EventProducer() as producer:
        for record in _iter_manifest(config.MANIFEST_PATH):
            image_id = record["image_id"]
            producer.send(
                image_created(
                    image_id,
                    image_key=_image_key(image_id),
                    image_url=record["image_url"],
                    width=record.get("width"),
                    height=record.get("height"),
                    status="stored",
                    source=record.get("source"),
                )
            )
            producer.send(image_enriched(image_id, description=record["description"]))
            n += 1
            if n % 500 == 0:
                producer.flush()
                log.info("...%d images published (%d events)", n, n * 2)
            if limit is not None and n >= limit:
                break
    log.info("Done. published %d images -> %d events to '%s'", n, n * 2, config.KAFKA_TOPIC)
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish manifest records as Kafka events.")
    ap.add_argument("--limit", type=int, default=None, help="max images to publish")
    ap.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.limit)


if __name__ == "__main__":
    main()
