"""Stage 1 — index the prepared manifest into OpenSearch.

Reads each manifest record and runs both phases for it:

  1. index_basic       -> image_url + basic metadata
  2. (enrichment) embed the caption, persist the vector back to the storage
     sidecar, then index_enrichment -> description (BM25) + caption_vector (kNN)

This stands in for the real system's event flow (a producer would split each
record into two Kafka events; index_basic / index_enrichment are the consumer
handlers). Here we just call them in sequence.

Model-free reindex: if a record's sidecar already holds a caption_vector (from a
previous run), it is reused instead of re-embedding — the payoff of making
storage the source of truth.

Run:
    uv run python -m hybridsearch.index.run_from_manifest          # index everything
    uv run python -m hybridsearch.index.run_from_manifest --recreate
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterator, Optional

from .. import config
from ..embedding import embed
from ..search.client import get_client
from ..search.index import setup
from ..storage import get_storage
from .worker import index_basic, index_enrichment

log = logging.getLogger("run_from_manifest")


def _iter_manifest(manifest_path: Path) -> Iterator[dict]:
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("skipping malformed manifest line")


def _embed_text(record: dict) -> str:
    """Embed a single representative caption (richer signal than the 5 joined)."""
    captions = record.get("captions")
    if isinstance(captions, list) and captions:
        return captions[0]
    return record.get("description", "")


def index_record(client, storage, record: dict) -> None:
    image_id = record["image_id"]

    # --- 1st phase: basic metadata ---
    index_basic(
        client,
        image_id,
        image_url=record["image_url"],
        image_id=image_id,
        width=record.get("width"),
        height=record.get("height"),
        source=record.get("source"),
    )

    # --- 2nd phase: enrichment (embed -> persist to sidecar -> index) ---
    sidecar = storage.get_metadata(image_id) or {}
    vector: Optional[list] = sidecar.get("caption_vector")
    if vector is None:
        vector = embed(_embed_text(record))
        # Write the vector back so storage stays the source of truth and a future
        # reindex skips the model.
        storage.put_metadata(image_id, {**sidecar, **record, "caption_vector": vector})

    index_enrichment(
        client,
        image_id,
        description=record["description"],
        vector=vector,
    )


def run(recreate: bool) -> int:
    if not config.MANIFEST_PATH.exists():
        raise SystemExit(
            f"manifest not found: {config.MANIFEST_PATH}\n"
            "run `python -m hybridsearch.ingest.prepare_dataset` first."
        )

    setup(recreate=recreate)  # index + hybrid search pipeline
    client = get_client()
    storage = get_storage()

    n = 0
    for record in _iter_manifest(config.MANIFEST_PATH):
        index_record(client, storage, record)
        n += 1
        if n % 500 == 0:
            log.info("...%d records indexed", n)

    client.indices.refresh(index=config.INDEX_NAME)
    log.info("Done. indexed=%d into index '%s'", n, config.INDEX_NAME)
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Index the manifest into OpenSearch.")
    parser.add_argument(
        "--recreate", action="store_true", help="drop & recreate the index first",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(args.recreate)


if __name__ == "__main__":
    main()
