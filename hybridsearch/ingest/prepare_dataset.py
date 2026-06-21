"""Stage 0 — data preparation.

Stream flickr30k, store each image via the ObjectStorage backend (local dir now,
S3 later), and emit one manifest record per image. The manifest is the canonical
source data that the event producer will later split into:

  * 1st index event  (basic metadata): image_id, image_url, width, height, source
  * 2nd index event  (enrichment)    : description, captions  -> embedding

Run:
    uv run python -m hybridsearch.ingest.prepare_dataset --limit 10000

Idempotent & resumable: image_ids already present in the manifest are skipped, so
re-running continues where it left off.
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterator, Set

from datasets import load_dataset

from .. import config
from ..storage import get_storage

log = logging.getLogger("prepare_dataset")


def _image_id(example: dict) -> str:
    """Stable id from the flickr filename (e.g. '1000092795.jpg' -> '1000092795')."""
    filename = example.get("filename") or ""
    stem = Path(filename).stem
    return stem or str(example.get("img_id", "")).strip()


def _description(captions) -> str:
    """flickr30k gives 5 captions per image; join them for richer BM25 recall."""
    if isinstance(captions, str):
        captions = [captions]
    parts = [c.strip() for c in (captions or []) if c and c.strip()]
    return " ".join(parts)


def _load_seen_ids(manifest_path: Path) -> Set[str]:
    seen: Set[str] = set()
    if not manifest_path.exists():
        return seen
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                seen.add(json.loads(line)["image_id"])
            except (json.JSONDecodeError, KeyError):
                continue
    return seen


def prepare(limit: int) -> int:
    storage = get_storage()
    config.MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    seen = _load_seen_ids(config.MANIFEST_PATH)
    log.info("Resuming with %d image_ids already in manifest", len(seen))

    ds: Iterator[dict] = load_dataset(
        config.DATASET_NAME, split=config.DATASET_SPLIT, streaming=True
    )

    written = 0
    skipped_empty = 0
    with config.MANIFEST_PATH.open("a", encoding="utf-8") as out:
        for example in ds:
            if written + len(seen) >= limit:
                break

            image_id = _image_id(example)
            if not image_id or image_id in seen:
                continue

            description = _description(example.get("caption"))
            if not description:  # filter empty-text rows — useless for hybrid search
                skipped_empty += 1
                continue

            image = example["image"]
            image_url = storage.put_image(image_id, image)

            record = {
                "image_id": image_id,
                "image_url": image_url,
                "width": int(image.width),
                "height": int(image.height),
                "source": config.DATASET_NAME,
                "description": description,
                "captions": example.get("caption"),
            }

            # Persist the full record as a sidecar so the object store is the
            # source of truth: the index can be rebuilt from S3 alone, without
            # re-streaming the dataset or re-running the enrichment model.
            metadata_url = storage.put_metadata(image_id, record)
            record["metadata_url"] = metadata_url

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()
            seen.add(image_id)
            written += 1

            if written % 500 == 0:
                log.info("...%d images prepared", written)

    log.info(
        "Done. wrote=%d skipped_empty=%d total_in_manifest=%d -> %s",
        written, skipped_empty, len(seen), config.MANIFEST_PATH,
    )
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare flickr30k for indexing.")
    parser.add_argument(
        "--limit", type=int, default=config.DEFAULT_LIMIT,
        help="target total number of images in the manifest (default: %(default)s)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="debug logging",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    prepare(args.limit)


if __name__ == "__main__":
    main()
