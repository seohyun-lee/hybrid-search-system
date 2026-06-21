from __future__ import annotations

import argparse
import io

from datasets import load_dataset
from PIL import Image

import config
from embedding import embed
from indexer import index_basic, index_enrichment
from opensearch_index import get_client, setup
from storage import LocalStorage


def to_jpeg_bytes(img: Image.Image) -> bytes:
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def derive_key(ex: dict) -> str:
    fn = ex.get("filename")
    if fn:
        return fn if fn.lower().endswith((".jpg", ".jpeg", ".png")) else f"{fn}.jpg"
    return f"{ex.get('img_id', 'unknown')}.jpg"


def run(limit: int, recreate: bool) -> None:
    setup(recreate=recreate)                       # 인덱스 + 하이브리드 파이프라인
    client = get_client()
    storage = LocalStorage(config.IMAGE_DIR, config.IMAGE_BASE_URL)

    # flickr30k: 전부 test split(31K)에 들어있음. streaming 으로 limit 만큼만.
    # (스크립트 신뢰 요구 시 trust_remote_code=True 추가)
    ds = load_dataset("nlphuji/flickr30k", split="test", streaming=True)

    n = 0
    for ex in ds:
        if n >= limit:
            break
        captions = ex.get("caption") or []
        if not captions:                           # 캡션 없는 행 스킵
            continue

        key = derive_key(ex)
        doc_id = key.rsplit(".", 1)[0]
        img = ex["image"]

        # --- 1차 색인: 이미지 → 스토리지(dir) → URL + 기본 메타데이터 ---
        url = storage.put(key, to_jpeg_bytes(img))
        index_basic(
            client, doc_id,
            image_key=key, image_url=url,
            width=img.width, height=img.height, status="stored",
        )

        # --- 2차 색인: 캡션(BM25) + 임베딩(kNN) ---
        description = " ".join(captions)           # 5개 합쳐 BM25 recall ↑
        vector = embed(captions[0])                # 대표 캡션 1개 임베딩
        index_enrichment(client, doc_id, description=description, vector=vector)

        n += 1
        if n % 500 == 0:
            print(f"indexed {n}")

    client.indices.refresh(index=config.INDEX_NAME)
    print(f"done: {n} docs in '{config.INDEX_NAME}', images at {config.IMAGE_DIR}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=10_000)
    p.add_argument("--recreate", action="store_true")
    run(**vars(p.parse_args()))
