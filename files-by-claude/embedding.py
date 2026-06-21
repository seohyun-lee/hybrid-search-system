from __future__ import annotations

from functools import lru_cache
from typing import List

import config


@lru_cache(maxsize=1)
def _model():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(config.EMBEDDING_MODEL)


def embed(text: str) -> List[float]:
    # cosine 용으로 정규화 → OpenSearch space_type=cosinesimil 와 일관.
    vec = _model().encode(text, normalize_embeddings=True)
    return vec.tolist()


def embed_batch(texts: List[str]) -> List[List[float]]:
    vecs = _model().encode(texts, normalize_embeddings=True, batch_size=64)
    return [v.tolist() for v in vecs]
