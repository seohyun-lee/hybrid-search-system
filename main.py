"""Hybrid search API — the Query Coordinator.

Flow per request:
    normalize(q) -> result-cache lookup (hit: return) -> embed q (cached, same
    MiniLM + L2-normalization as the indexing worker) -> OpenSearch hybrid query
    (BM25 `match` on `description` + `knn` on `caption_vector`) fused by the
    `hybrid-pipeline` search pipeline (min-max normalize, weighted mean) -> map
    hits to {image_url, description, score}.

Consistency contract: the coordinator's embedding model AND normalization must
stay 100% identical to the worker's (hybridsearch.embedding.embed). If they drift
the two vector spaces diverge and the kNN half becomes meaningless.

The model is loaded once at startup (warm) and reused; never per request.
Enrichment-pending docs have no `caption_vector`, so kNN simply skips them.

Run:
    uv run uvicorn main:app --host 0.0.0.0 --port 8000
    # then: GET http://localhost:8000/search?q=two+dogs+playing+on+the+beach
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from hybridsearch import config
from hybridsearch.embedding import embed
from hybridsearch.search.client import get_client

log = logging.getLogger("hybridsearch.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm the (heavy) embedding model once so the first real query isn't slow.
    log.info("warming embedding model %s", config.EMBEDDING_MODEL)
    _embed_cached("warmup")
    yield


app = FastAPI(
    title="Hybrid Image Search — Query Coordinator",
    description="BM25 + kNN hybrid search over indexed image captions.",
    version="0.1.0",
    lifespan=lifespan,
)

# One client, reused across requests (embedding model is cached in embed()).
_client = get_client()


class Hit(BaseModel):
    image_id: str
    image_url: Optional[str] = None
    description: Optional[str] = None
    score: float
    width: Optional[int] = None
    height: Optional[int] = None
    source: Optional[str] = None


class SearchResponse(BaseModel):
    query: str
    total: int
    results: List[Hit]


def _normalize(q: str) -> str:
    """Canonical query form for cache keys: lowercased, whitespace-collapsed."""
    return " ".join(q.lower().split())


@lru_cache(maxsize=config.EMBED_CACHE_SIZE)
def _embed_cached(q_norm: str) -> Tuple[float, ...]:
    """Embed once per distinct normalized query; tuple so it's cacheable."""
    return tuple(embed(q_norm))


def _build_hybrid_query(text: str, vector: List[float], k: int) -> Dict[str, Any]:
    """Hybrid query: BM25 on `description` + kNN on `caption_vector`.

    Sub-query order ([BM25, kNN]) must match the pipeline's weights [0.4, 0.6].
    """
    return {
        "hybrid": {
            "queries": [
                {"match": {"description": {"query": text}}},
                {"knn": {"caption_vector": {"vector": vector, "k": k}}},
            ]
        }
    }


def _weighted_pipeline(w_bm25: float, w_knn: float) -> Dict[str, Any]:
    """Inline search pipeline mirroring `hybrid-pipeline` but with custom weights.

    Lets the request tune the BM25:kNN balance without mutating the named pipeline.
    """
    return {
        "phase_results_processors": [
            {
                "normalization-processor": {
                    "normalization": {"technique": "min_max"},
                    "combination": {
                        "technique": "arithmetic_mean",
                        "parameters": {"weights": [w_bm25, w_knn]},
                    },
                }
            }
        ]
    }


def _map_hits(resp: Dict[str, Any], query: str) -> SearchResponse:
    hits = resp.get("hits", {}).get("hits", [])
    results = [
        Hit(
            image_id=h["_source"].get("image_id", h["_id"]),
            image_url=h["_source"].get("image_url"),
            description=h["_source"].get("description"),
            score=h.get("_score", 0.0),
            width=h["_source"].get("width"),
            height=h["_source"].get("height"),
            source=h["_source"].get("source"),
        )
        for h in hits
    ]
    return SearchResponse(query=query, total=len(results), results=results)


@lru_cache(maxsize=config.QUERY_CACHE_SIZE)
def _search_cached(
    q_norm: str, size: int, w_bm25: Optional[float], w_knn: Optional[float]
) -> SearchResponse:
    """Run + map a hybrid search, cached on the normalized (q, k, weights) key.

    When weights are given, use an inline pipeline; otherwise the named one.
    """
    vector = list(_embed_cached(q_norm))
    body: Dict[str, Any] = {
        "size": size,
        "query": _build_hybrid_query(q_norm, vector, k=size),
        # caption_vector is large and not useful in the response — exclude it.
        "_source": {"excludes": ["caption_vector"]},
    }
    if w_bm25 is not None:
        body["search_pipeline"] = _weighted_pipeline(w_bm25, w_knn)
        params = None
    else:
        params = {"search_pipeline": config.SEARCH_PIPELINE}

    resp = _client.search(index=config.INDEX_NAME, body=body, params=params)
    return _map_hits(resp, q_norm)


@app.get("/health")
def health() -> Dict[str, Any]:
    """Liveness + OpenSearch reachability."""
    try:
        ok = _client.ping()
    except Exception as e:  # noqa: BLE001 - report any connection failure as unhealthy
        log.warning("OpenSearch ping failed: %s", e)
        ok = False
    return {"status": "ok" if ok else "degraded", "opensearch": ok}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1, description="Free-text search query"),
    size: int = Query(10, ge=1, le=100, description="Max results to return"),
    w_bm25: Optional[float] = Query(
        None, ge=0.0, le=1.0, description="BM25 (keyword) weight; pair with w_knn"
    ),
    w_knn: Optional[float] = Query(
        None, ge=0.0, le=1.0, description="kNN (semantic) weight; pair with w_bm25"
    ),
) -> SearchResponse:
    q_norm = _normalize(q)
    if not q_norm:
        raise HTTPException(status_code=400, detail="query is empty after normalization")

    # If either weight is supplied, fill the missing one with its default so the
    # inline pipeline always gets both (otherwise stick with the named pipeline).
    if w_bm25 is not None or w_knn is not None:
        w_bm25 = config.BM25_WEIGHT if w_bm25 is None else w_bm25
        w_knn = config.KNN_WEIGHT if w_knn is None else w_knn

    try:
        return _search_cached(q_norm, size, w_bm25, w_knn)
    except Exception as e:  # noqa: BLE001 - surface backend errors as 502
        log.exception("search failed")
        raise HTTPException(status_code=502, detail=f"search backend error: {e}")
