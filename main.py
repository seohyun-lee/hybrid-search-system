"""Hybrid search API.

Exposes the BM25 + kNN hybrid search built by the indexing pipeline over HTTP.
A query is embedded once, then sent to OpenSearch as a `hybrid` query whose two
sub-queries — BM25 on `description` and kNN on `caption_vector` — are fused by the
`hybrid-pipeline` search pipeline (min-max normalize, weighted mean [0.4, 0.6]).

Run:
    uv run uvicorn main:app --host 0.0.0.0 --port 8000
    # then: GET http://localhost:8000/search?q=two+dogs+playing+on+the+beach
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from hybridsearch import config
from hybridsearch.embedding import embed
from hybridsearch.search.client import get_client

log = logging.getLogger("hybridsearch.api")

app = FastAPI(
    title="Hybrid Image Search",
    description="BM25 + kNN hybrid search over the indexed image captions.",
    version="0.1.0",
)

# One client + one (lazily-loaded) embedding model, reused across requests.
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


def hybrid_search(text: str, size: int = 10) -> Dict[str, Any]:
    vector = embed(text)
    body = {
        "size": size,
        "query": _build_hybrid_query(text, vector, k=size),
        # caption_vector is large and not useful in the response — exclude it.
        "_source": {"excludes": ["caption_vector"]},
    }
    return _client.search(
        index=config.INDEX_NAME,
        body=body,
        params={"search_pipeline": config.SEARCH_PIPELINE},
    )


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
) -> SearchResponse:
    try:
        resp = hybrid_search(q, size=size)
    except Exception as e:  # noqa: BLE001 - surface backend errors as 502
        log.exception("search failed")
        raise HTTPException(status_code=502, detail=f"search backend error: {e}")

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
    return SearchResponse(query=q, total=len(results), results=results)
