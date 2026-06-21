"""Index mapping (BM25 text + kNN vector) and the hybrid search pipeline.

Run standalone to (re)create both:
    uv run python -m hybridsearch.search.index --recreate
"""
from __future__ import annotations

from opensearchpy import OpenSearch

from .. import config
from .client import get_client

INDEX_BODY = {
    "settings": {
        "index": {
            "knn": True,  # enable kNN search
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }
    },
    "mappings": {
        "properties": {
            "image_id": {"type": "keyword"},
            "image_url": {"type": "keyword"},
            "status": {"type": "keyword"},
            "width": {"type": "integer"},
            "height": {"type": "integer"},
            "source": {"type": "keyword"},
            "description": {"type": "text"},  # BM25 (keyword search)
            "caption_vector": {  # kNN (semantic search)
                "type": "knn_vector",
                "dimension": config.EMBEDDING_DIM,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    # If the engine rejects cosinesimil, switch to "l2".
                    # Embeddings are normalized, so l2 and cosine rank identically.
                    "space_type": "cosinesimil",
                    "parameters": {"ef_construction": 128, "m": 16},
                },
            },
            "phase": {"type": "keyword"},  # basic | enriched
            "event_ts": {"type": "long"},  # ordering / idempotency guard
            "updated_at": {"type": "date"},
        }
    },
}

# Hybrid fusion: min-max normalize each sub-query score, then weighted mean.
# weights = [BM25, kNN] = [0.4, 0.6] (lean slightly on semantic).
SEARCH_PIPELINE_BODY = {
    "description": "BM25 + kNN hybrid score normalization",
    "phase_results_processors": [
        {
            "normalization-processor": {
                "normalization": {"technique": "min_max"},
                "combination": {
                    "technique": "arithmetic_mean",
                    "parameters": {"weights": [0.4, 0.6]},
                },
            }
        }
    ],
}


def create_index(client: OpenSearch, recreate: bool = False) -> None:
    if client.indices.exists(index=config.INDEX_NAME):
        if not recreate:
            return
        client.indices.delete(index=config.INDEX_NAME)
    client.indices.create(index=config.INDEX_NAME, body=INDEX_BODY)


def create_search_pipeline(client: OpenSearch) -> None:
    client.transport.perform_request(
        "PUT",
        f"/_search/pipeline/{config.SEARCH_PIPELINE}",
        body=SEARCH_PIPELINE_BODY,
    )


def setup(recreate: bool = False) -> None:
    client = get_client()
    create_index(client, recreate=recreate)
    create_search_pipeline(client)
    print(f"ready: index='{config.INDEX_NAME}', pipeline='{config.SEARCH_PIPELINE}'")


if __name__ == "__main__":
    import sys

    setup(recreate="--recreate" in sys.argv)
