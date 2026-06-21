from __future__ import annotations

from opensearchpy import OpenSearch

import config


def get_client() -> OpenSearch:
    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
        http_auth=(config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD),
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
    )


INDEX_BODY = {
    "settings": {
        "index": {
            "knn": True,                # kNN 검색 활성화
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }
    },
    "mappings": {
        "properties": {
            "image_key": {"type": "keyword"},
            "image_url": {"type": "keyword"},
            "status": {"type": "keyword"},
            "width": {"type": "integer"},
            "height": {"type": "integer"},
            "description": {"type": "text"},          # BM25 (키워드 검색)
            "caption_vector": {                        # kNN (시맨틱 검색)
                "type": "knn_vector",
                "dimension": config.EMBEDDING_DIM,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    # 엔진이 cosinesimil 을 거부하면 "l2" 로 교체.
                    # (임베딩이 정규화돼 있어 l2 와 cosine 순위는 동일)
                    "space_type": "cosinesimil",
                    "parameters": {"ef_construction": 128, "m": 16},
                },
            },
            "phase": {"type": "keyword"},              # basic | enriched
            "event_ts": {"type": "long"},              # 순서/멱등 가드용
            "updated_at": {"type": "date"},
        }
    },
}

# 하이브리드 융합: 각 서브쿼리 점수를 min-max 정규화 후 가중 산술평균.
# weights = [BM25, kNN] = [0.4, 0.6] (시맨틱에 약간 더 무게)
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
    if client.indices.exists(config.INDEX_NAME):
        if not recreate:
            return
        client.indices.delete(config.INDEX_NAME)
    client.indices.create(config.INDEX_NAME, body=INDEX_BODY)


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
