# 하이브리드 이미지 검색 (mini) — 색인 슬라이스

서현 담당분(스토리지 + OpenSearch 인덱스 + 워커 색인)의 실행 가능한 시작점.
S3는 인터페이스만 맞춰 두고 지금은 로컬 dir(`data/images`)에 저장.

## 구성
- `config.py` — 환경변수 설정
- `storage.py` — `LocalStorage`(dir, 현재) / `S3Storage`(나중, 동일 인터페이스)
- `opensearch_index.py` — 인덱스 매핑(text + knn_vector) + 하이브리드 search pipeline
- `embedding.py` — all-MiniLM-L6-v2 (384d, 정규화)
- `indexer.py` — 워커 코어: `index_basic`(1차), `index_enrichment`(2차)
- `prepare_flickr30k.py` — 10K 로드 → 1차/2차 색인 드라이버

## 실행
```bash
pip install -r requirements.txt

# 1) 로컬 OpenSearch (보안 비활성)
docker compose up -d
# 보안 비활성이므로 인증 불필요:
export OPENSEARCH_USE_SSL=false

# 2) 인덱스 + 파이프라인 생성 (단독 실행도 가능)
python opensearch_index.py --recreate

# 3) 데이터 색인 (이미지는 ./data/images 에 저장됨)
python prepare_flickr30k.py --limit 10000 --recreate

# 4) 이미지 확인용 정적 서버 (image_url = http://localhost:8080/<key>)
python -m http.server 8080 -d ./data/images
```

## 메모
- knn 엔진이 `cosinesimil` 을 거부하면 `opensearch_index.py` 에서 `space_type` 을 `l2` 로 교체(정규화 임베딩이라 순위 동일).
- `space_type` 변경 시 인덱스 재생성(`--recreate`) 필요.
- S3 전환: `prepare_flickr30k.py` 의 `LocalStorage(...)` 를 `S3Storage(bucket=...)` 로 교체. 그 외 변경 없음.
- 1차/2차는 지금 한 루프에서 순차 호출(Kafka 없이). 실 시스템에선 두 Kafka 이벤트로 분리되며, `index_basic`/`index_enrichment` 가 각 consumer 핸들러가 됨.

## 다음 단계
- 검색: `hybrid` 쿼리(match + knn) + `search_pipeline=hybrid-pipeline` 로 Query Coordinator 구현.
