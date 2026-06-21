# Hybrid Search System

이미지 캡션에 대한 **하이브리드 검색**(BM25 키워드 + kNN 시맨틱) 시스템. flickr30k를
받아 이미지는 오브젝트 스토리지(**실제 백엔드는 S3**)에 저장하고, 캡션은 OpenSearch에
색인해 키워드 + 의미 기반 검색을 함께 제공한다. 로컬 디렉터리 백엔드는 **테스트용**으로,
S3 없이 같은 파이프라인을 돌려보기 위한 대체물이다.

## 아키텍처 / 데이터 흐름

```
flickr30k (HF, streaming)
      │
      ▼
[ Stage 0 ] prepare_dataset      이미지 + 메타데이터 사이드카 저장 → 스토리지
      │                          레코드 1줄씩 기록            → data/manifest.jsonl
      ▼
data/manifest.jsonl  (canonical source / 이벤트 피드)
      │
      ▼
[ Stage 1 ] run_from_manifest    각 레코드를 2단계로 색인:
      │                            index_basic      → 이미지 URL + 기본 메타 (1차 이벤트)
      │                            index_enrichment → 캡션(BM25) + 임베딩(kNN) (2차 이벤트)
      ▼
OpenSearch index `images`  +  hybrid search pipeline
```

- **스토리지가 source of truth.** 이미지 옆에 메타데이터 JSON(사이드카)을 같이 저장하고,
  색인 단계에서 임베딩 벡터를 사이드카에 되써 넣는다. 그래서 인덱스가 날아가도 스토리지만으로
  재색인할 수 있고, 비싼 임베딩 모델을 다시 돌릴 필요가 없다.
- **2단계 색인**(`index_basic` / `index_enrichment`)은 실제 시스템의 두 Kafka 이벤트를 모델링한
  것. 둘 다 `doc_as_upsert`라 멱등하고 순서에 무관하며, 서로 겹치지 않는 필드만 쓴다.
- `manifest`는 항상 로컬 파일(스토리지 백엔드와 무관). 이미지/사이드카만 백엔드로 나간다.

## 프로젝트 구조

```
hybridsearch/
  config.py                  중앙 설정 (전부 환경변수로 override 가능, .env 자동 로드)
  storage.py                 ObjectStorage 추상 + LocalStorage / S3Storage + get_storage()
  embedding.py               all-MiniLM-L6-v2 (384d, 정규화)
  ingest/
    prepare_dataset.py       Stage 0: 데이터셋 → 스토리지 + manifest
  search/
    client.py                OpenSearch 클라이언트 팩토리
    index.py                 인덱스 매핑(text + knn_vector) + 하이브리드 search pipeline
  index/
    worker.py                index_basic / index_enrichment (2단계 색인)
    run_from_manifest.py     Stage 1: manifest → OpenSearch 색인 드라이버
docker-compose.yml           로컬 OpenSearch (보안 비활성, 개발용)
files-by-claude/             참고용 스캐폴드 (현재 패키지가 이걸 기반으로 발전)
```

## 사전 준비

```bash
# 의존성 설치 (uv 사용)
uv sync
```

## 사용법

### 1) OpenSearch 띄우기

```bash
docker compose up -d
export OPENSEARCH_USE_SSL=false        # 개발용 컨테이너는 보안 비활성
# 준비 확인 (status: green/yellow 나오면 OK)
curl -s localhost:9200/_cluster/health
```

### 2) 데이터 준비 (Stage 0)

```bash
# 작은 수량으로 로컬 테스트 — data/images/ 에 떨어짐 (S3 안 씀)
HS_STORAGE_BACKEND=local uv run python -m hybridsearch.ingest.prepare_dataset --limit 50

# 실제 적재 (S3) — .env의 HS_S3_BUCKET 이 가리키는 버킷으로 업로드, 기본 10,000장
uv run python -m hybridsearch.ingest.prepare_dataset --limit 10000
```

- `--limit`은 "manifest 총량 목표"라 **재실행하면 이어받기**(resumable)된다. 캡션 없는 행은 건너뛴다.
- flickr30k(`test` split)는 총 약 **31,783장**이 상한. parquet에 이미지 바이너리가 들어 있어
  streaming이어도 받는 데이터량이 적지 않다(1만 장 ≈ 수 GB).

### 3) OpenSearch 색인 (Stage 1)

```bash
# 인덱스 + 하이브리드 파이프라인 생성 후, manifest 전체 색인
uv run python -m hybridsearch.index.run_from_manifest --recreate

# 인덱스를 지우지 않고 추가 색인만
uv run python -m hybridsearch.index.run_from_manifest
```

- 사이드카에 `caption_vector`가 이미 있으면 **재임베딩 없이 재사용**한다(모델 재실행 불필요).
- 인덱스 매핑/파이프라인만 따로 만들고 싶으면:
  `uv run python -m hybridsearch.search.index --recreate`

### 4) 이미지 확인용 정적 서버 (local 백엔드일 때)

```bash
python -m http.server 8000 -d ./data/images   # image_url = http://localhost:8000/images/<key>
```

## 스토리지 백엔드

| 백엔드 | 용도 | 이미지/사이드카 저장 위치 | 설정 |
|--------|------|---------------------------|------|
| `s3` | **실제 적재** | `s3://<bucket>/images/{id}.jpg` + `meta/{id}.json` | `HS_STORAGE_BACKEND=s3` + 아래 S3 변수 |
| `local` | **테스트용** | `data/images/{id}.jpg` + `{id}.json` | `HS_STORAGE_BACKEND=local` |

로컬 디렉터리는 S3 없이 파이프라인을 돌려보기 위한 대체물이다. 두 백엔드는 동일한
`ObjectStorage` 인터페이스라 호출부 코드는 그대로고, 백엔드만 바꾸면 된다.

> ⚠️ **주의:** S3 백엔드는 `prepare`가 **실제 S3 버킷에 업로드**한다. `.env`의
> `HS_S3_BUCKET`이 올바른 버킷을 가리키는지 먼저 확인할 것. 빠르게 동작만 확인할 때는
> S3를 건드리지 않도록 `HS_STORAGE_BACKEND=local`을 명시하는 게 안전하다.

## OpenSearch 연결 (`OPENSEARCH_AUTH`)

`get_client()`가 `OPENSEARCH_AUTH` 값에 따라 접속/인증 방식을 고른다. 색인·검색 코드는
그대로고 **이 설정만 바꾸면 로컬 도커 ↔ AWS Managed OpenSearch로 전환**된다.

| 모드 | 용도 | 연결 | 인증 |
|------|------|------|------|
| `basic` | **AWS Managed (운영)** | HTTPS:443 | 마스터 유저 ID/PW (FGAC) |
| `iam` | AWS Managed (대안) | HTTPS:443 | AWS SigV4 서명 (비번 없음, EC2 롤 등) |
| `local` (기본) | 로컬 도커 (개발) | http:9200 | 보안 비활성 |

### AWS Managed 연결 (basic) — `.env`

```dotenv
OPENSEARCH_AUTH=basic
OPENSEARCH_HOST=search-xxxxx.ap-northeast-2.es.amazonaws.com   # https:// 빼고 호스트만
OPENSEARCH_PORT=443                                            # ⚠️ 필수: 기본값 9200 아님
OPENSEARCH_USER=<master-user>
OPENSEARCH_PASSWORD=<master-password>
```

> `basic`/`iam` 모두 HTTPS라 **포트를 443으로 둬야 한다**(기본값 9200은 로컬 도커용).
> 자격증명은 코드에 두지 말고 `.env`(git 미추적) 또는 시크릿으로 주입할 것.
>
> `iam` 방식은 비밀번호 없이 SigV4로 서명한다. 대신 EC2 인스턴스 롤에 `es:ESHttp*` 권한 +
> 도메인 액세스 정책이 필요하고, `.env`는 `OPENSEARCH_AUTH=iam` + `OPENSEARCH_HOST` +
> `AWS_REGION`만 있으면 된다(Serverless는 `OPENSEARCH_AWS_SERVICE=aoss`).

## 설정 (환경변수)

`hybridsearch/config.py`의 모든 값은 환경변수로 덮어쓸 수 있고, 루트 `.env`가 있으면 자동
로드된다(이미 셸에 설정된 실제 환경변수가 `.env`보다 우선).

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `HS_STORAGE_BACKEND` | `local` | `local` / `s3` |
| `HS_DATA_DIR` | `./data` | 데이터 루트 |
| `HS_IMAGES_DIR` | `./data/images` | local 백엔드 이미지 디렉터리 |
| `HS_MANIFEST_PATH` | `./data/manifest.jsonl` | manifest 경로 (항상 로컬) |
| `HS_STORAGE_BASE_URL` | `http://localhost:8000/images` | local 서빙 주소 / S3는 비워두면 자동 유도 |
| `HS_S3_BUCKET` | (빈값) | S3 버킷명 |
| `AWS_REGION` | `ap-northeast-2` | S3 / SigV4 리전 |
| `AWS_ACCESS_KEY_ID` | (빈값) | IAM 액세스 키. 비우면 boto3 기본 체인(`~/.aws` / EC2 롤) 사용 |
| `AWS_SECRET_ACCESS_KEY` | (빈값) | IAM 시크릿 키. `AWS_ACCESS_KEY_ID`와 한 쌍 |
| `AWS_SESSION_TOKEN` | (빈값) | 임시(STS) 자격증명일 때만 |
| `HS_S3_IMAGE_PREFIX` | `images` | S3 이미지 키 prefix |
| `HS_S3_META_PREFIX` | `meta` | S3 메타데이터 키 prefix |
| `HS_DATASET_NAME` | `lmms-lab/flickr30k` | HF 데이터셋 (parquet 미러) |
| `HS_DATASET_SPLIT` | `test` | flickr30k는 전체가 `test`에 있음 |
| `HS_DEFAULT_LIMIT` | `10000` | `--limit` 기본값 |
| `OPENSEARCH_AUTH` | `local` | 접속/인증 방식: `local` / `basic` / `iam` (위 "OpenSearch 연결" 참고) |
| `OPENSEARCH_HOST` / `OPENSEARCH_PORT` | `localhost` / `9200` | OpenSearch 접속 (AWS Managed는 도메인 + `443`) |
| `OPENSEARCH_USER` / `OPENSEARCH_PASSWORD` | `admin` / `admin` | `basic` 모드 인증 (FGAC 마스터 유저) |
| `OPENSEARCH_USE_SSL` | `false` | `local` 모드의 SSL 여부 (`basic`/`iam`은 항상 HTTPS) |
| `OPENSEARCH_AWS_SERVICE` | `es` | `iam` 모드 SigV4 서비스명 (Serverless는 `aoss`) |
| `HS_INDEX_NAME` | `images` | 인덱스 이름 |
| `HS_SEARCH_PIPELINE` | `hybrid-pipeline` | 하이브리드 search pipeline 이름 |
| `HS_EMBEDDING_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | 임베딩 모델 |
| `HS_EMBEDDING_DIM` | `384` | 임베딩 차원 (인덱스 매핑과 일치해야 함) |

### `.env` 예시

전체 항목은 [`.env.example`](.env.example) 참고. 자주 쓰는 조합:

```dotenv
# 로컬 개발 (기본): 로컬 도커 OpenSearch + 로컬 디렉터리 스토리지
HS_STORAGE_BACKEND=local
OPENSEARCH_AUTH=local

# 운영: S3 적재 + AWS Managed OpenSearch (basic)
# HS_STORAGE_BACKEND=s3
# HS_S3_BUCKET=your-bucket
# AWS_REGION=ap-northeast-2
# OPENSEARCH_AUTH=basic
# OPENSEARCH_HOST=search-xxxxx.ap-northeast-2.es.amazonaws.com
# OPENSEARCH_PORT=443
# OPENSEARCH_USER=<master-user>
# OPENSEARCH_PASSWORD=<master-password>
```

## 메모

- kNN 엔진이 `cosinesimil`을 거부하면 `hybridsearch/search/index.py`에서 `space_type`을
  `l2`로 교체한다(임베딩이 정규화돼 있어 순위는 동일). 변경 시 인덱스 재생성(`--recreate`) 필요.
- 개발용 OpenSearch는 `DISABLE_SECURITY_PLUGIN=true`로 보안을 끈다(2.12+는 보안 활성 시
  `OPENSEARCH_INITIAL_ADMIN_PASSWORD`를 요구함).
- 하이브리드 융합: 각 서브쿼리 점수를 min-max 정규화 후 가중 산술평균(BM25:kNN = 0.4:0.6).

## 다음 단계

- 검색(Query Coordinator): `hybrid` 쿼리(match + knn) + `search_pipeline=hybrid-pipeline`로
  실제 검색 엔드포인트 구현.
