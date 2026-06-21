import os

# --- OpenSearch ---
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_USE_SSL = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"

INDEX_NAME = os.getenv("INDEX_NAME", "images")
SEARCH_PIPELINE = os.getenv("SEARCH_PIPELINE", "hybrid-pipeline")

# --- Embedding ---
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = 384

# --- Storage (지금은 로컬 dir = S3 대체) ---
IMAGE_DIR = os.getenv("IMAGE_DIR", "./data/images")
# 로컬에서 이미지 확인용 정적 서버 주소. S3 전환 시 버킷 URL 로 대체됨.
IMAGE_BASE_URL = os.getenv("IMAGE_BASE_URL", "http://localhost:8080")
