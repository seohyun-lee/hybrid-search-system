"""Central config. Paths and dataset settings, overridable via env vars."""
import os
from pathlib import Path

# repo root = parent of this package
ROOT_DIR = Path(__file__).resolve().parent.parent

# Load a local .env if python-dotenv is available. Real env vars already set in
# the shell win over the file (override=False), so prod/CI config is unaffected.
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT_DIR / ".env", override=False)
except ModuleNotFoundError:
    pass
DATA_DIR = Path(os.getenv("HS_DATA_DIR", ROOT_DIR / "data"))
IMAGES_DIR = Path(os.getenv("HS_IMAGES_DIR", DATA_DIR / "images"))
MANIFEST_PATH = Path(os.getenv("HS_MANIFEST_PATH", DATA_DIR / "manifest.jsonl"))

# Storage: "local" now, "s3" later. Same ObjectStorage interface either way.
STORAGE_BACKEND = os.getenv("HS_STORAGE_BACKEND", "local")
# Base URL prepended to stored object keys. For local dev this is a placeholder
# that the FE/BE will serve from. For S3 leave it as-is (or unset) and S3Storage
# derives the canonical https://<bucket>.s3.<region>.amazonaws.com/<key> URL;
# set it only to front the bucket with a CDN/custom host.
STORAGE_BASE_URL = os.getenv("HS_STORAGE_BASE_URL", "http://localhost:8000/images")
# S3 backend settings (used when HS_STORAGE_BACKEND=s3)
S3_BUCKET = os.getenv("HS_S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_IMAGE_PREFIX = os.getenv("HS_S3_IMAGE_PREFIX", "images")
S3_META_PREFIX = os.getenv("HS_S3_META_PREFIX", "meta")
# AWS region for the S3 client. Empty -> boto3 resolves it from the standard
# chain (AWS_REGION / AWS_DEFAULT_REGION / ~/.aws/config).
S3_REGION = os.getenv("HS_S3_REGION", "") or None

# Dataset
# parquet-based mirror — `nlphuji/flickr30k` is script-based and breaks on datasets>=4
DATASET_NAME = os.getenv("HS_DATASET_NAME", "lmms-lab/flickr30k")
DATASET_SPLIT = os.getenv("HS_DATASET_SPLIT", "test")  # flickr30k ships all rows in `test`
DEFAULT_LIMIT = int(os.getenv("HS_DEFAULT_LIMIT", "10000"))

# OpenSearch
# Auth mode decides how get_client() connects:
#   local -> dockerized OpenSearch, security off, http (dev default)
#   basic -> https + username/password (FGAC master user / secured self-hosted)
#   iam   -> https + AWS SigV4 request signing (AWS Managed OpenSearch; no password)
OPENSEARCH_AUTH = os.getenv("OPENSEARCH_AUTH", "local").lower()
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_USE_SSL = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"
# Managed-domain SigV4 service name: "es" for a provisioned domain, "aoss" for Serverless.
OPENSEARCH_AWS_SERVICE = os.getenv("OPENSEARCH_AWS_SERVICE", "es")
INDEX_NAME = os.getenv("HS_INDEX_NAME", "images")
SEARCH_PIPELINE = os.getenv("HS_SEARCH_PIPELINE", "hybrid-pipeline")

# Embedding
EMBEDDING_MODEL = os.getenv("HS_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = int(os.getenv("HS_EMBEDDING_DIM", "384"))
