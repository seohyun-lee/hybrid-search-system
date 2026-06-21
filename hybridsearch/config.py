"""Central config. Paths and dataset settings, overridable via env vars."""
import os
from pathlib import Path

# repo root = parent of this package
ROOT_DIR = Path(__file__).resolve().parent.parent
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
S3_PREFIX = os.getenv("HS_S3_PREFIX", "images")

# Dataset
# parquet-based mirror — `nlphuji/flickr30k` is script-based and breaks on datasets>=4
DATASET_NAME = os.getenv("HS_DATASET_NAME", "lmms-lab/flickr30k")
DATASET_SPLIT = os.getenv("HS_DATASET_SPLIT", "test")  # flickr30k ships all rows in `test`
DEFAULT_LIMIT = int(os.getenv("HS_DEFAULT_LIMIT", "10000"))
