"""Object storage abstraction.

Today we write images to a local directory. Later, swapping to S3 means
implementing S3Storage and flipping HS_STORAGE_BACKEND=s3 — nothing else in the
pipeline (prepare script / worker) changes, because everyone depends only on the
ObjectStorage interface and the returned URL.
"""
from __future__ import annotations

import abc
import io
import os
import json

from pathlib import Path
from typing import Any, Dict, Optional

from PIL import Image

from . import config


class ObjectStorage(abc.ABC):
    """Stores image bytes + a metadata sidecar under a stable key (image_id).

    The image is the blob; the sidecar JSON holds the ML-derived metadata
    (captions, description, dimensions, source...). Persisting both makes the
    object store the source of truth: if the search index is lost it can be
    rebuilt from storage alone — no re-streaming the dataset and, crucially, no
    re-running the (expensive) enrichment model.
    """

    @abc.abstractmethod
    def put_image(self, image_id: str, image: Image.Image) -> str:
        """Persist `image` for `image_id` and return the URL to reach it."""

    @abc.abstractmethod
    def put_metadata(self, image_id: str, metadata: Dict[str, Any]) -> str:
        """Persist the metadata sidecar for `image_id` and return its URL."""

    @abc.abstractmethod
    def get_metadata(self, image_id: str) -> Optional[Dict[str, Any]]:
        """Read back the metadata sidecar for `image_id`, or None if absent.

        Lets indexing rebuild from storage alone — if the sidecar already holds a
        caption_vector, the (expensive) embedding model need not run again."""

    @abc.abstractmethod
    def exists(self, image_id: str) -> bool:
        """True if an object for `image_id` is already stored (for resume/idempotency)."""

    @abc.abstractmethod
    def url_for(self, image_id: str) -> str:
        """The URL an object for `image_id` would have, without storing it."""

    @abc.abstractmethod
    def metadata_url_for(self, image_id: str) -> str:
        """The URL the metadata sidecar for `image_id` would have, without storing it."""


class LocalStorage(ObjectStorage):
    """Writes JPEGs to a local directory. Drop-in stand-in for S3 during dev."""

    def __init__(self, images_dir: Path, base_url: str):
        self.images_dir = Path(images_dir)
        self.base_url = base_url.rstrip("/")
        self.images_dir.mkdir(parents=True, exist_ok=True)

    def _key(self, image_id: str) -> str:
        return f"{image_id}.jpg"

    def _meta_key(self, image_id: str) -> str:
        return f"{image_id}.json"

    def _path(self, image_id: str) -> Path:
        return self.images_dir / self._key(image_id)

    def _meta_path(self, image_id: str) -> Path:
        return self.images_dir / self._meta_key(image_id)

    def url_for(self, image_id: str) -> str:
        return f"{self.base_url}/{self._key(image_id)}"

    def metadata_url_for(self, image_id: str) -> str:
        return f"{self.base_url}/{self._meta_key(image_id)}"

    def exists(self, image_id: str) -> bool:
        return self._path(image_id).exists()

    def put_image(self, image_id: str, image: Image.Image) -> str:
        if image.mode != "RGB":
            image = image.convert("RGB")
        image.save(self._path(image_id), format="JPEG", quality=90)
        return self.url_for(image_id)

    def put_metadata(self, image_id: str, metadata: Dict[str, Any]) -> str:
        with self._meta_path(image_id).open("w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        return self.metadata_url_for(image_id)

    def get_metadata(self, image_id: str) -> Optional[Dict[str, Any]]:
        path = self._meta_path(image_id)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)


class S3Storage(ObjectStorage):
    """Uploads images + metadata sidecars to S3. Same interface as LocalStorage.

    Auth: standard boto3 chain — EC2 instance role on the worker, or AWS_PROFILE
    locally. Bucket is private (BucketOwnerEnforced), so no ACL is set and the
    stored URL is the canonical object URL; serve images via `presigned_url()`.
    Images live under HS_S3_IMAGE_PREFIX (`images/<id>.jpg`), metadata sidecars under
    HS_S3_META_PREFIX (`meta/<id>.json`) so the two can be lifecycled separately.
    """

    def __init__(self, bucket: str, base_url: str = "", region: str | None = None):
        if not bucket:
            raise ValueError("HS_S3_BUCKET is required when HS_STORAGE_BACKEND=s3")

        import boto3
        from botocore.config import Config as BotoConfig

        self.bucket = bucket
        self.region = region or os.getenv("AWS_REGION", "ap-northeast-2")
        self.key_prefix = os.getenv("HS_S3_IMAGE_PREFIX", "images").strip("/")
        # Metadata sidecars live under their own prefix so the image blobs and the
        # ML-derived JSON can be lifecycled / permissioned independently.
        self.meta_prefix = os.getenv("HS_S3_META_PREFIX", "meta").strip("/")
        # The LocalStorage default base_url (localhost) is meaningless for S3, so
        # fall back to deriving the standard S3 object URL unless a real CDN/host
        # was provided.
        self.base_url = base_url.rstrip("/") if base_url and "localhost" not in base_url else ""
        self._s3 = boto3.client(
            "s3", region_name=self.region,
            config=BotoConfig(retries={"max_attempts": 5}),
        )

    def _key(self, image_id: str) -> str:
        name = f"{image_id}.jpg"
        return f"{self.key_prefix}/{name}" if self.key_prefix else name

    def _meta_key(self, image_id: str) -> str:
        name = f"{image_id}.json"
        return f"{self.meta_prefix}/{name}" if self.meta_prefix else name

    def _url_for_key(self, key: str) -> str:
        if self.base_url:
            return f"{self.base_url}/{key}"
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{key}"

    def url_for(self, image_id: str) -> str:
        return self._url_for_key(self._key(image_id))

    def metadata_url_for(self, image_id: str) -> str:
        return self._url_for_key(self._meta_key(image_id))

    def exists(self, image_id: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._s3.head_object(Bucket=self.bucket, Key=self._key(image_id))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def put_image(self, image_id: str, image: Image.Image) -> str:
        if image.mode != "RGB":
            image = image.convert("RGB")
        buf = io.BytesIO()
        image.save(buf, format="JPEG", quality=90)
        self._s3.put_object(
            Bucket=self.bucket,
            Key=self._key(image_id),
            Body=buf.getvalue(),
            ContentType="image/jpeg",
        )
        return self.url_for(image_id)

    def put_metadata(self, image_id: str, metadata: Dict[str, Any]) -> str:
        body = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
        self._s3.put_object(
            Bucket=self.bucket,
            Key=self._meta_key(image_id),
            Body=body,
            ContentType="application/json",
        )
        return self.metadata_url_for(image_id)

    def get_metadata(self, image_id: str) -> Optional[Dict[str, Any]]:
        from botocore.exceptions import ClientError

        try:
            obj = self._s3.get_object(Bucket=self.bucket, Key=self._meta_key(image_id))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return json.loads(obj["Body"].read())

    def presigned_url(self, image_id: str, expires: int = 3600) -> str:
        """Time-limited GET URL for the private object (FE/BE use this to display)."""
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": self._key(image_id)},
            ExpiresIn=expires,
        )


def get_storage() -> ObjectStorage:
    """Construct the storage backend selected by config."""
    backend = config.STORAGE_BACKEND.lower()
    if backend == "local":
        return LocalStorage(config.IMAGES_DIR, config.STORAGE_BASE_URL)
    if backend == "s3":
        return S3Storage(config.S3_BUCKET, config.STORAGE_BASE_URL, config.S3_REGION)
    raise ValueError(f"Unknown HS_STORAGE_BACKEND: {config.STORAGE_BACKEND!r}")
