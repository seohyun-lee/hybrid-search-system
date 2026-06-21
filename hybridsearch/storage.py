"""Object storage abstraction.

Today we write images to a local directory. Later, swapping to S3 means
implementing S3Storage and flipping HS_STORAGE_BACKEND=s3 — nothing else in the
pipeline (prepare script / worker) changes, because everyone depends only on the
ObjectStorage interface and the returned URL.
"""
from __future__ import annotations

import abc
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
    """Future: upload to S3, return the object URL. Same interface as LocalStorage."""

    def __init__(self, bucket: str, base_url: str):
        self.bucket = bucket
        self.base_url = base_url.rstrip("/")

    def put_image(self, image_id: str, image: Image.Image) -> str:  # pragma: no cover
        raise NotImplementedError(
            "S3Storage not implemented yet. Set HS_STORAGE_BACKEND=local for now."
        )

    def put_metadata(self, image_id: str, metadata: Dict[str, Any]) -> str:  # pragma: no cover
        raise NotImplementedError(
            "S3Storage not implemented yet. Set HS_STORAGE_BACKEND=local for now."
        )

    def get_metadata(self, image_id: str) -> Optional[Dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    def exists(self, image_id: str) -> bool:  # pragma: no cover
        raise NotImplementedError

    def url_for(self, image_id: str) -> str:  # pragma: no cover
        return f"{self.base_url}/{image_id}.jpg"

    def metadata_url_for(self, image_id: str) -> str:  # pragma: no cover
        return f"{self.base_url}/{image_id}.json"


def get_storage() -> ObjectStorage:
    """Construct the storage backend selected by config."""
    backend = config.STORAGE_BACKEND.lower()
    if backend == "local":
        return LocalStorage(config.IMAGES_DIR, config.STORAGE_BASE_URL)
    if backend == "s3":
        return S3Storage(config.S3_BUCKET, config.STORAGE_BASE_URL)
    raise ValueError(f"Unknown HS_STORAGE_BACKEND: {config.STORAGE_BACKEND!r}")
