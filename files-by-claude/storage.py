from __future__ import annotations

from pathlib import Path
from typing import Protocol


class Storage(Protocol):
    """이미지 적재 추상화. put() 은 객체 URL 을 반환한다.
    LocalStorage <-> S3Storage 를 갈아끼워도 호출부는 동일하다."""

    def put(self, key: str, data: bytes, content_type: str = "image/jpeg") -> str: ...
    def url_for(self, key: str) -> str: ...


class LocalStorage:
    """S3 대신 로컬 디렉터리에 저장. 데모/개발용.
    base_url 은 dir 을 정적 서빙하는 주소(예: python -m http.server)."""

    def __init__(self, base_dir: str, base_url: str):
        self.base_dir = Path(base_dir)
        self.base_url = base_url.rstrip("/")
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def put(self, key: str, data: bytes, content_type: str = "image/jpeg") -> str:
        path = self.base_dir / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return self.url_for(key)

    def url_for(self, key: str) -> str:
        return f"{self.base_url}/{key}"


class S3Storage:
    """나중에 사용. boto3 필요. LocalStorage 와 동일 인터페이스라 교체만 하면 됨."""

    def __init__(self, bucket: str, prefix: str = "", region: str | None = None):
        import boto3  # lazy import

        self.s3 = boto3.client("s3", region_name=region)
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, key: str, data: bytes, content_type: str = "image/jpeg") -> str:
        self.s3.put_object(
            Bucket=self.bucket, Key=self._full_key(key),
            Body=data, ContentType=content_type,
        )
        return self.url_for(key)

    def url_for(self, key: str) -> str:
        return f"https://{self.bucket}.s3.amazonaws.com/{self._full_key(key)}"
