"""Object-store client that is the same code on MinIO, S3, and (via S3-compatible
gateways) the other clouds. Bronze objects are immutable: a key is written
once with a content hash in its manifest; re-running a job that finds the
same hash is a no-op, which is what makes every ingestion idempotent.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import Settings, settings


def s3_client(cfg: Settings = settings):
    if cfg.target == "azure":
        return AdlsClient(cfg)
    if cfg.target == "gcp":
        return GcsClient(cfg)
    kwargs: dict = {"region_name": cfg.aws_region, "config": Config(s3={"addressing_style": "path"} if cfg.s3_endpoint_url else {})}
    if cfg.s3_endpoint_url:
        kwargs.update(endpoint_url=cfg.s3_endpoint_url, aws_access_key_id=cfg.s3_access_key, aws_secret_access_key=cfg.s3_secret_key)
    return boto3.client("s3", **kwargs)


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class GcsClient:
    """Same four calls on Google Cloud Storage (Application Default Credentials)."""

    def __init__(self, cfg: Settings):
        from google.cloud import storage as gcs  # noqa: PLC0415
        self._client = gcs.Client(project=cfg.gcp_project) if cfg.gcp_project else gcs.Client()

    def get_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803
        from google.api_core.exceptions import NotFound  # noqa: PLC0415
        try:
            return {"Body": _Body(self._client.bucket(Bucket).blob(Key).download_as_bytes())}
        except NotFound as exc:
            raise ClientError({"Error": {"Code": "404", "Message": str(exc)}}, "GetObject") from exc

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self._client.bucket(bucket).blob(key).upload_from_filename(filename)

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str | None = None) -> None:  # noqa: N803
        self._client.bucket(Bucket).blob(Key).upload_from_string(Body, content_type=ContentType or "application/octet-stream")

    def list_objects_v2(self, Bucket: str, Prefix: str = "", ContinuationToken: str | None = None) -> dict:  # noqa: N803
        return {"Contents": [{"Key": b.name} for b in self._client.list_blobs(Bucket, prefix=Prefix)]}


class AdlsClient:
    """The four boto3 S3 calls BronzeStore uses, implemented on Azure Blob / ADLS Gen2.

    Keeping the S3 call shape (rather than a new abstraction) means the bronze
    layer's idempotency logic is literally the same code on MinIO, S3 and ADLS.
    Missing blobs raise a botocore ClientError with code 404 so the caller's
    error handling does not change either."""

    def __init__(self, cfg: Settings):
        from azure.storage.blob import BlobServiceClient  # noqa: PLC0415
        if not (cfg.azure_storage_account and cfg.azure_storage_key):
            raise ValueError("LAKEHOUSE_TARGET=azure needs AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY")
        self._svc = BlobServiceClient(f"https://{cfg.azure_storage_account}.blob.core.windows.net", credential=cfg.azure_storage_key)

    def _blob(self, bucket: str, key: str):
        return self._svc.get_blob_client(container=bucket, blob=key)

    def get_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803 - boto3 call shape
        from azure.core.exceptions import ResourceNotFoundError  # noqa: PLC0415
        try:
            return {"Body": _Body(self._blob(Bucket, Key).download_blob().readall())}
        except ResourceNotFoundError as exc:
            raise ClientError({"Error": {"Code": "404", "Message": str(exc)}}, "GetObject") from exc

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        with open(filename, "rb") as f:
            self._blob(bucket, key).upload_blob(f, overwrite=True, max_concurrency=4)

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str | None = None) -> None:  # noqa: N803
        from azure.storage.blob import ContentSettings  # noqa: PLC0415
        self._blob(Bucket, Key).upload_blob(Body, overwrite=True, content_settings=ContentSettings(content_type=ContentType))

    def list_objects_v2(self, Bucket: str, Prefix: str = "", ContinuationToken: str | None = None) -> dict:  # noqa: N803
        names = [b.name for b in self._svc.get_container_client(Bucket).list_blobs(name_starts_with=Prefix)]
        return {"Contents": [{"Key": n} for n in names]}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class PutResult:
    key: str
    sha256: str
    bytes: int
    skipped: bool  # already present with the same hash


class BronzeStore:
    """Write-once landing zone with a JSON manifest next to every object."""

    def __init__(self, cfg: Settings = settings, client=None):
        self.cfg = cfg
        self.client = client or s3_client(cfg)
        self.bucket = cfg.bucket

    def _manifest_key(self, key: str) -> str:
        return key + ".manifest.json"

    def exists_with_hash(self, key: str, sha256: str) -> bool:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=self._manifest_key(key))
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return False
            raise
        return json.loads(obj["Body"].read()).get("sha256") == sha256

    def put_file(self, local: Path, key: str, source_url: str, extra: dict | None = None) -> PutResult:
        digest = sha256_file(local)
        size = local.stat().st_size
        if self.exists_with_hash(key, digest):
            return PutResult(key, digest, size, skipped=True)
        self.client.upload_file(str(local), self.bucket, key)
        manifest = {"key": key, "sha256": digest, "bytes": size, "source_url": source_url,
                    "landed_at": datetime.now(UTC).isoformat(), **(extra or {})}
        self.client.put_object(Bucket=self.bucket, Key=self._manifest_key(key), Body=json.dumps(manifest, indent=1).encode("utf-8"),
                               ContentType="application/json")
        return PutResult(key, digest, size, skipped=False)

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys, token = [], None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            token = resp.get("NextContinuationToken")
            if not token:
                return keys

    def manifests(self, prefix: str) -> list[dict]:
        return [json.loads(self.get_bytes(k)) for k in self.list_keys(prefix) if k.endswith(".manifest.json")]
