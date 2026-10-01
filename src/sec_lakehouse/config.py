"""Runtime configuration for every stage of the platform.

One `Settings` object, read from the environment (a .env file is loaded if
present). `LAKEHOUSE_TARGET` selects where bytes live; the code paths are
identical across targets, only the object store, catalog and query engine
change (see docs/architecture.md, "portability").
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

TARGETS = ("local", "aws", "azure", "gcp")


@dataclass(frozen=True)
class Settings:
    target: str = os.getenv("LAKEHOUSE_TARGET", "local").strip().lower()
    bucket: str = os.getenv("LAKEHOUSE_BUCKET", "sec-lakehouse")
    sec_user_agent: str = os.getenv("SEC_USER_AGENT", "sec-lakehouse portfolio contact@example.com")

    # object store (MinIO locally; real S3/ADLS/GCS via the target's credentials)
    s3_endpoint_url: str | None = os.getenv("S3_ENDPOINT_URL") or None
    s3_access_key: str | None = os.getenv("MINIO_ROOT_USER") or None
    s3_secret_key: str | None = os.getenv("MINIO_ROOT_PASSWORD") or None
    aws_region: str = os.getenv("AWS_REGION", "us-east-2")

    # iceberg catalog
    iceberg_rest_uri: str = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    glue_database: str = os.getenv("GLUE_DATABASE", "sec_lakehouse")

    # azure (ADLS Gen2; `bucket` is the filesystem/container name)
    azure_storage_account: str | None = os.getenv("AZURE_STORAGE_ACCOUNT") or None
    azure_storage_key: str | None = os.getenv("AZURE_STORAGE_KEY") or None

    # gcp (GCS with Application Default Credentials)
    gcp_project: str | None = os.getenv("GOOGLE_CLOUD_PROJECT") or None

    # postgres + pgvector
    pg_dsn: str = field(default_factory=lambda: (
        f"postgresql://{os.getenv('PG_USER', 'lakehouse')}:{os.getenv('PG_PASSWORD', 'lakehouse-local')}"
        f"@{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5433')}/{os.getenv('PG_DB', 'lakehouse')}"
    ))

    # embeddings / answers
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
    answer_model: str = os.getenv("ANSWER_MODEL", "claude-haiku-4-5-20251001")
    max_filings_per_run: int = int(os.getenv("MAX_FILINGS_PER_RUN", "200"))

    # local scratch
    data_dir: Path = ROOT / "data"

    def __post_init__(self):
        if self.target not in TARGETS:
            raise ValueError(f"LAKEHOUSE_TARGET must be one of {TARGETS}, got {self.target!r}")

    @property
    def bronze_prefix(self) -> str:
        return "bronze"

    @property
    def warehouse(self) -> str:
        if self.target == "azure":
            return f"abfss://{self.bucket}@{self.azure_storage_account}.dfs.core.windows.net/warehouse"
        if self.target == "gcp":
            return f"gs://{self.bucket}/warehouse"
        return f"s3://{self.bucket}/warehouse"


settings = Settings()
