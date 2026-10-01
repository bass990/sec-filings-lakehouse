"""Iceberg catalog access, identical across targets.

local  -> REST catalog (docker compose) over MinIO
aws    -> Glue catalog over S3
azure  -> REST catalog over ADLS (or Unity when Databricks is the engine)
gcp    -> BigLake Iceberg catalog over GCS

Callers ask for `catalog()` and get a pyiceberg Catalog; table identifiers are
"<layer>.<table>" (silver.facts, gold.company_quarter). Every table is
created through `ensure_table` with an explicit schema and partition spec so
the physical layout is code, not something an engine guessed.
"""
from __future__ import annotations

from functools import lru_cache

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

from ..config import Settings, settings


@lru_cache(maxsize=1)
def catalog(cfg: Settings = settings) -> Catalog:
    if cfg.target == "local":
        return load_catalog("local", **{
            "type": "rest", "uri": cfg.iceberg_rest_uri, "warehouse": cfg.warehouse,
            "s3.endpoint": cfg.s3_endpoint_url, "s3.access-key-id": cfg.s3_access_key,
            "s3.secret-access-key": cfg.s3_secret_key, "s3.path-style-access": "true", "s3.region": "us-east-1",
        })
    if cfg.target == "aws":
        return load_catalog("glue", **{"type": "glue", "glue.region": cfg.aws_region, "warehouse": cfg.warehouse})
    # Azure and GCP: no managed Iceberg REST catalog is provisioned, so table metadata lives in
    # pyiceberg's SQL catalog (the same Postgres that holds the vector index) and data files in
    # ADLS / GCS. Engines that read Iceberg by metadata location (Spark, Trino, BigLake) work as is.
    if cfg.target == "azure":
        return load_catalog("azure", **{
            "type": "sql", "uri": cfg.pg_dsn.replace("postgresql://", "postgresql+psycopg://"), "init_catalog_tables": "true",
            "warehouse": cfg.warehouse,
            "adls.account-name": cfg.azure_storage_account, "adls.account-key": cfg.azure_storage_key,
        })
    if cfg.target == "gcp":
        return load_catalog("gcp", **{
            "type": "sql", "uri": cfg.pg_dsn.replace("postgresql://", "postgresql+psycopg://"),
            "warehouse": cfg.warehouse, "gcs.project-id": cfg.gcp_project or "", "init_catalog_tables": "true",
        })
    raise ValueError(f"unknown target {cfg.target!r}")


def ensure_namespace(cat: Catalog, ns: str) -> None:
    try:
        cat.create_namespace(ns)
    except NamespaceAlreadyExistsError:
        pass


def ensure_table(cat: Catalog, identifier: str, schema: Schema | pa.Schema, partition_by: list[str] | None = None) -> Table:
    ns = identifier.split(".")[0]
    ensure_namespace(cat, ns)
    try:
        return cat.load_table(identifier)
    except NoSuchTableError:
        spec = PartitionSpec()
        if partition_by:
            fields = []
            for i, name in enumerate(partition_by, start=1000):
                src = schema.find_field(name)
                fields.append(PartitionField(source_id=src.field_id, field_id=i, transform=IdentityTransform(), name=name))
            spec = PartitionSpec(*fields)
        return cat.create_table(identifier, schema=schema, partition_spec=spec)


def arrow_to_schema(table: pa.Table) -> Schema:
    """Derive an Iceberg schema from an Arrow table (used for tables whose shape is data-driven)."""
    from pyiceberg.io.pyarrow import pyarrow_to_schema  # noqa: PLC0415
    return pyarrow_to_schema(table.schema)
