"""Bridge between Iceberg (system of record) and DuckDB (the SQL engine dbt uses).

Why not point dbt at Iceberg directly? DuckDB's iceberg extension reads a
table by metadata-file path and, on some platforms, cannot follow pyiceberg's
copy-on-write deletes. Materialising the silver tables into a local DuckDB
file is deterministic, fast (Arrow zero-copy), and gives dbt an engine it
fully supports. Gold models are then published *back* to Iceberg so every
downstream consumer (API, Spark, Athena, BigQuery) reads one catalog.

    silver.* (Iceberg)  --snapshot-->  data/lakehouse.duckdb  --dbt-->  gold.*  --publish-->  gold.* (Iceberg)
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pyarrow as pa

from ..config import Settings, settings
from .catalog import catalog, ensure_table

SILVER_TABLES = ("silver.submissions", "silver.facts", "silver.processed_manifest")
GOLD_TABLES = ("company_quarter", "fact_versions", "restatements", "data_quality")


def duckdb_path(cfg: Settings = settings) -> Path:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    return cfg.data_dir / "lakehouse.duckdb"


def snapshot_silver(cfg: Settings = settings, tables: tuple[str, ...] = SILVER_TABLES) -> dict[str, int]:
    """Copy the current Iceberg silver tables into DuckDB schema `silver`."""
    cat = catalog(cfg)
    counts: dict[str, int] = {}
    with duckdb.connect(str(duckdb_path(cfg))) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        for ident in tables:
            name = ident.split(".")[1]
            arrow = cat.load_table(ident).scan().to_arrow()
            con.register("_src", arrow)
            con.execute(f"CREATE OR REPLACE TABLE silver.{name} AS SELECT * FROM _src")
            con.unregister("_src")
            counts[ident] = len(arrow)
            print(f"snapshot {ident}: {len(arrow):,} rows")
    return counts


def publish_gold(cfg: Settings = settings, tables: tuple[str, ...] = GOLD_TABLES) -> dict[str, int]:
    """Write dbt's gold models from DuckDB into Iceberg `gold.*` (full overwrite: marts are small and rebuilt each run)."""
    cat = catalog(cfg)
    counts: dict[str, int] = {}
    with duckdb.connect(str(duckdb_path(cfg)), read_only=True) as con:
        for name in tables:
            arrow: pa.Table = con.execute(f"SELECT * FROM gold.{name}").fetch_arrow_table()
            arrow = _iceberg_friendly(arrow)
            ident = f"gold.{name}"
            t = ensure_table(cat, ident, arrow.schema)  # pyiceberg assigns fresh field ids from the Arrow schema
            t.overwrite(arrow)
            counts[ident] = len(arrow)
            print(f"publish {ident}: {len(arrow):,} rows")
    return counts


def _iceberg_friendly(t: pa.Table) -> pa.Table:
    """Iceberg has no unsigned/large-string/decimal-without-scale distinctions DuckDB may emit; normalise."""
    fields = []
    for f in t.schema:
        typ = f.type
        if pa.types.is_large_string(typ):
            typ = pa.string()
        elif pa.types.is_decimal(typ):
            typ = pa.float64()
        elif pa.types.is_timestamp(typ):
            typ = pa.timestamp("us", tz="UTC") if typ.tz else pa.timestamp("us")  # Iceberg timestamptz must be UTC
        elif pa.types.is_unsigned_integer(typ) or (pa.types.is_integer(typ) and typ.bit_width < 32):
            typ = pa.int64()
        fields.append(pa.field(f.name, typ, nullable=True))
    return t.cast(pa.schema(fields))


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["snapshot", "publish"])
    args = ap.parse_args(argv)
    (snapshot_silver if args.step == "snapshot" else publish_gold)()
    return 0


if __name__ == "__main__":
    sys.exit(main())
