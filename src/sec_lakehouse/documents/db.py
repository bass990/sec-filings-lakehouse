"""Postgres (+pgvector) schema for the document side of the platform.

An index version = (embedding model, chunk set, retrieval strategy). `retrieval`
is 'dense' (pgvector cosine only) or 'hybrid' (dense + Postgres full-text,
fused with reciprocal rank fusion). Changing the strategy is a new version
that must pass the same eval as a model change.

Tables
------
documents        one row per fetched filing document (adsh); the bronze key and sha of the raw HTML
chunks           content-addressed text chunks (chunk_id = sha256 of adsh|section|index|text)
index_versions   every embedding index ever built: model, chunk count, status, eval results
embeddings       (index_version, chunk_id) -> vector; a version is an immutable set of rows
index_pointer    single row: which index_version the API serves ("promoted")
cost_ledger      every run that spent compute or tokens, with estimated USD

The promotion rule lives in evaluate.py: an index is built as `candidate`,
evaluated, and only then either `promoted` (the pointer moves) or `rejected`.
The API never reads a candidate. Rolling back = moving the pointer.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime

import psycopg
from pgvector.psycopg import register_vector

from ..config import Settings, settings

DDL = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS docs;

CREATE TABLE IF NOT EXISTS docs.documents (
    adsh          text PRIMARY KEY,
    cik           integer NOT NULL,
    company       text,
    form          text NOT NULL,
    filed         date NOT NULL,
    period        date,
    source_url    text NOT NULL,
    bronze_key    text NOT NULL,
    raw_sha256    text NOT NULL,
    n_chars       integer NOT NULL,
    n_chunks      integer NOT NULL,
    fetched_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS docs.chunks (
    chunk_id      text PRIMARY KEY,
    adsh          text NOT NULL REFERENCES docs.documents(adsh) ON DELETE CASCADE,
    cik           integer NOT NULL,
    section       text NOT NULL,
    chunk_index   integer NOT NULL,
    text          text NOT NULL,
    n_chars       integer NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (adsh, section, chunk_index)
);
CREATE INDEX IF NOT EXISTS chunks_cik_idx ON docs.chunks (cik);
-- lexical side of hybrid retrieval: Postgres full-text search over the same chunks
ALTER TABLE docs.chunks ADD COLUMN IF NOT EXISTS tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', text)) STORED;
CREATE INDEX IF NOT EXISTS chunks_tsv_idx ON docs.chunks USING GIN (tsv);

CREATE TABLE IF NOT EXISTS docs.index_versions (
    index_version text PRIMARY KEY,
    model         text NOT NULL,
    dims          integer NOT NULL,
    n_chunks      integer NOT NULL,
    n_embedded_new integer NOT NULL,
    status        text NOT NULL CHECK (status IN ('candidate', 'promoted', 'rejected', 'superseded')),
    eval          jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    decided_at    timestamptz
);
-- an index version is (embeddings, retrieval strategy); the strategy is part of what gets evaluated and promoted
ALTER TABLE docs.index_versions ADD COLUMN IF NOT EXISTS retrieval text NOT NULL DEFAULT 'dense'
    CHECK (retrieval IN ('dense', 'hybrid'));

CREATE TABLE IF NOT EXISTS docs.embeddings (
    index_version text NOT NULL REFERENCES docs.index_versions(index_version) ON DELETE CASCADE,
    chunk_id      text NOT NULL REFERENCES docs.chunks(chunk_id) ON DELETE CASCADE,
    embedding     vector NOT NULL,
    PRIMARY KEY (index_version, chunk_id)
);

CREATE TABLE IF NOT EXISTS docs.index_pointer (
    name          text PRIMARY KEY,
    index_version text REFERENCES docs.index_versions(index_version),
    moved_at      timestamptz NOT NULL DEFAULT now()
);
INSERT INTO docs.index_pointer (name, index_version) VALUES ('promoted', NULL) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS docs.cost_ledger (
    id            bigserial PRIMARY KEY,
    run_at        timestamptz NOT NULL DEFAULT now(),
    stage         text NOT NULL,
    units         text NOT NULL,
    quantity      double precision NOT NULL,
    usd           double precision NOT NULL,
    detail        jsonb
);
"""


def connect(cfg: Settings = settings) -> psycopg.Connection:
    conn = psycopg.connect(cfg.pg_dsn, autocommit=False)
    register_vector(conn)
    return conn


@contextmanager
def tx(cfg: Settings = settings):
    conn = connect(cfg)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema(cfg: Settings = settings) -> None:
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute(DDL)


def record_cost(conn: psycopg.Connection, stage: str, units: str, quantity: float, usd: float, detail: dict | None = None) -> None:
    conn.execute("INSERT INTO docs.cost_ledger (stage, units, quantity, usd, detail) VALUES (%s, %s, %s, %s, %s)",
                 (stage, units, quantity, usd, json.dumps(detail or {})))


def promoted_version(conn: psycopg.Connection) -> str | None:
    row = conn.execute("SELECT index_version FROM docs.index_pointer WHERE name = 'promoted'").fetchone()
    return row[0] if row else None


def utcnow() -> datetime:
    return datetime.now(UTC)
