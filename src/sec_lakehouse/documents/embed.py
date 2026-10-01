"""Build a new, versioned embedding index as a *candidate*.

Incremental by construction: chunk_ids are content hashes, so a chunk that
already has a vector in the currently promoted index (same model) is copied
rather than recomputed. Only new/changed chunks hit the model. The candidate
is never served until evaluate.py promotes it.

Cost is written to the ledger even for the free local model: the number of
chunks and seconds is what you would multiply by a hosted-embedding price.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime

from ..config import Settings, settings
from . import db

# hosted-price equivalent used for the ledger (USD per 1M tokens) so the "what would this cost on
# OpenAI text-embedding-3-small" number is visible even though the local model is free.
HOSTED_PRICE_PER_MTOK = 0.02
CHARS_PER_TOKEN = 4


class Embedder:
    """Thin wrapper so tests can substitute a deterministic fake."""

    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415  (slow import)
        self.model_name = model_name
        self.model = SentenceTransformer(model_name, device="cpu")
        self.dims = self.model.get_sentence_embedding_dimension()

    def encode(self, texts: list[str]) -> list[list[float]]:
        return self.model.encode(texts, batch_size=32, normalize_embeddings=True, show_progress_bar=False).tolist()

    def encode_query(self, text: str) -> list[float]:
        # bge models want an instruction prefix on the query side only
        prefix = "Represent this sentence for searching relevant passages: " if "bge" in self.model_name else ""
        return self.encode([prefix + text])[0]


def new_version_id(model_name: str, retrieval: str = "dense") -> str:
    slug = model_name.split("/")[-1].lower().replace("_", "-")
    return f"{slug}-{retrieval}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def build_candidate(cfg: Settings = settings, embedder: Embedder | None = None, ciks: list[int] | None = None,
                    retrieval: str | None = None) -> str:
    db.init_schema(cfg)
    embedder = embedder or Embedder(cfg.embedding_model)
    retrieval = retrieval or os.getenv("RETRIEVAL_MODE", "hybrid")
    version = new_version_id(embedder.model_name, retrieval)
    t0 = time.perf_counter()
    with db.tx(cfg) as conn:
        where = "" if not ciks else f" WHERE cik IN ({', '.join(map(str, ciks))})"
        all_chunks = conn.execute(f"SELECT chunk_id, text FROM docs.chunks{where} ORDER BY chunk_id").fetchall()
        if not all_chunks:
            raise RuntimeError("no chunks in docs.chunks; run documents.fetch first")
        conn.execute("INSERT INTO docs.index_versions (index_version, model, dims, n_chunks, n_embedded_new, status, retrieval)"
                     " VALUES (%s, %s, %s, %s, 0, 'candidate', %s)", (version, embedder.model_name, embedder.dims, len(all_chunks), retrieval))
        # a vector depends only on (model, chunk text) and chunk_ids are content hashes, so any earlier
        # version built with the same model is a valid source, whatever its status
        source = conn.execute("SELECT index_version FROM docs.index_versions WHERE model = %s AND index_version <> %s"
                              " ORDER BY created_at DESC LIMIT 1", (embedder.model_name, version)).fetchone()
        reused = 0
        if source:
            reused = conn.execute(
                "INSERT INTO docs.embeddings (index_version, chunk_id, embedding)"
                " SELECT %s, e.chunk_id, e.embedding FROM docs.embeddings e"
                " JOIN docs.chunks c ON c.chunk_id = e.chunk_id"
                f" WHERE e.index_version = %s{where.replace('WHERE', 'AND') if where else ''}",
                (version, source[0])).rowcount
        have = {r[0] for r in conn.execute("SELECT chunk_id FROM docs.embeddings WHERE index_version = %s", (version,)).fetchall()}
        todo = [(cid, text) for cid, text in all_chunks if cid not in have]
        n_chars = 0
        with conn.cursor() as cur:
            for i in range(0, len(todo), 64):
                batch = todo[i:i + 64]
                vecs = embedder.encode([t for _, t in batch])
                cur.executemany("INSERT INTO docs.embeddings (index_version, chunk_id, embedding) VALUES (%s, %s, %s)",
                                [(version, cid, vec) for (cid, _), vec in zip(batch, vecs, strict=True)])
                n_chars += sum(len(t) for _, t in batch)
                if (i // 64) % 20 == 0:
                    print(f"  embedded {min(i + 64, len(todo)):,}/{len(todo):,}")
        conn.execute("UPDATE docs.index_versions SET n_embedded_new = %s WHERE index_version = %s", (len(todo), version))
        seconds = time.perf_counter() - t0
        tokens = n_chars / CHARS_PER_TOKEN
        db.record_cost(conn, "documents.embed", "tokens", tokens, tokens / 1e6 * HOSTED_PRICE_PER_MTOK,
                       {"index_version": version, "new": len(todo), "reused": reused, "seconds": round(seconds, 1),
                        "note": "local CPU model is free; usd is the hosted-equivalent price"})
    print(f"candidate {version} ({retrieval}): {len(all_chunks):,} chunks, {len(todo):,} newly embedded, {reused:,} reused, {seconds:.0f}s")
    return version


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("--retrieval", choices=["dense", "hybrid"], default=None, help="default: $RETRIEVAL_MODE or hybrid")
    args = ap.parse_args(argv)
    build_candidate(retrieval=args.retrieval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
