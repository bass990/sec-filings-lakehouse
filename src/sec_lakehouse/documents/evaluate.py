"""Retrieval evaluation that gates index promotion.

Two eval sets, both stored under eval/ so results are reproducible:

1. known-item (eval/known_item.jsonl, generated once per corpus, seeded): a
   query is the first ~25 words of a real chunk; the expected hit is that
   chunk. Measures whether the index can find text it definitely contains.
   recall@1 / recall@5 / MRR.

2. section intent (eval/section_intent.jsonl, hand-written): natural questions
   ("what are the main risk factors?") whose correct answer lives in a known
   10-K item (1a). A hit = any top-5 chunk from the right section of the
   right company. This is closer to what a user types.

Gate (promote_if): the candidate's known-item recall@5 must be >= 0.80 and
section-intent hit@5 >= 0.60, and neither may drop more than 0.02 below the
currently promoted index. Otherwise the candidate is marked rejected and the
pointer does not move. Results are written to docs.index_versions.eval and
eval/reports/<version>.json.
"""
from __future__ import annotations

import json
import random
import sys
import time

from ..config import ROOT, Settings, settings
from . import db
from .embed import Embedder

EVAL_DIR = ROOT / "eval"
KNOWN_ITEM = EVAL_DIR / "known_item.jsonl"
SECTION_INTENT = EVAL_DIR / "section_intent.jsonl"
FLOORS = {"known_item_recall_at_5": 0.80, "section_hit_at_5": 0.60}
MAX_REGRESSION = 0.02


def ensure_known_item_set(conn, n: int = 150, seed: int = 42) -> list[dict]:
    """Seeded sample of real chunks. The set is regenerated (same seed) when the chunk
    set changed underneath it, e.g. after a chunker fix; an eval item whose expected
    chunk no longer exists would otherwise be a guaranteed miss and lie about recall."""
    if KNOWN_ITEM.exists():
        items = [json.loads(line) for line in KNOWN_ITEM.read_text(encoding="utf-8").splitlines() if line.strip()]
        existing = {r[0] for r in conn.execute("SELECT chunk_id FROM docs.chunks WHERE chunk_id = ANY(%s)",
                                               ([i["expected_chunk_id"] for i in items],)).fetchall()}
        stale = [i for i in items if i["expected_chunk_id"] not in existing]
        if not stale:
            return items
        print(f"known-item set: {len(stale)}/{len(items)} expected chunks no longer exist (corpus re-chunked); regenerating with seed {seed}")
        KNOWN_ITEM.unlink()
    rows = conn.execute("SELECT chunk_id, cik, section, text FROM docs.chunks WHERE n_chars > 600 ORDER BY chunk_id").fetchall()
    rng = random.Random(seed)
    sample = rng.sample(rows, min(n, len(rows)))
    items = []
    for cid, cik, section, text in sample:
        words = text.split()
        # take a window from the middle so the query is not the section heading
        start = min(len(words) // 3, max(0, len(words) - 30))
        items.append({"query": " ".join(words[start:start + 25]), "expected_chunk_id": cid, "cik": cik, "section": section})
    EVAL_DIR.mkdir(exist_ok=True)
    KNOWN_ITEM.write_text("\n".join(json.dumps(i) for i in items) + "\n", encoding="utf-8")
    return items


RRF_K = 60
CANDIDATES = 30


def retrieval_mode(conn, version: str) -> str:
    row = conn.execute("SELECT retrieval FROM docs.index_versions WHERE index_version = %s", (version,)).fetchone()
    return row[0] if row else "dense"


def search(conn, embedder: Embedder, version: str, query: str, k: int = 5, cik: int | None = None,
           form: str | None = None, mode: str | None = None) -> list[dict]:
    """Dense (pgvector cosine) or hybrid (dense + Postgres full-text, reciprocal rank fusion) retrieval.

    Hybrid exists because a 384-dim embedding of a query full of numbers or a
    rare proper noun is a poor key; the lexical side catches exactly those, and
    RRF needs no score calibration between the two systems."""
    mode = mode or retrieval_mode(conn, version)
    vec = embedder.encode_query(query)
    filters = ""
    params: dict = {"v": vec, "ver": version, "cik": cik, "form": form, "q": query}
    if cik is not None:
        filters += " AND c.cik = %(cik)s"
    if form is not None:
        filters += " AND d.form = %(form)s"
    dense_sql = f"""SELECT c.chunk_id, 1 - (e.embedding <=> %(v)s::vector) AS score
                    FROM docs.embeddings e JOIN docs.chunks c ON c.chunk_id = e.chunk_id JOIN docs.documents d ON d.adsh = c.adsh
                    WHERE e.index_version = %(ver)s{filters}
                    ORDER BY e.embedding <=> %(v)s::vector LIMIT {CANDIDATES if mode == "hybrid" else k}"""
    dense = conn.execute(dense_sql, params).fetchall()
    ranked: list[tuple[str, float]]
    if mode == "hybrid":
        lex = conn.execute(f"""SELECT c.chunk_id, ts_rank_cd(c.tsv, websearch_to_tsquery('english', %(q)s)) AS score
                               FROM docs.chunks c JOIN docs.documents d ON d.adsh = c.adsh
                               JOIN docs.embeddings e ON e.chunk_id = c.chunk_id AND e.index_version = %(ver)s
                               WHERE c.tsv @@ websearch_to_tsquery('english', %(q)s){filters}
                               ORDER BY score DESC LIMIT {CANDIDATES}""", params).fetchall()
        fused: dict[str, float] = {}
        for rows in (dense, lex):
            for rank, (cid, _) in enumerate(rows, start=1):
                fused[cid] = fused.get(cid, 0.0) + 1.0 / (RRF_K + rank)
        ranked = sorted(fused.items(), key=lambda x: -x[1])[:k]
    else:
        ranked = [(cid, float(s)) for cid, s in dense]
    if not ranked:
        return []
    order = {cid: i for i, (cid, _) in enumerate(ranked)}
    rows = conn.execute("SELECT chunk_id, adsh, cik, section, chunk_index, text FROM docs.chunks WHERE chunk_id = ANY(%s)",
                        ([cid for cid, _ in ranked],)).fetchall()
    rows.sort(key=lambda r: order[r[0]])
    scores = dict(ranked)
    return [dict(chunk_id=r[0], adsh=r[1], cik=r[2], section=r[3], chunk_index=r[4], text=r[5], score=float(scores[r[0]])) for r in rows]


def evaluate_version(conn, embedder: Embedder, version: str) -> dict:
    known = ensure_known_item_set(conn)
    intents = [json.loads(line) for line in SECTION_INTENT.read_text(encoding="utf-8").splitlines() if line.strip()]
    # section intents use 10-K item numbering (Item 1A, 7, ...); 10-Qs number their parts differently,
    # so the intent eval is restricted to companies with a 10-K in the index and searches 10-K chunks only
    with_10k = {r[0] for r in conn.execute("""SELECT DISTINCT c.cik FROM docs.embeddings e JOIN docs.chunks c USING (chunk_id)
                                             JOIN docs.documents d ON d.adsh = c.adsh WHERE e.index_version = %s AND d.form = '10-K'""", (version,))}
    intents = [i for i in intents if i["cik"] in with_10k]
    lat, r1 = [], 0
    r5, rr = 0, 0.0
    for item in known:
        t0 = time.perf_counter()
        hits = search(conn, embedder, version, item["query"], k=5)
        lat.append(time.perf_counter() - t0)
        ids = [h["chunk_id"] for h in hits]
        if ids and ids[0] == item["expected_chunk_id"]:
            r1 += 1
        if item["expected_chunk_id"] in ids:
            r5 += 1
            rr += 1.0 / (ids.index(item["expected_chunk_id"]) + 1)
    sec_hit = 0
    for item in intents:
        hits = search(conn, embedder, version, item["query"], k=5, cik=item["cik"], form="10-K")
        if any(h["section"] == item["expected_section"] for h in hits):
            sec_hit += 1
    lat.sort()
    n_k, n_i = max(len(known), 1), max(len(intents), 1)
    return {
        "index_version": version, "retrieval": retrieval_mode(conn, version), "n_known_item": len(known), "n_section_intent": len(intents),
        "known_item_recall_at_1": round(r1 / n_k, 4), "known_item_recall_at_5": round(r5 / n_k, 4), "known_item_mrr": round(rr / n_k, 4),
        "section_hit_at_5": round(sec_hit / n_i, 4) if intents else None,
        "latency_p50_ms": round(1000 * lat[len(lat) // 2], 1) if lat else None,
        "latency_p95_ms": round(1000 * lat[int(len(lat) * 0.95)], 1) if lat else None,
    }


def promote_if(candidate: dict, incumbent: dict | None) -> tuple[bool, list[str]]:
    reasons = []
    for metric, floor in FLOORS.items():
        v = candidate.get(metric)
        if v is None:
            continue
        if v < floor:
            reasons.append(f"{metric}={v} below floor {floor}")
        if incumbent and incumbent.get(metric) is not None and v < incumbent[metric] - MAX_REGRESSION:
            reasons.append(f"{metric}={v} regressed vs promoted {incumbent[metric]}")
    return (not reasons), reasons


def run(cfg: Settings = settings, version: str | None = None, embedder: Embedder | None = None) -> dict:
    db.init_schema(cfg)  # idempotent migrations (e.g. the tsvector column) before any query
    with db.tx(cfg) as conn:
        if version is None:
            row = conn.execute("SELECT index_version FROM docs.index_versions WHERE status = 'candidate' ORDER BY created_at DESC LIMIT 1").fetchone()
            if not row:
                raise RuntimeError("no candidate index to evaluate; run documents.embed first")
            version = row[0]
        model = conn.execute("SELECT model FROM docs.index_versions WHERE index_version = %s", (version,)).fetchone()[0]
        embedder = embedder or Embedder(model)
        t0 = time.perf_counter()
        result = evaluate_version(conn, embedder, version)
        promoted = db.promoted_version(conn)
        incumbent = None
        if promoted:
            incumbent = conn.execute("SELECT eval FROM docs.index_versions WHERE index_version = %s", (promoted,)).fetchone()[0]
        ok, reasons = promote_if(result, incumbent)
        result.update({"promoted": ok, "reasons": reasons, "incumbent": promoted, "eval_seconds": round(time.perf_counter() - t0, 1)})
        conn.execute("UPDATE docs.index_versions SET eval = %s, status = %s, decided_at = now() WHERE index_version = %s",
                     (json.dumps(result), "promoted" if ok else "rejected", version))
        if ok:
            if promoted:
                conn.execute("UPDATE docs.index_versions SET status = 'superseded' WHERE index_version = %s", (promoted,))
            conn.execute("UPDATE docs.index_pointer SET index_version = %s, moved_at = now() WHERE name = 'promoted'", (version,))
        db.record_cost(conn, "documents.evaluate", "queries", result["n_known_item"] + result["n_section_intent"], 0.0,
                       {"index_version": version, "promoted": ok})
    out = EVAL_DIR / "reports" / f"{version}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return result


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default=None)
    args = ap.parse_args(argv)
    run(version=args.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
