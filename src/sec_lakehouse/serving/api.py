"""Serving layer.

/health          infra reachability + gold data_quality scorecard + promoted index
/facts           point-in-time facts: what was known about (cik, tag) on `as_of`
/companies/{cik} headline metrics per fiscal period from gold.company_quarter
/restatements    facts whose value changed, filterable by cik
/search          vector search over the promoted index (citations only, no LLM)
/ask             RAG: retrieve from the promoted index, answer with Claude if a key is
                 configured, otherwise return an extractive answer; always with citations
/index           current index version + eval numbers + history
/costs           cost ledger totals per stage

Facts are read from the DuckDB snapshot (sub-second on 7M rows) so the API does
not touch the Iceberg catalog on every request; the snapshot is refreshed by the
pipeline. Everything returned carries the provenance needed to audit it: adsh,
filing date, knowledge date, index version.
"""
from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date
from functools import lru_cache

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from .. import __version__
from ..config import settings
from ..documents import db
from ..documents.embed import Embedder
from ..documents.evaluate import search as vector_search
from ..lakehouse.snapshot import duckdb_path


@lru_cache(maxsize=1)
def embedder() -> Embedder:
    return Embedder(settings.embedding_model)


def ddb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(duckdb_path(settings)), read_only=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="SEC lakehouse API", version=__version__, lifespan=lifespan)


@app.middleware("http")
async def request_meta(request: Request, call_next):
    rid = request.headers.get("x-request-id", uuid.uuid4().hex[:12])
    t0 = time.perf_counter()
    resp = await call_next(request)
    resp.headers["x-request-id"] = rid
    resp.headers["x-response-time-ms"] = f"{(time.perf_counter() - t0) * 1000:.1f}"
    return resp


# ---------------------------------------------------------------- health / meta

@app.get("/health")
def health():
    out = {"status": "ok", "version": __version__, "target": settings.target, "checks": {}}
    try:
        with ddb() as con:
            rows = con.execute("SELECT * FROM gold.data_quality ORDER BY quarter").fetch_arrow_table().to_pylist()
        out["checks"]["gold"] = {"ok": True, "quarters": [{k: (str(v) if isinstance(v, date) else v) for k, v in r.items() if k != "built_at"} for r in rows]}
    except Exception as exc:  # noqa: BLE001
        out["checks"]["gold"] = {"ok": False, "error": str(exc)[:200]}
        out["status"] = "degraded"
    try:
        with db.tx(settings) as conn:
            promoted = db.promoted_version(conn)
            n = conn.execute("SELECT count(*) FROM docs.chunks").fetchone()[0]
        out["checks"]["vector_index"] = {"ok": promoted is not None, "promoted": promoted, "n_chunks": n}
        if promoted is None:
            out["status"] = "degraded"
    except Exception as exc:  # noqa: BLE001
        out["checks"]["vector_index"] = {"ok": False, "error": str(exc)[:200]}
        out["status"] = "degraded"
    return out


@app.get("/index")
def index_info():
    with db.tx(settings) as conn:
        rows = conn.execute("SELECT index_version, model, dims, n_chunks, n_embedded_new, status, eval, created_at, decided_at"
                            " FROM docs.index_versions ORDER BY created_at DESC LIMIT 20").fetchall()
        promoted = db.promoted_version(conn)
    cols = ["index_version", "model", "dims", "n_chunks", "n_embedded_new", "status", "eval", "created_at", "decided_at"]
    return {"promoted": promoted, "history": [dict(zip(cols, r, strict=True)) for r in rows]}


@app.get("/costs")
def costs():
    with db.tx(settings) as conn:
        rows = conn.execute("SELECT stage, count(*), sum(quantity), sum(usd), max(run_at) FROM docs.cost_ledger GROUP BY stage ORDER BY stage").fetchall()
    return {"by_stage": [{"stage": r[0], "runs": r[1], "quantity": r[2], "usd": round(r[3], 4), "last_run": r[4]} for r in rows],
            "total_usd": round(sum(r[3] for r in rows), 4)}


# ---------------------------------------------------------------- facts (point in time)

class FactOut(BaseModel):
    cik: int
    tag: str
    ddate: date
    qtrs: int
    uom: str
    value: float | None
    valid_from: date
    valid_to: date
    version_no: int
    n_versions: int
    is_latest: bool
    adsh: str
    form: str | None
    fy: int | None
    fp: str | None


@app.get("/facts", response_model=list[FactOut])
def facts(cik: int, tag: str, as_of: date | None = None, ddate: date | None = None, limit: int = Query(50, le=500)):
    """Values of `tag` for `cik` as they were known on `as_of` (default: today).
    A restated figure returns the OLD value when as_of predates the restatement."""
    as_of = as_of or date.today()
    sql = """SELECT cik, tag, ddate, qtrs, uom, value, valid_from, valid_to, version_no, n_versions, is_latest, adsh, form, fy, fp
             FROM gold.fact_versions
             WHERE cik = ? AND tag = ? AND coreg IS NULL AND valid_from <= ? AND valid_to > ?"""
    params: list = [cik, tag, as_of, as_of]
    if ddate:
        sql += " AND ddate = ?"
        params.append(ddate)
    sql += " ORDER BY ddate DESC, qtrs LIMIT ?"
    params.append(limit)
    with ddb() as con:
        rows = con.execute(sql, params).fetch_arrow_table().to_pylist()
    if not rows:
        raise HTTPException(404, f"no {tag} facts for cik {cik} known on {as_of}")
    return rows


@app.get("/facts/history")
def fact_history(cik: int, tag: str, ddate: date, qtrs: int = 4):
    """Every reported version of one fact, oldest first: the restatement trail."""
    with ddb() as con:
        rows = con.execute("""SELECT value, valid_from, valid_to, version_no, adsh, form, footnote, taxonomy_version
                              FROM gold.fact_versions WHERE cik = ? AND tag = ? AND ddate = ? AND qtrs = ? AND coreg IS NULL
                              ORDER BY version_no""", [cik, tag, ddate, qtrs]).fetch_arrow_table().to_pylist()
    if not rows:
        raise HTTPException(404, "fact not found")
    return {"cik": cik, "tag": tag, "ddate": ddate, "qtrs": qtrs, "versions": rows}


@app.get("/companies/{cik}")
def company(cik: int):
    with ddb() as con:
        rows = con.execute("SELECT * FROM gold.company_quarter WHERE cik = ? ORDER BY fy DESC, fp DESC", [cik]).fetch_arrow_table().to_pylist()
    if not rows:
        raise HTTPException(404, f"cik {cik} not in gold.company_quarter")
    return {"cik": cik, "company_name": rows[0]["company_name"], "periods": rows}


@app.get("/restatements")
def restatements(cik: int | None = None, min_rel_change: float = 0.0, limit: int = Query(100, le=1000)):
    sql = "SELECT * FROM gold.restatements WHERE abs(coalesce(rel_change, 0)) >= ?"
    params: list = [min_rel_change]
    if cik:
        sql += " AND cik = ?"
        params.append(cik)
    sql += " ORDER BY abs(coalesce(rel_change, 0)) DESC LIMIT ?"
    params.append(limit)
    with ddb() as con:
        return con.execute(sql, params).fetch_arrow_table().to_pylist()


# ---------------------------------------------------------------- retrieval + RAG

class Citation(BaseModel):
    chunk_id: str
    adsh: str
    cik: int
    company: str | None = None
    form: str | None = None
    filed: date | None = None
    section: str
    chunk_index: int
    score: float
    text: str


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=2000)
    cik: int | None = None
    form: str | None = None
    k: int = Field(5, ge=1, le=20)


class AskResponse(BaseModel):
    answer: str
    answer_mode: str  # "llm" | "extractive"
    model: str | None
    index_version: str
    citations: list[Citation]
    usage: dict


def _enrich(conn, hits: list[dict]) -> list[Citation]:
    if not hits:
        return []
    meta = {r[0]: r[1:] for r in conn.execute("SELECT adsh, company, form, filed FROM docs.documents WHERE adsh = ANY(%s)",
                                               ([h["adsh"] for h in hits],)).fetchall()}
    out = []
    for h in hits:
        company, form, filed = meta.get(h["adsh"], (None, None, None))
        out.append(Citation(company=company, form=form, filed=filed, **h))
    return out


@app.get("/search", response_model=list[Citation])
def search(q: str, cik: int | None = None, form: str | None = None, k: int = Query(5, le=20)):
    with db.tx(settings) as conn:
        version = db.promoted_version(conn)
        if not version:
            raise HTTPException(503, "no promoted index; run embed + evaluate")
        return _enrich(conn, vector_search(conn, embedder(), version, q, k=k, cik=cik, form=form))


PRICES = {"claude-haiku-4-5-20251001": (1.0, 5.0), "claude-sonnet-5": (3.0, 15.0)}  # USD per 1M input/output tokens


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    with db.tx(settings) as conn:
        version = db.promoted_version(conn)
        if not version:
            raise HTTPException(503, "no promoted index; run embed + evaluate")
        cites = _enrich(conn, vector_search(conn, embedder(), version, req.question, k=req.k, cik=req.cik, form=req.form))
        if not cites:
            raise HTTPException(404, "nothing retrieved")
        key = os.getenv("ANTHROPIC_API_KEY")
        usage: dict = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0}
        msg = None
        llm_error = None
        if key:
            import anthropic  # noqa: PLC0415
            context = "\n\n".join(f"[{i + 1}] {c.company} {c.form} filed {c.filed}, Item {c.section}:\n{c.text}" for i, c in enumerate(cites))
            try:
                msg = anthropic.Anthropic(api_key=key, max_retries=1).messages.create(
                    model=settings.answer_model, max_tokens=600,
                    system="You answer questions about SEC filings using ONLY the numbered excerpts provided. Cite excerpts as [n]. "
                           "If the excerpts do not contain the answer, say so. Never invent figures.",
                    messages=[{"role": "user", "content": f"Excerpts:\n\n{context}\n\nQuestion: {req.question}"}])
            except anthropic.APIError as exc:  # auth, billing, rate limit, outage: degrade, never fail the request
                llm_error = f"{type(exc).__name__}: {str(exc)[:120]}"
        if msg is not None:
            answer, mode = msg.content[0].text, "llm"
            pin, pout = PRICES.get(settings.answer_model, (3.0, 15.0))
            usage = {"input_tokens": msg.usage.input_tokens, "output_tokens": msg.usage.output_tokens,
                     "usd": round(msg.usage.input_tokens / 1e6 * pin + msg.usage.output_tokens / 1e6 * pout, 6)}
            db.record_cost(conn, "serving.ask", "tokens", usage["input_tokens"] + usage["output_tokens"], usage["usd"],
                           {"model": settings.answer_model, "index_version": version})
        else:
            top = cites[0]
            why = llm_error or "no ANTHROPIC_API_KEY configured"
            answer = (f"({why}; extractive answer) Most relevant passage from {top.company} {top.form} "
                      f"filed {top.filed}, Item {top.section} [1]:\n\n{top.text[:800]}")
            mode = "extractive"
        return AskResponse(answer=answer, answer_mode=mode, model=settings.answer_model if mode == "llm" else None,
                           index_version=version, citations=cites, usage=usage)
