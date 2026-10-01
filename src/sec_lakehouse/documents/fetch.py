"""Fetch primary 10-K / 10-Q documents for the tracked companies, land the raw
HTML in bronze (immutable, sha-manifested), and write section-aware chunks to
Postgres. Which filings exist is read from Iceberg `silver.submissions`, so the
document corpus can never reference a filing the facts side does not know.

Idempotent: a filing already in docs.documents with the same raw sha is skipped.
Rate-limited to the SEC's 10 requests/second policy with a wide margin.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import requests
import yaml

from ..config import ROOT, Settings, settings
from ..lakehouse.catalog import catalog
from ..storage import BronzeStore, sha256_file
from . import db
from .chunking import build_chunks, html_to_text

ARCHIVES = "https://www.sec.gov/Archives/edgar/data"


def tracked_companies(path: Path = ROOT / "config" / "companies.yml") -> tuple[list[dict], list[str]]:
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg["companies"], cfg["forms"]


def primary_doc_url(cik: int, adsh: str, instance: str) -> str:
    """sub.txt's `instance` is the inline-XBRL instance name (aapl-20240928_htm.xml);
    the human-readable primary document is the same name with `_htm.xml` -> `.htm`."""
    name = instance.replace("_htm.xml", ".htm")
    return f"{ARCHIVES}/{cik}/{adsh.replace('-', '')}/{name}"


def candidate_filings(cfg: Settings, companies: list[dict], forms: list[str]) -> list[dict]:
    ciks = [c["cik"] for c in companies]
    subs = catalog(cfg).load_table("silver.submissions").scan(
        row_filter=f"cik IN ({', '.join(map(str, ciks))})",
        selected_fields=("adsh", "cik", "name", "form", "filed", "period", "instance"),
    ).to_arrow().to_pylist()
    subs = [s for s in subs if s["form"] in forms and s["instance"]]
    subs.sort(key=lambda s: (forms.index(s["form"]), s["filed"]), reverse=False)
    return subs


def fetch_document(url: str, user_agent: str, dest: Path, retries: int = 4) -> None:
    for attempt in range(retries):
        r = requests.get(url, headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}, timeout=60)
        if r.status_code == 200:
            dest.write_bytes(r.content)
            return
        if r.status_code in (403, 429, 503):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
    raise RuntimeError(f"{url}: gave up after {retries} attempts")


def run(cfg: Settings = settings, limit: int | None = None) -> dict:
    companies, forms = tracked_companies()
    db.init_schema(cfg)
    store = BronzeStore(cfg)
    filings = candidate_filings(cfg, companies, forms)
    limit = limit or cfg.max_filings_per_run
    tmp = cfg.data_dir / "documents"
    tmp.mkdir(parents=True, exist_ok=True)
    stats = {"seen": 0, "fetched": 0, "skipped": 0, "chunks": 0, "bytes": 0}
    with db.tx(cfg) as conn:
        known = {r[0]: r[1] for r in conn.execute("SELECT adsh, raw_sha256 FROM docs.documents").fetchall()}
        for s in filings[:limit]:
            stats["seen"] += 1
            url = primary_doc_url(s["cik"], s["adsh"], s["instance"])
            local = tmp / f"{s['adsh']}.htm"
            fetch_document(url, cfg.sec_user_agent, local)
            time.sleep(0.15)  # ~6 req/s, under the SEC's 10/s ceiling
            sha = sha256_file(local)
            if known.get(s["adsh"]) == sha:
                stats["skipped"] += 1
                local.unlink()
                continue
            key = f"{cfg.bronze_prefix}/documents/{s['cik']}/{s['adsh']}/primary.htm"
            store.put_file(local, key, url, extra={"adsh": s["adsh"], "cik": s["cik"], "form": s["form"]})
            text = html_to_text(local.read_bytes())
            chunks = build_chunks(s["adsh"], text)
            conn.execute("DELETE FROM docs.documents WHERE adsh = %s", (s["adsh"],))  # cascades to chunks/embeddings
            conn.execute(
                "INSERT INTO docs.documents (adsh, cik, company, form, filed, period, source_url, bronze_key, raw_sha256, n_chars, n_chunks)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (s["adsh"], s["cik"], s["name"], s["form"], s["filed"], s["period"], url, key, sha, len(text), len(chunks)))
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO docs.chunks (chunk_id, adsh, cik, section, chunk_index, text, n_chars) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [(c.chunk_id, s["adsh"], s["cik"], c.section, c.chunk_index, c.text, len(c.text)) for c in chunks])
            stats["fetched"] += 1
            stats["chunks"] += len(chunks)
            stats["bytes"] += local.stat().st_size
            print(f"{s['name'][:28]:28} {s['form']:5} filed {s['filed']}  {len(text):>9,} chars  {len(chunks):>4} chunks")
            local.unlink()
        db.record_cost(conn, "documents.fetch", "requests", stats["seen"], 0.0, stats)
    print(f"documents: {stats}")
    return stats


def rechunk(cfg: Settings = settings) -> dict:
    """Re-derive chunks for every stored document from its bronze HTML (no SEC traffic).
    Used after a chunker change; documents whose chunk set is unchanged are left alone so their
    embeddings stay valid, changed ones are replaced (their old embeddings cascade-delete)."""
    store = BronzeStore(cfg)
    stats = {"documents": 0, "rechunked": 0, "unchanged": 0, "chunks": 0}
    with db.tx(cfg) as conn:
        docs = conn.execute("SELECT adsh, cik, bronze_key FROM docs.documents ORDER BY adsh").fetchall()
        for adsh, cik, key in docs:
            stats["documents"] += 1
            chunks = build_chunks(adsh, html_to_text(store.get_bytes(key)))
            old = {r[0] for r in conn.execute("SELECT chunk_id FROM docs.chunks WHERE adsh = %s", (adsh,)).fetchall()}
            if old == {c.chunk_id for c in chunks}:
                stats["unchanged"] += 1
                continue
            conn.execute("DELETE FROM docs.chunks WHERE adsh = %s", (adsh,))
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO docs.chunks (chunk_id, adsh, cik, section, chunk_index, text, n_chars) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [(c.chunk_id, adsh, cik, c.section, c.chunk_index, c.text, len(c.text)) for c in chunks])
            conn.execute("UPDATE docs.documents SET n_chunks = %s WHERE adsh = %s", (len(chunks), adsh))
            stats["rechunked"] += 1
            stats["chunks"] += len(chunks)
        db.record_cost(conn, "documents.rechunk", "documents", stats["documents"], 0.0, stats)
    print(f"rechunk: {stats}")
    return stats


def main(argv=None) -> int:
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser(description="fetch + chunk 10-K/10-Q text for tracked companies")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--rechunk", action="store_true", help="re-chunk stored documents from bronze after a chunker change")
    args = ap.parse_args(argv)
    if args.rechunk:
        rechunk()
    else:
        run(limit=args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
