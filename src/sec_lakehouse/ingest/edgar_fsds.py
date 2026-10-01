"""Ingest the SEC Financial Statement Data Sets (quarterly XBRL bulk files).

Source: https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets
Each quarter is one zip (~80 MB) with four tab-separated tables:

    sub.txt  one row per submission (filing): adsh, cik, name, form, period, filed, ...
    num.txt  one row per numeric fact: adsh, tag, version, ddate, qtrs, uom, value
    pre.txt  presentation: where each tag appears in the statements
    tag.txt  the taxonomy tags used

Why the raw zip is landed unchanged: a quarter's file can be re-published by
the SEC, and later quarters restate earlier facts. Bronze keeps every version
of the bytes with a hash manifest; the silver layer (lakehouse/facts.py)
derives point-in-time validity from `filed` dates rather than from which
file a row came from.

    python -m sec_lakehouse.ingest.edgar_fsds --quarters 2024q4 2025q1
    python -m sec_lakehouse.ingest.edgar_fsds --latest 2

The SEC asks for a descriptive User-Agent with contact info and no more than
10 requests per second; this module sends one request per quarter.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

import requests

from ..config import Settings, settings
from ..storage import BronzeStore, PutResult

FSDS_URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{quarter}.zip"


def quarter_id(d: date) -> str:
    return f"{d.year}q{(d.month - 1) // 3 + 1}"


def latest_quarters(n: int, today: date | None = None) -> list[str]:
    """The n most recent COMPLETED quarters (the SEC publishes a quarter's file after it ends)."""
    today = today or date.today()
    y, q = today.year, (today.month - 1) // 3 + 1
    out = []
    for _ in range(n):
        q -= 1
        if q == 0:
            y, q = y - 1, 4
        out.append(f"{y}q{q}")
    return list(reversed(out))


def download_quarter(quarter: str, dest_dir: Path, cfg: Settings = settings, session: requests.Session | None = None,
                     max_retries: int = 3) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{quarter}.zip"
    url = FSDS_URL.format(quarter=quarter)
    sess = session or requests.Session()
    headers = {"User-Agent": cfg.sec_user_agent, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(1, max_retries + 1):
        try:
            with sess.get(url, headers=headers, stream=True, timeout=120) as r:
                if r.status_code == 404:
                    raise FileNotFoundError(f"{quarter} not published yet ({url})")
                r.raise_for_status()
                tmp = dest.with_suffix(".part")
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                tmp.replace(dest)
                return dest
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == max_retries:
                raise
            time.sleep(2 ** attempt)
            print(f"retry {attempt} for {quarter}: {exc}", file=sys.stderr)
    raise RuntimeError("unreachable")


def land_quarter(quarter: str, store: BronzeStore, cfg: Settings = settings, session=None) -> PutResult:
    local = download_quarter(quarter, cfg.data_dir / "bronze" / "fsds", cfg, session)
    key = f"{cfg.bronze_prefix}/fsds/{quarter}/{quarter}.zip"
    result = store.put_file(local, key, FSDS_URL.format(quarter=quarter), extra={"dataset": "fsds", "quarter": quarter})
    print(f"{quarter}: {'already landed' if result.skipped else 'landed'} {result.bytes / 1e6:.1f} MB sha256={result.sha256[:12]}")
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--quarters", nargs="+", help="e.g. 2024q4 2025q1")
    g.add_argument("--latest", type=int, help="the N most recent completed quarters")
    args = ap.parse_args(argv)
    quarters = args.quarters or latest_quarters(args.latest)
    store = BronzeStore()
    for q in quarters:
        try:
            land_quarter(q, store)
        except FileNotFoundError as exc:
            print(f"skip: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
