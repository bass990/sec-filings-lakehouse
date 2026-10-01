"""Shared fixtures: a tiny synthetic FSDS zip with a restatement in it.

Two filings for the same company: the FY2023 10-K (filed 2024-02-01) reports
Revenue 100 and NetIncome 10; the FY2024 10-K (filed 2025-02-01) restates
FY2023 Revenue to 105 and reports FY2024 Revenue 120. That is enough to test
point-in-time queries, restatement handling and idempotent reloads.
"""
from __future__ import annotations

import io
import os
import zipfile

import pytest

os.environ.setdefault("LAKEHOUSE_TARGET", "local")

SUB = "\t".join(["adsh", "cik", "name", "sic", "countryba", "stprba", "fye", "form", "period", "fy", "fp", "filed", "accepted",
                 "prevrpt", "detail", "instance", "nciks"]) + "\n" + "\n".join([
    "\t".join(["0001-24-000001", "1000", "ACME CORP", "3570", "US", "CA", "1231", "10-K", "20231231", "2023", "FY", "20240201",
               "2024-02-01 09:00:00", "0", "1", "acme-20231231.htm", "1"]),
    "\t".join(["0001-25-000002", "1000", "ACME CORP", "3570", "US", "CA", "1231", "10-K", "20241231", "2024", "FY", "20250201",
               "2025-02-01 09:00:00", "0", "1", "acme-20241231.htm", "1"]),
])

NUM = "\t".join(["adsh", "tag", "version", "ddate", "qtrs", "uom", "segments", "coreg", "value", "footnote"]) + "\n" + "\n".join([
    "\t".join(["0001-24-000001", "Revenues", "us-gaap/2023", "20231231", "4", "USD", "", "", "100", ""]),
    "\t".join(["0001-24-000001", "NetIncomeLoss", "us-gaap/2023", "20231231", "4", "USD", "", "", "10", ""]),
    "\t".join(["0001-25-000002", "Revenues", "us-gaap/2024", "20231231", "4", "USD", "", "", "105", "restated"]),
    "\t".join(["0001-25-000002", "Revenues", "us-gaap/2024", "20241231", "4", "USD", "", "", "120", ""]),
    # a segment slice of FY2024 revenue: same tag/date, non-empty segments -> a different fact
    "\t".join(["0001-25-000002", "Revenues", "us-gaap/2024", "20241231", "4", "USD", "ProductOrServiceAxis=WidgetMember;", "", "70", ""]),
])


def make_zip(sub: str = SUB, num: str = NUM) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("sub.txt", sub)
        zf.writestr("num.txt", num)
        zf.writestr("pre.txt", "adsh\treport\tline\n")
        zf.writestr("tag.txt", "tag\tversion\n")
    return buf.getvalue()


@pytest.fixture
def tiny_zip() -> bytes:
    return make_zip()
