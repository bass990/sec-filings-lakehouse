"""HTML -> clean text -> section-aware chunks. Pure functions, fully unit-tested.

Section detection follows the 10-K/10-Q item structure ("Item 1A. Risk
Factors", "Item 7. Management's Discussion..."). A chunk never crosses a
section boundary, so a citation can say "Apple 10-K FY2024, Item 1A, chunk 3".
Chunks are content-addressed: the same text in the same place yields the same
chunk_id, so a re-fetch of an unchanged filing produces zero new chunks and
zero new embeddings (that is what makes the embed step incremental).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

# "Item 1A." / "ITEM 7 ." / "Item 2:" at the start of a line, followed by a title
# "Item 7. Management's Discussion and Analysis of Financial Condition and Results of Operations 45"
# titles run to ~100 chars and a table-of-contents line may end in a page number
_ITEM_RE = re.compile(r"^\s*item\s+(\d{1,2}[a-c]?)\s*[.:\-—–]?\s+(.{3,160}?)(?:\s+\d{1,3})?\s*$", re.IGNORECASE | re.MULTILINE)
_WS_RE = re.compile(r"[ \t ]+")
_NL_RE = re.compile(r"\n{3,}")

KNOWN_ITEMS = {
    "1": "Business", "1a": "Risk Factors", "1b": "Unresolved Staff Comments", "1c": "Cybersecurity", "2": "Properties",
    "3": "Legal Proceedings", "4": "Mine Safety Disclosures", "5": "Market for Common Equity", "6": "Reserved",
    "7": "Management's Discussion and Analysis", "7a": "Quantitative and Qualitative Disclosures About Market Risk",
    "8": "Financial Statements and Supplementary Data", "9": "Changes in and Disagreements with Accountants",
    "9a": "Controls and Procedures", "9b": "Other Information", "10": "Directors, Executive Officers and Corporate Governance",
    "11": "Executive Compensation", "12": "Security Ownership", "13": "Certain Relationships", "14": "Principal Accountant Fees",
    "15": "Exhibits", "16": "Form 10-K Summary",
}


def html_to_text(html: str | bytes) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "head", "title", "meta", "noscript"]):
        tag.decompose()
    # inline XBRL hides facts in ix:header; drop it (the numbers come from the FSDS, not the text)
    for tag in soup.find_all(re.compile(r"^ix:header$", re.IGNORECASE)):
        tag.decompose()
    text = soup.get_text("\n")
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    text = _WS_RE.sub(" ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    return _NL_RE.sub("\n\n", text).strip()


@dataclass(frozen=True)
class Section:
    item: str          # "1a"
    title: str         # "Risk Factors"
    text: str


def split_sections(text: str) -> list[Section]:
    """Split on Item headings. The table of contents also lists every item, so a
    heading is only accepted when the text that follows it is long enough to be
    the section itself (>= 400 chars before the next heading)."""
    matches = list(_ITEM_RE.finditer(text))
    if not matches:
        return [Section("full", "Full document", text)]
    sections: list[Section] = []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if len(body) < 400:
            continue
        item = m.group(1).lower()
        title = KNOWN_ITEMS.get(item, m.group(2).strip().rstrip("."))
        sections.append(Section(item, title, body))
    if not sections:
        return [Section("full", "Full document", text)]
    # if the same item appears twice (TOC survived the length filter), keep the longer one
    best: dict[str, Section] = {}
    for s in sections:
        if s.item not in best or len(s.text) > len(best[s.item].text):
            best[s.item] = s
    return sorted(best.values(), key=lambda s: sections.index(s))


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    section: str
    chunk_index: int
    text: str


def chunk_text(text: str, max_chars: int = 1500, overlap: int = 200) -> list[str]:
    """Paragraph-aware sliding window: fill up to max_chars on paragraph
    boundaries, fall back to sentence boundaries for huge paragraphs, and carry
    `overlap` trailing characters into the next chunk so a sentence cut by the
    window is still retrievable."""
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    buf = ""
    for p in paras:
        if len(p) > max_chars:  # split a giant paragraph on sentences
            for sent in re.split(r"(?<=[.!?])\s+", p):
                if len(buf) + len(sent) + 1 > max_chars and buf:
                    chunks.append(buf.strip())
                    buf = buf[-overlap:] if overlap else ""
                buf += (" " if buf else "") + sent
            continue
        if len(buf) + len(p) + 2 > max_chars and buf:
            chunks.append(buf.strip())
            buf = buf[-overlap:] if overlap else ""
        buf += ("\n\n" if buf else "") + p
    if buf.strip():
        chunks.append(buf.strip())
    return [c for c in chunks if len(c) >= 50]


def build_chunks(adsh: str, text: str, max_chars: int = 1500, overlap: int = 200) -> list[Chunk]:
    out: list[Chunk] = []
    for sec in split_sections(text):
        for i, c in enumerate(chunk_text(sec.text, max_chars, overlap)):
            cid = hashlib.sha256(f"{adsh}|{sec.item}|{i}|{c}".encode()).hexdigest()[:32]
            out.append(Chunk(cid, sec.item, i, c))
    return out
