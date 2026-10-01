"""Chunking, section detection, content addressing, and the promotion gate. No network, no DB."""
from __future__ import annotations

from sec_lakehouse.documents.chunking import build_chunks, chunk_text, html_to_text, split_sections
from sec_lakehouse.documents.evaluate import promote_if
from sec_lakehouse.documents.fetch import primary_doc_url

TOC = "Item 1. Business\nItem 1A. Risk Factors\nItem 7. Management's Discussion\n"
BODY = (
    "\n\nItem 1. Business\n\n" + ("Apple designs, manufactures and markets smartphones. " * 30) +
    "\n\nItem 1A. Risk Factors\n\n" + ("The Company's business can be affected by global economic conditions. " * 40) +
    "\n\nItem 7. Management's Discussion and Analysis\n\n" + ("Net sales increased 2% compared to the prior year. " * 35)
)
HTML = f"<html><head><style>p{{}}</style><title>10-K</title></head><body><ix:header>hidden</ix:header><p>{TOC}</p>{BODY.replace(chr(10), '<br/>')}</body></html>"


def test_html_to_text_strips_markup_and_hidden_xbrl():
    text = html_to_text(HTML)
    assert "hidden" not in text and "<" not in text and "10-K" not in text
    assert "Risk Factors" in text


def test_sections_ignore_table_of_contents_and_keep_longest():
    secs = split_sections(TOC + BODY)
    items = [s.item for s in secs]
    assert items == ["1", "1a", "7"]
    assert secs[1].title == "Risk Factors" and len(secs[1].text) > 1000


def test_chunks_respect_limits_and_overlap():
    text = "\n\n".join(f"Paragraph {i}. " + ("word " * 60) for i in range(20))
    chunks = chunk_text(text, max_chars=800, overlap=100)
    assert all(len(c) <= 800 + 100 for c in chunks) and len(chunks) > 5
    # overlap: the tail of chunk n appears at the head of chunk n+1
    assert chunks[0][-40:].strip().split()[-3:] == chunks[1][:200].split()[:3] or chunks[1].startswith(chunks[0][-100:].strip()[:20])


def test_build_chunks_is_content_addressed_and_section_scoped():
    a = build_chunks("0001-25-1", TOC + BODY)
    b = build_chunks("0001-25-1", TOC + BODY)
    assert [c.chunk_id for c in a] == [c.chunk_id for c in b]
    assert {c.section for c in a} == {"1", "1a", "7"}
    # a different filing with identical text gets different ids (provenance is part of the address)
    c = build_chunks("0001-25-2", TOC + BODY)
    assert not set(x.chunk_id for x in a) & set(x.chunk_id for x in c)
    # editing one section changes only that section's ids
    d = build_chunks("0001-25-1", (TOC + BODY).replace("Net sales increased 2%", "Net sales increased 3%"))
    same = {x.chunk_id for x in a} & {x.chunk_id for x in d}
    assert same and all(x.section != "7" for x in a if x.chunk_id in same)


def test_primary_doc_url():
    assert primary_doc_url(320193, "0000320193-24-000123", "aapl-20240928_htm.xml") == \
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"


def test_promotion_gate():
    good = {"known_item_recall_at_5": 0.9, "section_hit_at_5": 0.7}
    assert promote_if(good, None) == (True, [])
    ok, why = promote_if({"known_item_recall_at_5": 0.7, "section_hit_at_5": 0.7}, None)
    assert not ok and "below floor" in why[0]
    # regression vs the incumbent beyond the tolerance is rejected even above the floor
    ok, why = promote_if({"known_item_recall_at_5": 0.85, "section_hit_at_5": 0.7}, {"known_item_recall_at_5": 0.9, "section_hit_at_5": 0.7})
    assert not ok and "regressed" in why[0]
    # within tolerance is fine
    assert promote_if({"known_item_recall_at_5": 0.89, "section_hit_at_5": 0.7}, good)[0]
    # a missing metric (no section-intent items for this corpus) does not block
    assert promote_if({"known_item_recall_at_5": 0.9, "section_hit_at_5": None}, None)[0]


def test_long_item_titles_and_page_numbers_are_detected():
    nl = chr(10) * 2
    text = ("Item 6. [Reserved]" + nl + ("reserved filler. " * 30) + nl
            + "Item 7. Management's Discussion and Analysis of Financial Condition and Results of Operations 45" + nl
            + ("Revenue grew because of data center demand. " * 40) + nl
            + "Item 9. Changes in and Disagreements With Accountants on Accounting and Financial Disclosure" + nl + ("None. " * 100))
    items = [s.item for s in split_sections(text)]
    assert items == ["6", "7", "9"], items
