"""Run the gold SQL logic on a tiny DuckDB with the restatement fixture, without dbt.

The dbt models are rendered by substituting source()/ref() with plain table
names, so the exact SQL that ships is what is tested here.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from sec_lakehouse.config import ROOT
from sec_lakehouse.lakehouse.facts import parse_zip

MODELS = ROOT / "transformation" / "dbt_project" / "models" / "gold"


def render(name: str) -> str:
    """Render the model with Jinja exactly as dbt would, with source()/ref() mapped to plain table names."""
    from jinja2 import Template  # noqa: PLC0415
    sql = (MODELS / f"{name}.sql").read_text(encoding="utf-8")
    return Template(sql).render(source=lambda schema, table: f"{schema}.{table}", ref=lambda table: f"gold.{table}")


@pytest.fixture
def con(tiny_zip: bytes):
    subs, facts = parse_zip(tiny_zip, "2025q1", "abc")
    c = duckdb.connect()
    c.execute("CREATE SCHEMA silver; CREATE SCHEMA gold")
    c.register("s", subs)
    c.register("f", facts)
    c.execute("CREATE TABLE silver.submissions AS SELECT * FROM s")
    c.execute("CREATE TABLE silver.facts AS SELECT * FROM f")
    c.execute(f"CREATE TABLE gold.fact_versions AS {render('fact_versions')}")
    c.execute(f"CREATE TABLE gold.restatements AS {render('restatements')}")
    return c


def test_fact_versions_windows_tile_and_single_latest(con):
    rows = con.execute("""SELECT value, valid_from, valid_to, version_no, is_latest FROM gold.fact_versions
                          WHERE tag = 'Revenues' AND ddate = DATE '2023-12-31' ORDER BY version_no""").fetchall()
    assert rows == [(100.0, date(2024, 2, 1), date(2025, 2, 1), 1, False), (105.0, date(2025, 2, 1), date(9999, 12, 31), 2, True)]
    # point-in-time: as of 2024-06-30 the answer is 100
    v = con.execute("""SELECT value FROM gold.fact_versions WHERE tag='Revenues' AND ddate=DATE '2023-12-31'
                       AND valid_from <= DATE '2024-06-30' AND valid_to > DATE '2024-06-30'""").fetchone()[0]
    assert v == 100.0
    bad = con.execute("SELECT count(*) FROM (SELECT fact_key FROM gold.fact_versions GROUP BY 1 HAVING sum(is_latest::int) <> 1)").fetchone()[0]
    assert bad == 0
    # the dimensional slice is a separate key with its own single version
    dim = con.execute("SELECT n_versions, is_dimensional FROM gold.fact_versions WHERE segments IS NOT NULL").fetchall()
    assert dim == [(1, True)]


def test_restatements_capture_only_changed_values(con):
    rows = con.execute("SELECT tag, original_value, latest_value, rel_change, days_between, n_versions FROM gold.restatements").fetchall()
    assert len(rows) == 1
    tag, orig, latest, rel, days, n = rows[0]
    assert (tag, orig, latest, n) == ("Revenues", 100.0, 105.0, 2) and abs(rel - 0.05) < 1e-9 and days == 366


def test_company_quarter_pivots_latest_values(con):
    con.execute(f"CREATE TABLE gold.company_quarter AS {render('company_quarter').replace('current_timestamp', 'now()')}")
    rows = con.execute("SELECT fy, fp, revenue, net_income, revenue_first_reported FROM gold.company_quarter WHERE fp = 'FY' ORDER BY fy").fetchall()
    # FY2023 shows the RESTATED 105 (latest), FY2024 shows 120 (segment slice of 70 excluded)
    assert rows == [(2023, "FY", 105.0, 10.0, date(2024, 2, 1)), (2024, "FY", 120.0, None, date(2025, 2, 1))]


def test_render_is_pure_jinja_free_sql():
    for name in ("fact_versions", "restatements", "data_quality"):
        assert "{{" not in render(name), name
    assert Path(MODELS / "schema.yml").exists()
