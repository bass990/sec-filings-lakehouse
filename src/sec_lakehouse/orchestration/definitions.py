"""Dagster asset graph: the pipeline as a DAG with lineage, checks, schedules and backfills.

    fsds_bronze ─▶ silver_facts ─▶ silver_snapshot ─▶ gold_marts ─▶ gold_published
                        │
                        └─▶ filing_documents ─▶ embedding_candidate ─▶ retrieval_eval (promotes or rejects)

Partitions: the fact side is partitioned by SEC quarter (2009q1..), so a
backfill of one quarter is one click and re-running a quarter is idempotent
(sha manifest). Asset checks fail the run when the data contract is violated.
"""

from datetime import date

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    StaticPartitionsDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from ..config import settings
from ..ingest.edgar_fsds import land_quarter, latest_quarters
from ..lakehouse.facts import load_quarter
from ..lakehouse.snapshot import publish_gold, snapshot_silver


def _all_quarters(start_year: int = 2020) -> list[str]:
    today = date.today()
    out = []
    for y in range(start_year, today.year + 1):
        for q in range(1, 5):
            if (y, q) <= (today.year, (today.month - 1) // 3 + 1):
                out.append(f"{y}q{q}")
    return out


quarters = StaticPartitionsDefinition(_all_quarters())


@asset(partitions_def=quarters, group_name="facts", description="Raw SEC Financial Statement Data Set zip in bronze (immutable, sha256 manifest).")
def fsds_bronze(context: AssetExecutionContext) -> MaterializeResult:
    from ..storage import BronzeStore  # noqa: PLC0415
    res = land_quarter(context.partition_key, BronzeStore(settings), settings)
    return MaterializeResult(metadata={"sha256": res.sha256, "bytes": res.bytes, "skipped": res.skipped})


@asset(partitions_def=quarters, deps=[fsds_bronze], group_name="facts",
       description="Iceberg silver.facts / silver.submissions for the quarter; delete-then-append, idempotent by zip sha.")
def silver_facts(context: AssetExecutionContext) -> MaterializeResult:
    res = load_quarter(context.partition_key, settings)
    return MaterializeResult(metadata={k: v for k, v in res.items() if k != "quarter"})


@asset(deps=[silver_facts], group_name="facts", description="DuckDB snapshot of the silver tables that dbt models against.")
def silver_snapshot() -> MaterializeResult:
    return MaterializeResult(metadata=snapshot_silver(settings))


@asset(deps=[silver_snapshot], group_name="facts", description="dbt build: gold.fact_versions, restatements, company_quarter, data_quality (+ contract tests).")
def gold_marts(context: AssetExecutionContext) -> MaterializeResult:
    import subprocess  # noqa: PLC0415
    import sys  # noqa: PLC0415

    from ..config import ROOT  # noqa: PLC0415
    proj = ROOT / "transformation" / "dbt_project"
    dbt = str(ROOT / ".venv" / ("Scripts/dbt.exe" if sys.platform == "win32" else "bin/dbt"))
    out = subprocess.run([dbt, "build", "--profiles-dir", "."], cwd=proj, capture_output=True, text=True, check=False)
    context.log.info(out.stdout[-4000:])
    if out.returncode != 0:
        raise RuntimeError(out.stderr[-2000:] or out.stdout[-2000:])
    return MaterializeResult(metadata={"dbt_tail": out.stdout[-600:]})


@asset(deps=[gold_marts], group_name="facts", description="Gold marts published back to Iceberg gold.* for every engine.")
def gold_published() -> MaterializeResult:
    return MaterializeResult(metadata=publish_gold(settings))


@asset_check(asset=gold_published, description="Data contract: every fact key has exactly one latest version and windows tile.")
def gold_contract() -> AssetCheckResult:
    import duckdb  # noqa: PLC0415

    from ..lakehouse.snapshot import duckdb_path  # noqa: PLC0415
    with duckdb.connect(str(duckdb_path(settings)), read_only=True) as con:
        bad_latest = con.execute("SELECT count(*) FROM (SELECT fact_key FROM gold.fact_versions GROUP BY 1 HAVING sum(is_latest::int) <> 1)").fetchone()[0]
        n = con.execute("SELECT count(*) FROM gold.fact_versions").fetchone()[0]
    return AssetCheckResult(passed=bad_latest == 0, metadata={"rows": n, "keys_without_single_latest": bad_latest})


@asset(deps=[silver_facts], group_name="documents", description="10-K/10-Q primary documents for tracked companies: raw HTML in bronze, chunks in Postgres.")
def filing_documents() -> MaterializeResult:
    from ..documents.fetch import run  # noqa: PLC0415
    return MaterializeResult(metadata=run(settings))


@asset(deps=[filing_documents], group_name="documents", description="Versioned candidate embedding index in pgvector (incremental: unchanged chunks reuse vectors).")
def embedding_candidate() -> MaterializeResult:
    from ..documents.embed import build_candidate  # noqa: PLC0415
    return MaterializeResult(metadata={"index_version": build_candidate(settings)})


@asset(deps=[embedding_candidate], group_name="documents", description="Retrieval eval; promotes the candidate only if it clears the floors and does not regress.")
def retrieval_eval() -> MaterializeResult:
    from ..documents.evaluate import run  # noqa: PLC0415
    res = run(settings)
    return MaterializeResult(metadata={k: v for k, v in res.items() if isinstance(v, int | float | str | bool)})


@asset_check(asset=retrieval_eval, description="A promoted index must exist after eval (either the candidate or the incumbent).")
def promoted_index_exists() -> AssetCheckResult:
    from ..documents import db  # noqa: PLC0415
    with db.tx(settings) as conn:
        v = db.promoted_version(conn)
    return AssetCheckResult(passed=v is not None, metadata={"promoted": v or ""})


facts_job = define_asset_job("facts_pipeline", selection=[fsds_bronze, silver_facts, silver_snapshot, gold_marts, gold_published])
documents_job = define_asset_job("documents_pipeline", selection=[filing_documents, embedding_candidate, retrieval_eval])

# the SEC publishes a quarter's dataset in the first days after quarter end; run on the 5th and re-run
# the previous quarter too (it is republished when late filers land)
quarterly = ScheduleDefinition(
    job=facts_job, cron_schedule="0 6 5 1,4,7,10 *",
    execution_fn=lambda ctx: [facts_job.run_request_for_partition(q) for q in latest_quarters(2)],  # type: ignore[arg-type]
)
weekly_docs = ScheduleDefinition(job=documents_job, cron_schedule="0 7 * * 1")

defs = Definitions(
    assets=[fsds_bronze, silver_facts, silver_snapshot, gold_marts, gold_published, filing_documents, embedding_candidate, retrieval_eval],
    asset_checks=[gold_contract, promoted_index_exists],
    jobs=[facts_job, documents_job],
    schedules=[quarterly, weekly_docs],
)
