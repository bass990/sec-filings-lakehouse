# Architecture

## The problem this platform solves

Financial facts change after they are published. A company reports FY2023
revenue in February 2024, then restates it in February 2025 when the next
10-K carries the prior year as a comparative. A naive table keeps one value
per fact and silently overwrites it. Anything trained or back-tested on that
table sees the future: it learns from a number that was not knowable at the
time. The same thing happens on the text side: a 10-K/A replaces a 10-K, an
embedding index is rebuilt, and nobody can say which version of which chunk
produced last week's answer.

This platform treats both as versioned, point-in-time data with lineage:

* every reported version of every numeric fact is kept, with the date it
  became known (`knowledge_date`) and a `[valid_from, valid_to)` window;
* every text chunk is content-addressed, every embedding index is an
  immutable version, and the API serves only a version that passed a
  retrieval evaluation.

## Data flow

```
SEC EDGAR ──▶ bronze (immutable zips + HTML, sha256 manifests)          object store
                 │
                 ▼
             silver.facts / silver.submissions (Iceberg, partitioned by quarter)
                 │                                   │
                 ▼                                   ▼
             DuckDB snapshot ──dbt──▶ gold.*     docs.chunks (Postgres)
                 │                                   │
                 ▼                                   ▼
             gold.* published to Iceberg         docs.embeddings[index_version] (pgvector)
                 │                                   │  candidate ──eval──▶ promoted | rejected
                 ▼                                   ▼
             FastAPI  /facts /companies /restatements /search /ask /index /costs /health
```

Dagster owns the DAG: quarter-partitioned assets for the fact side (backfill
any quarter since 2009), weekly document refresh, asset checks that fail the
run when the contract breaks.

## Layers

**Bronze.** Object store, write-once. Every object has a sibling
`.manifest.json` with sha256, byte size, source URL and landing time. A
re-run that finds the same hash is a no-op. The SEC re-publishes quarterly
datasets when late filers land; a changed hash triggers a replace of exactly
that quarter downstream.

**Silver (Iceberg).** `silver.facts` is one row per *reported version* of a
fact: the same `fact_key` appears once per filing that carried it. Columns
that make history queryable: `knowledge_date` (the filing date), `segments`
(NULL for the consolidated entity, otherwise the dimensional slice) and
`is_dimensional`. Loads are delete-by-quarter then append inside a
transaction, and the processed-manifest table records the zip hash so the
load is idempotent.

Real-world quirks handled explicitly: rows with a literal tab inside a free
text field (about 30 per quarter) are counted and skipped rather than aborting
a 3.5M-row load; the `accepted` timestamp gained fractional seconds in 2025;
the taxonomy version (`us-gaap/2024` vs `/2025`) is deliberately *not* part
of the fact key because the re-report under the newer taxonomy is exactly the
restatement version we want to line up.

**Gold (dbt on DuckDB, published back to Iceberg).** `fact_versions` turns
the version rows into validity windows (`valid_from <= D < valid_to` answers
"what was known on D"); `restatements` lists facts whose value changed with
original, latest, relative change and days between; `company_quarter` pivots
the headline metrics per company and reporting period, placing a restated
value in the period it describes, not the period of the filing that carried
it, and records when each metric was first reported; `data_quality` is the
per-quarter scorecard `/health` exposes. The `fact_versions` contract is
enforced (`contract: enforced`) plus custom tests: exactly one latest version
per key, windows tile without gaps or overlaps, composite-key uniqueness.

Why dbt runs on a DuckDB snapshot rather than on Iceberg directly: DuckDB's
Iceberg extension cannot follow pyiceberg's copy-on-write deletes on every
platform, and the snapshot is a deterministic Arrow copy that takes seconds.
The published gold tables in Iceberg are what Athena, Spark and BigQuery read.

**Documents.** Which filings exist comes from `silver.submissions`, so the
text corpus can never cite a filing the facts side does not know. HTML goes
to bronze as-is; text is split on 10-K items (the table of contents is
filtered out by requiring a minimum body length, and duplicate items keep the
longest body) then into ~1500-character paragraph-aware chunks with 200
characters of overlap. `chunk_id = sha256(adsh | item | index | text)`.

**Embeddings.** An index version is an immutable set of `(chunk_id, vector)`
rows under one `index_version`. Building a new version copies vectors for
chunks that already exist in the promoted version (same model) and only
embeds new chunks. The candidate is invisible to the API until promoted.

**Evaluation gate.** Two eval sets under `eval/`: 150 known-item queries
(a 25-word window from a real chunk; expected hit = that chunk) and 32
hand-written section-intent questions per company (expected = the right 10-K
item). Floors: known-item recall@5 >= 0.80, section hit@5 >= 0.60; a
candidate may not regress more than 0.02 on either against the promoted
index. Rejected candidates stay in the table with their numbers so the
history of what was tried is visible in `/index`. Rollback is moving the
pointer.

**Serving.** FastAPI over the DuckDB snapshot (facts) and pgvector
(retrieval). `/ask` retrieves from the promoted index and answers with Claude
when a key is configured, else returns the best passage extractively; every
answer carries citations (adsh, form, filing date, item, chunk) and the index
version. Request ids and timings are on every response.

**Cost ledger.** Every stage writes what it consumed (requests, tokens,
seconds) and an estimated USD amount, including the hosted-equivalent price
for the free local embedding model, so the number is there when someone asks
"what would this cost on OpenAI embeddings". `/costs` totals it per stage.

## Portability

`LAKEHOUSE_TARGET` selects object store + catalog; the code is identical:

| target | store | catalog | verified |
|---|---|---|---|
| local | MinIO | Iceberg REST | full pipeline |
| aws | S3 | Glue | Terraform applied; Glue catalog wired in `catalog.py` |
| azure | ADLS Gen2 | pyiceberg SQL catalog + ADLS IO | Terraform validated (apply blocked by the subscription's region policy; see deploy/README.md) |
| gcp | GCS | pyiceberg SQL catalog + GCS IO | Terraform validated (apply blocked: project billing account closed) |

## What is deliberately not here

* No Kafka: EDGAR is a batch source; streaming would be theatre. The
  companion project (GitHub Archive) is the streaming one.
* No hosted embedding API by default: a 384-dim local model embeds the
  corpus in minutes on CPU and keeps the pipeline free to re-run.
* No LLM in the pipeline: the LLM is only at the answer edge, behind a
  retrieval step that is evaluated without it.
