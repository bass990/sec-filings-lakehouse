.PHONY: help infra infra-down install ingest silver marts documents embed evaluate api dagster test lint pipeline clean

PYTHON ?= .venv/Scripts/python.exe
QUARTERS ?= 2025q1 2025q2
export PYTHONUTF8 = 1

help:
	@echo "SEC EDGAR AI-ready data platform"
	@echo "  infra        docker compose up (RustFS S3 store, Iceberg REST catalog, Postgres+pgvector)"
	@echo "  ingest       land the latest quarterly XBRL datasets in bronze   (QUARTERS='2025q1 2025q2')"
	@echo "  silver       parse bronze zips into Iceberg silver.facts / silver.submissions (point-in-time)"
	@echo "  marts        dbt build: gold company_quarter, restatements, data-quality tests"
	@echo "  documents    fetch + chunk 10-K text for the tracked companies"
	@echo "  embed        embed new/changed chunks into pgvector (versioned index)"
	@echo "  evaluate     retrieval eval (recall@k, freshness, cost); gates index promotion"
	@echo "  api          FastAPI: /ask (RAG with citations), /facts (point-in-time), /health"
	@echo "  dagster      Dagster UI with the full asset graph, schedules and backfills"
	@echo "  pipeline     ingest -> silver -> marts -> documents -> embed -> evaluate"
	@echo "  test / lint  offline tests (moto S3, synthetic zips) / ruff"

infra:
	docker compose up -d

infra-down:
	docker compose down -v

install:
	$(PYTHON) -m pip install -e ".[dev]"

ingest:
	$(PYTHON) -m sec_lakehouse.ingest.edgar_fsds --quarters $(QUARTERS)

silver:
	$(PYTHON) -m sec_lakehouse.lakehouse.facts --quarters $(QUARTERS)
	$(PYTHON) -m sec_lakehouse.lakehouse.snapshot snapshot

marts:
	cd transformation/dbt_project && ../../.venv/Scripts/dbt build --profiles-dir .
	$(PYTHON) -m sec_lakehouse.lakehouse.snapshot publish

documents:
	$(PYTHON) -m sec_lakehouse.documents.fetch

embed:
	$(PYTHON) -m sec_lakehouse.documents.embed

evaluate:
	$(PYTHON) -m sec_lakehouse.documents.evaluate

api:
	$(PYTHON) -m uvicorn sec_lakehouse.serving.api:app --port 8080 --reload

dagster:
	$(PYTHON) -m dagster dev -m sec_lakehouse.orchestration.definitions

pipeline: ingest silver marts documents embed evaluate

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check src tests

clean:
	rm -rf data/bronze/* .pytest_cache .ruff_cache transformation/dbt_project/target
