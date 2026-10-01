# Serving image: the FastAPI layer over the gold snapshot + pgvector.
# The pipeline itself runs from `make pipeline` (or Dagster); this image only serves.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONUTF8=1 \
    HF_HOME=/models
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
# CPU torch keeps the image ~1.5 GB instead of ~6 GB
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch \
 && pip install --no-cache-dir -e .

# bake the embedding model so the container needs no network at startup
RUN python -c "from sentence_transformers import SentenceTransformer as S; S('BAAI/bge-small-en-v1.5')"

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8080/health')"
CMD ["uvicorn", "sec_lakehouse.serving.api:app", "--host", "0.0.0.0", "--port", "8080"]
