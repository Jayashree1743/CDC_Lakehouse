# ==============================================================================
# DAY 4 - vector-sync worker image
# ==============================================================================
# A deliberately SMALL, standalone service (python:3.11-slim) that:
#   reads Silver Delta tables (delta-rs, no JVM) -> chunks -> embeds (fastembed)
#   -> upserts to Qdrant -> advances a watermark in a sync_state Delta table.
#
# The embedding model is PRE-DOWNLOADED at build time so runtime never depends
# on HuggingFace being reachable (deterministic + offline-friendly).

FROM python:3.11-slim

WORKDIR /app

# Embedding model cache location (pre-warmed below)
ENV HF_HOME=/models
ENV FastEmbedCache=/models

COPY requirements.txt .
# Large wheels (onnxruntime, etc.) can hit transient read timeouts on slow
# links -> raise pip's timeout and retry count so the build is robust.
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt

# Pre-download the model into the image (idempotent; ~100MB for bge-small)
RUN python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5'); print('model cached')"

COPY *.py /

# Prometheus metrics for Day 6 (scraped from the host at :8002/metrics)
EXPOSE 8002

CMD ["python3", "/worker.py"]
