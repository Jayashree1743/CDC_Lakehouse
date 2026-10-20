# ==============================================================================
# DAY 5 - API image (FastAPI over Qdrant + Gold Delta)
# ==============================================================================
# Same reasoning as vector-sync: a SMALL python:3.11 service that reads Gold
# Delta via delta-rs and Qdrant directly — no Spark, no JVM. Spark builds the
# lakehouse; this serves it.

FROM python:3.11-slim
WORKDIR /app

ENV HF_HOME=/models
ENV FastEmbedCache=/models

COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt
# pre-download the SAME embedding model the worker uses (query == doc embedding)
RUN python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5'); print('model cached')"

COPY main.py /app/main.py
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
