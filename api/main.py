#!/usr/bin/env python3
"""
DAY 5 - SERVING LAYER (FastAPI + Hybrid Search)
================================================
A working API over the two hot paths a real app needs:

  POST /search                    -> semantic search over Qdrant (+ payload filter)
  GET  /contracts/{id}/history    -> SQL over Gold fct_contract_changes
  GET  /analytics/changes-per-hour-> aggregation over Gold
  GET  /health                   -> Kafka/Delta/Qdrant reachability

Interview vocabulary
--------------------
* FastAPI route/handler  - one function per endpoint; path+method decorate it.
* Pydantic model         - typed request/response; FastAPI validates + documents it.
* Dependency injection   - `Depends()` shares expensive clients (Qdrant, embedder)
                           across requests instead of rebuilding per call.
* OpenAPI /docs          - interactive docs generated FOR FREE from the type hints.
* Hybrid search          - vector similarity PLUS a metadata filter (e.g. only this
                           customer's contracts). Pure vector search would happily
                           return another tenant's clauses — a data-leak bug.
* Score threshold        - drop weak matches so the LLM (or the user) never sees
                           irrelevant context. Below threshold = "no good match".

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
"""
import os
import time
from functools import lru_cache
from typing import List, Optional

import numpy as np
from deltalake import DeltaTable
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastembed import TextEmbedding
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchValue

# --------------------------------------------------------------------------- #
# Configuration                                                                #
# --------------------------------------------------------------------------- #
LAKEHOUSE = os.environ.get("LAKEHOUSE", "/lakehouse")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "contracts")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "BAAI/bge-small-en-v1.5")
GOLD_FCT = f"{LAKEHOUSE}/gold.db/fct_contract_changes"
# trivial shared-secret auth; a real deploy would validate a JWT / OAuth token
API_KEY = os.environ.get("API_KEY", "demo-key")

app = FastAPI(
    title="CDC Lakehouse + Vector Sync API",
    description="Semantic search over Qdrant + analytics over the Gold lakehouse.",
    version="1.0.0",
)

# --- Prometheus metrics (Day 6 scrape target on :8000/metrics) ---------------
REQUESTS = Counter(
    "api_requests_total", "API requests", ["endpoint", "status_code"])


@app.middleware("http")
async def count_requests(request: Request, call_next):
    """Middleware: every request increments a labelled Counter, whatever route."""
    response = await call_next(request)
    REQUESTS.labels(endpoint=request.url.path,
                    status_code=str(response.status_code)).inc()
    return response


@app.get("/metrics")
def metrics():
    """Prometheus exposition format - scraped by the Prometheus server."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)



# --------------------------------------------------------------------------- #
# Shared clients (dependency injection)                                        #
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def get_embedder() -> TextEmbedding:
    return TextEmbedding(EMBED_MODEL)


@lru_cache(maxsize=1)
def get_qdrant() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def require_api_key(x_api_key: str = Header(default="")):
    """Header auth: every endpoint calls this via Depends()."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing x-api-key")


# --------------------------------------------------------------------------- #
# Pydantic response models                                                     #
# --------------------------------------------------------------------------- #
class SearchHit(BaseModel):
    score: float = Field(..., description="cosine similarity (higher = closer)")
    contract_id: Optional[int] = None
    document_id: Optional[str] = None
    chunk_index: Optional[int] = None
    text: str


class SearchResponse(BaseModel):
    query: str
    top_k: int
    min_score: float
    hits: List[SearchHit]


class ChangeRow(BaseModel):
    change_uid: str
    contract_id: int
    customer_id: Optional[int] = None
    title: Optional[str] = None
    monthly_price: Optional[float] = None
    status: Optional[str] = None
    cdc_op: str
    source_commit_ts: Optional[str] = None
    change_hour: Optional[str] = None


class HourlyBucket(BaseModel):
    change_hour: str
    cdc_op: str
    changes: int


class HealthStatus(BaseModel):
    status: str
    qdrant: str
    gold: str
    elapsed_ms: float


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #
def read_gold_fct():
    """Read the Gold fact table by PATH (no Spark / no metastore needed).

    dbt wrote fct_contract_changes as a Delta table at this path, so delta-rs
    reads it directly. This is the lakehouse contract paying off: any engine
    that speaks Delta can query Gold.

    NOTE: delta-rs returns timestamp columns as `datetime` objects. Our Pydantic
    response models declare them as `str`, so we stringify them here to avoid a
    ValidationError (pydantic: "Input should be a valid string").
    """
    rows = DeltaTable(GOLD_FCT).to_pyarrow_table().to_pylist()
    for r in rows:
        for k, v in list(r.items()):
            if hasattr(v, "isoformat"):        # datetime/date -> ISO string
                r[k] = v.isoformat()
    return rows


# --------------------------------------------------------------------------- #
# Endpoints                                                                    #
# --------------------------------------------------------------------------- #
@app.get("/health", response_model=HealthStatus)
def health():
    t0 = time.time()
    # Qdrant reachable?
    try:
        get_qdrant().get_collection(COLLECTION)
        q = "ok"
    except Exception as e:
        q = f"error: {e}"
    # Gold readable?
    try:
        _ = DeltaTable(GOLD_FCT)
        g = "ok"
    except Exception as e:
        g = f"error: {e}"
    ok = q == "ok" and g == "ok"
    return HealthStatus(status="ok" if ok else "degraded", qdrant=q, gold=g,
                        elapsed_ms=round((time.time() - t0) * 1000, 1))


@app.post("/search", response_model=SearchResponse, dependencies=[Depends(require_api_key)])
def search(body: dict):
    """Semantic search. Optional `customer_id` narrows to that tenant only."""
    query = body.get("query", "")
    top_k = int(body.get("top_k", 5))
    min_score = float(body.get("min_score", 0.25))
    customer_id = body.get("customer_id")   # optional metadata filter

    if not query.strip():
        raise HTTPException(status_code=400, detail="query must be non-empty")

    # 1. embed the query with the SAME model used for documents
    qv = np.asarray(next(iter(get_embedder().embed([query]))), dtype=np.float32).tolist()

    # 2. build a payload filter if a tenant filter is requested (hybrid search)
    qfilter = None
    if customer_id is not None:
        # NOTE: we filter on contract_id via a lookup of that customer's contracts
        try:
            cust_contracts = [r["contract_id"] for r in read_gold_fct()
                              if r.get("customer_id") == customer_id]
        except Exception:
            cust_contracts = []
        if not cust_contracts:
            return SearchResponse(query=query, top_k=top_k, min_score=min_score, hits=[])
        qfilter = Filter(must=[FieldCondition(
            key="contract_id", match=MatchAny(any=list(set(cust_contracts))))])

    # 3. query Qdrant
    res = get_qdrant().query_points(
        collection_name=COLLECTION, query=qv, limit=top_k,
        query_filter=qfilter, with_payload=True).points

    hits = [SearchHit(score=round(p.score, 4),
                      contract_id=p.payload.get("contract_id"),
                      document_id=p.payload.get("document_id"),
                      chunk_index=p.payload.get("chunk_index"),
                      text=p.payload.get("text", ""))
            for p in res if p.score >= min_score]          # score threshold

    return SearchResponse(query=query, top_k=top_k, min_score=min_score, hits=hits)


@app.get("/contracts/{contract_id}/history", response_model=List[ChangeRow],
         dependencies=[Depends(require_api_key)])
def contract_history(contract_id: int):
    """Change history for one contract, newest first (SQL over Gold)."""
    try:
        rows = read_gold_fct()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Gold unavailable: {e}")
    hist = [ChangeRow(**{k: r.get(k) for k in ChangeRow.model_fields})
            for r in rows if r.get("contract_id") == contract_id]
    hist.sort(key=lambda r: r.source_commit_ts or "", reverse=True)
    return hist


@app.get("/analytics/changes-per-hour", response_model=List[HourlyBucket],
         dependencies=[Depends(require_api_key)])
def changes_per_hour():
    """How many contract changes per hour (aggregation over Gold)."""
    try:
        rows = read_gold_fct()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Gold unavailable: {e}")
    counts = {}
    for r in rows:
        key = (r.get("change_hour") or "unknown", r.get("cdc_op") or "?")
        counts[key] = counts.get(key, 0) + 1
    out = [HourlyBucket(change_hour=h, cdc_op=op, changes=n)
           for (h, op), n in counts.items()]
    out.sort(key=lambda b: b.change_hour, reverse=True)
    return out


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
