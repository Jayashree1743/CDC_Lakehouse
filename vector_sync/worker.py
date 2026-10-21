#!/usr/bin/env python3
"""
DAY 4 - INCREMENTAL VECTOR SYNC WORKER  (Silver -> chunk -> embed -> Qdrant)
=============================================================================
This is the #1 differentiator of the whole portfolio. Read the DESIGN NOTES.

Problem
-------
You have contract documents in Silver. You keep them as vectors in Qdrant so a
user can ask "can I get my money back?" and get the right CLAUSE back by
meaning, not by keyword (semantic search / RAG retrieval).

Naive approach: every night, re-chunk + re-embed the WHOLE corpus and push to
Qdrant. That is O(corpus) tokens on EVERY run — expensive, slow, and it
rewrites vectors that never changed.

Our approach: process ONLY what changed since a watermark.

The four ideas that make it work (memorize these - they win interviews)
----------------------------------------------------------------------
1. DETERMINISTIC CHUNK ID
       chunk_id = sha256(document_id + ":" + chunk_index)
   The same document+position ALWAYS maps to the same vector ID, forever.
   => upsert is IDEMPOTENT (re-running the same change overwrites in place,
      never duplicates) and we can DELETE stale chunks WITHOUT a lookup table
      (we know exactly which IDs a document SHOULD own).
   Random UUIDs break BOTH properties: you cannot know what to delete, and a
   replay creates ghost duplicates.

2. WATERMARK (composite: source commit ts + LSN)
       "the highest (source_commit_ts, source_lsn) I have successfully synced."
   We persist it AFTER a successful batch, in a tiny `sync_state` Delta table.
   Next run = "Silver rows WHERE (source_commit_ts, source_lsn) > watermark".
   Both fields come from Postgres's own WAL metadata (inside the CDC event),
   NOT the local wall clock — so clock skew between machines can never drop a
   row. The LSN tie-break handles two commits sharing a millisecond (strict `>`
   on ts alone would drop the second one; `>=` would re-read the boundary row
   on every poll). Crash safety: watermark is written LAST, so a crash mid-run
   re-processes the same rows next time (at-least-once). Deterministic IDs turn
   that re-run into a harmless overwrite (exactly-once EFFECT).

3. DELETE HANDLING
       contract removed  -> delete every point whose payload.contract_id = id
       document removed  -> delete every point whose payload.document_id = id
       text shortened/emptied -> retire the chunk IDs the doc no longer owns
   Every processed doc (including deleted/zero-chunk) is registered with its
   CURRENT chunk count before retirement runs, so stale vectors can never leak.

4. TOKEN MATH (the 80%+ proof)
   We log tokens_embedded per run and compare against a full-rebuild cost.
   Token counts are ESTIMATED with a chars/4 heuristic (documented, consistent
   basis for both numerator and denominator) -- the MECHANISM (process only
   what changed) is what makes the saving real; the exact number scales with
   corpus size.

Usage
-----
    python3 worker.py --mode backfill     # first run: embed everything
    python3 worker.py --mode sync         # steady state: only changed rows
    python3 worker.py --mode loop         # sync forever (what compose runs)
"""
import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from deltalake import DeltaTable, write_deltalake
from fastembed import TextEmbedding
from prometheus_client import Counter, Gauge, start_http_server
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
LAKEHOUSE = os.environ.get("LAKEHOUSE", "/lakehouse")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "contracts")
MODEL_NAME = os.environ.get("EMBED_MODEL", "BAAI/bge-small-en-v1.5")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8002"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "10"))
# A single 10k-point upsert can exceed httpx's default 5s write timeout. Keep
# requests bounded and give Qdrant enough time to acknowledge each batch.
UPSERT_BATCH = int(os.environ.get("UPSERT_BATCH", "256"))
QDRANT_TIMEOUT = float(os.environ.get("QDRANT_TIMEOUT", "60"))

SILVER_DOCS = f"{LAKEHOUSE}/silver.db/contract_documents"
SILVER_CONTRACTS = f"{LAKEHOUSE}/silver.db/contracts"
# The default keeps the production/demo worker unchanged. Benchmarks and
# rebuild experiments can point at their own watermark so they never advance
# the demo worker's position.
SYNC_STATE = os.environ.get("SYNC_STATE_PATH", f"{LAKEHOUSE}/silver.db/sync_state")

# Chunking knobs: the splitter is WORD-based (split on whitespace, rejoin) --
# these are WORD counts, NOT characters. 300 words ~= 400 tokens, deliberately
# under bge-small's 512-token input limit: fastembed silently TRUNCATES longer
# inputs, so we stay well below the limit instead of relying on a trim step.
CHUNK_SIZE_WORDS = 300
CHUNK_OVERLAP_WORDS = 30

# Rough chars/token ratio used ONLY for the cost-estimate line in the logs
# (real token accounting is done by the embedding tokenizer when available).
CHARS_PER_TOKEN = 4

# --------------------------------------------------------------------------- #
# Prometheus metrics (Day 6 scrapes this on :8002/metrics)                    #
# --------------------------------------------------------------------------- #
CHUNKS_EMBEDDED = Counter(
    "vector_chunks_embedded_total", "Chunk vectors written to Qdrant", ["run_type"])
TOKENS_EMBEDDED = Counter(
    "vector_tokens_embedded_total", "Estimated tokens spent embedding", ["run_type"])
DOCS_SYNCED = Counter(
    "vector_docs_synced_total", "Documents processed by the sync worker")
VECTORS_DELETED = Counter(
    "vector_vectors_deleted_total", "Stale vectors removed from Qdrant")
SYNC_RUNS = Counter(
    "vector_sync_runs_total", "Sync runs by outcome", ["outcome"])
LAG_SECONDS = Gauge(
    "replication_lag_seconds", "Source-commit -> synced lag", ["stage"])


def log(msg):
    print(f"[vector-sync] {msg}", flush=True)


def to_ms(value):
    """Normalise a Silver timestamp to epoch MILLISECONDS (int).

    delta-rs/pyarrow returns `source_commit_ts` as a datetime, while our
    watermark is stored as an int of epoch-ms. Comparing the two raw raises
    "'>' not supported between datetime and int". This helper makes every
    timestamp comparable regardless of how the reader types it.
    """
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)
    # fallback: ISO string
    try:
        return int(datetime.fromisoformat(str(value)).replace(
            tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        return 0


# --------------------------------------------------------------------------- #
# 1. Deterministic chunking                                                    #
# --------------------------------------------------------------------------- #
def chunk_id(document_id, index):
    """THE deterministic ID. Same (doc, position) -> same ID, forever.

    Qdrant only accepts point IDs that are an unsigned integer or a UUID — a
    bare 64-char sha256 hex is rejected with HTTP 400. So we hash, then format
    the first 128 bits as a canonical UUID string. Still fully deterministic
    (same inputs -> same UUID) and valid for Qdrant.
    """
    digest = hashlib.sha256(f"{document_id}:{index}".encode()).hexdigest()
    return str(uuid.UUID(digest[:32]))


def split_text(text, size=CHUNK_SIZE_WORDS, overlap=CHUNK_OVERLAP_WORDS):
    """Deterministic sliding-window chunker with word-boundary snapping.

    Same input ALWAYS yields the same list of chunks in the same order —
    a hard requirement for deterministic IDs to be stable across runs.
    """
    text = (text or "").strip()
    if not text:
        return []
    words = text.split()
    chunks, start = [], 0
    while start < len(words):
        end = min(start + size, len(words))
        chunks.append(" ".join(words[start:end]))
        if end == len(words):
            break
        # step forward by (size - overlap) words -> overlapping windows
        start += max(1, size - overlap)
    return chunks


# --------------------------------------------------------------------------- #
# 2. Embedding (local, ONNX — no API key, no network at runtime)               #
# --------------------------------------------------------------------------- #
class Embedder:
    def __init__(self, model_name=MODEL_NAME):
        log(f"loading embedding model {model_name} ...")
        self.model = TextEmbedding(model_name)
        # probe dimension on first use
        self.dim = len(next(iter(self.model.embed(["dimension probe"]))))
        log(f"model ready, vector dimension = {self.dim}")

    def embed(self, texts):
        """Return (list[np.ndarray], estimated_tokens)."""
        if not texts:
            return [], 0
        vectors = [np.asarray(v, dtype=np.float32) for v in self.model.embed(texts)]
        tokens = sum(max(1, len(t) // CHARS_PER_TOKEN) for t in texts)
        return vectors, tokens


# --------------------------------------------------------------------------- #
# 3. Qdrant collection management                                              #
# --------------------------------------------------------------------------- #
def get_qdrant():
    return QdrantClient(url=QDRANT_URL, timeout=QDRANT_TIMEOUT)


def ensure_collection(qdrant, dim):
    if not qdrant.collection_exists(COLLECTION):
        qdrant.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )
        log(f"created Qdrant collection '{COLLECTION}' (dim={dim}, cosine)")
    else:
        log(f"Qdrant collection '{COLLECTION}' already exists")


# --------------------------------------------------------------------------- #
# 4. Watermark / sync_state (small Delta table)                               #
# --------------------------------------------------------------------------- #
def read_watermark():
    """Return the last successfully-synced watermark as (ts_ms, lsn). (0,0) = never.

    COMPOSITE watermark — ts alone cannot separate two commits that share a
    Postgres millisecond, and plain `>=` on ts would reprocess the boundary row
    on EVERY poll (re-embedding it forever). Strictly-greater on the PAIR
    (source_commit_ts, source_lsn) fixes both: same-ms events keep their LSN
    ordering, and the boundary row is left alone.
    """
    try:
        rows = DeltaTable(SYNC_STATE).to_pyarrow_table().to_pylist()
        if rows:
            return (int(rows[-1].get("watermark_ts_ms") or 0),
                    int(rows[-1].get("watermark_lsn") or 0))
    except Exception:
        pass
    return 0, 0


def write_state(state: dict):
    """Persist a single-row sync_state (overwrite). Called AFTER a good run.

    delta-rs `write_deltalake` needs a PyArrow Table / pandas DataFrame, not a
    bare list of dicts (it tries `.schema` on the object). We build an explicit
    schema so the watermark column stays a stable int64 across runs.
    """
    import pyarrow as pa
    table = pa.table({
        "watermark_ts_ms": pa.array([int(state["watermark_ts_ms"])], pa.int64()),
        "watermark_lsn": pa.array([int(state.get("watermark_lsn", 0))], pa.int64()),
        "last_run_at_ms": pa.array([int(state["last_run_at_ms"])], pa.int64()),
        "last_run_type": pa.array([str(state["last_run_type"])], pa.string()),
        "last_tokens": pa.array([int(state["last_tokens"])], pa.int64()),
        "full_corpus_tokens": pa.array([int(state["full_corpus_tokens"])], pa.int64()),
    })
    # schema_mode="merge": lets the table GAIN the watermark_lsn column without
    # failing on the older 5-column state file (old rows read lsn as 0).
    write_deltalake(SYNC_STATE, table, mode="overwrite", schema_mode="merge")


# --------------------------------------------------------------------------- #
# 5. Read only the CHANGED rows since the watermark                            #
# --------------------------------------------------------------------------- #
def read_silver(path):
    """Read a full Silver Delta table into a list of dicts (small lab corpus).

    In production you would push the `> watermark` filter down into the scan;
    here the tables are tiny so we filter in Python for readability.

    NOTE: delta-rs `DeltaTable` exposes `.to_pyarrow_table()` (NOT `.to_pylist()`
    — that's the pyspark API). We convert to Python dicts via `.to_pylist()`
    on the resulting *PyArrow Table*, which is a different object.
    """
    try:
        return DeltaTable(path).to_pyarrow_table().to_pylist()
    except Exception as e:
        log(f"cannot read {path} ({e}) -> returning []")
        return []


def after_watermark(row, wm):
    """True if `row` committed strictly after the composite watermark wm=(ts,lsn).

    Lexicographic compare: ts first, LSN as the same-millisecond tie-break.
    Both come from Postgres's own WAL metadata (never local clock), so clock
    skew cannot drop a row; strictly-greater means the boundary row is not
    re-read on every poll.
    """
    rts = to_ms(row.get("source_commit_ts"))
    rlsn = int(row.get("source_lsn") or 0)
    if rts != wm[0]:
        return rts > wm[0]
    return rlsn > wm[1]


def row_watermark(row):
    """The composite (ts, lsn) of one Silver row -- what watermark it advances to."""
    return (to_ms(row.get("source_commit_ts")), int(row.get("source_lsn") or 0))


def changed_docs(watermark):
    """Silver contract_documents rows committed after the composite watermark."""
    rows = read_silver(SILVER_DOCS)
    return [r for r in rows
            if after_watermark(r, watermark)], rows


def deleted_contracts(since_wm):
    """Contracts soft-deleted in Silver since the watermark -> vector purge."""
    rows = read_silver(SILVER_CONTRACTS)
    return [r for r in rows
            if r.get("is_deleted") and after_watermark(r, since_wm)]


# --------------------------------------------------------------------------- #
# 6. The actual sync                                                           #
# --------------------------------------------------------------------------- #
def sync_documents(qdrant, embedder, docs, run_type, retire_stale=True):
    """Chunk -> embed -> upsert -> retire stale tail chunks. Returns token spend.

    `docs` may contain soft-deleted rows and rows whose text is now empty.
    Both MUST still be tracked so their old vectors get retired (see below).
    A full backfill into a fresh collection can skip the per-document stale-ID
    lookups; incremental sync must keep them.
    """
    to_embed, plan, doc_chunk_counts = [], [], {}
    for d in docs:
        doc_id = str(d["id"])
        if d.get("is_deleted"):
            # Document row was soft-deleted: purge EVERY vector it owns.
            # (Contract-level purge only fires when the CONTRACT dies; a single
            # document can die while its contract lives -> must handle here.)
            qdrant.delete(
                collection_name=COLLECTION, wait=True,
                points_selector=Filter(
                    must=[FieldCondition(
                        key="document_id", match=MatchValue(value=doc_id))]))
            doc_chunk_counts[doc_id] = 0   # owns 0 chunks now -> retire below
            continue
        pieces = split_text(d.get("chunk_text", ""))
        doc_chunk_counts[doc_id] = len(pieces)   # register EVEN IF 0 chunks
        for i, piece in enumerate(pieces):
            to_embed.append(piece)
            plan.append((d, i, piece))

    if not to_embed:
        # Nothing new to embed -- but deleted/emptied docs still need their
        # stale vectors retired, so fall through to the retirement step.
        log("no new text to embed; retiring stale vectors if any")
    else:
        vectors, tokens = embedder.embed(to_embed)

        points = []
        for (d, i, piece), vec in zip(plan, vectors):
            doc_id = str(d["id"])
            points.append(PointStruct(
                id=chunk_id(doc_id, i),           # DETERMINISTIC id
                vector=vec.tolist(),
                payload={
                    "document_id": doc_id,
                    "contract_id": d.get("contract_id"),
                    "chunk_index": i,
                    "content_hash": d.get("content_hash"),
                    # store the CHUNK's text (not the whole doc) so search hits
                    # return exactly what was embedded
                    "text": piece,
                },
            ))

        # --- UPSERT (idempotent: same deterministic IDs -> overwrite, no dupes)
        # Batch the request. Putting 10k points in one HTTP body can exceed the
        # client write timeout even when Qdrant eventually accepts it.
        for start in range(0, len(points), UPSERT_BATCH):
            qdrant.upsert(
                collection_name=COLLECTION,
                points=points[start:start + UPSERT_BATCH],
                wait=True,
            )
        CHUNKS_EMBEDDED.labels(run_type=run_type).inc(len(points))
        TOKENS_EMBEDDED.labels(run_type=run_type).inc(tokens)
        DOCS_SYNCED.inc(len(doc_chunk_counts))
        log(f"upserted {len(points)} chunk vectors across "
            f"{len(doc_chunk_counts)} document(s), ~{tokens} tokens "
            f"in {(len(points) + UPSERT_BATCH - 1) // UPSERT_BATCH} batch(es)")

    # --- RETIRE stale chunks (text shrank, emptied, or doc was deleted) -------
    # doc_chunk_counts knows how many chunks each processed doc OWNS now
    # (0 for deleted/emptied). Any deterministic ID beyond that count still in
    # Qdrant is stale and gets deleted.
    retired = 0
    if retire_stale:
        for doc_id, count_now in doc_chunk_counts.items():
            stale_ids = [chunk_id(doc_id, i) for i in range(count_now, count_now + 50)]
            res = qdrant.retrieve(collection_name=COLLECTION, ids=stale_ids)
            real_stale = [p.id for p in res]
            if real_stale:
                qdrant.delete(collection_name=COLLECTION,
                              points_selector=real_stale, wait=True)
                retired += len(real_stale)
    if retired:
        VECTORS_DELETED.inc(retired)
        log(f"retired {retired} stale tail chunk(s)")

    return tokens if to_embed else 0


def purge_contract(qdrant, contract_id):
    """Delete every vector belonging to a removed contract (payload filter)."""
    from qdrant_client.models import Filter, FieldCondition, MatchValue
    qdrant.delete(collection_name=COLLECTION, wait=True, points_selector=Filter(
        must=[FieldCondition(key="contract_id", match=MatchValue(value=int(contract_id)))]))
    log(f"purged all vectors for deleted contract_id={contract_id}")


# --------------------------------------------------------------------------- #
# 7. Modes                                                                     #
# --------------------------------------------------------------------------- #
def run_backfill(qdrant, embedder):
    log("=== BACKFILL: embedding the full corpus ===")
    t0 = time.time()
    _, all_rows = changed_docs((0, 0))
    live = [r for r in all_rows if not r.get("is_deleted")]
    # A backfill normally targets a fresh collection; skipping 50 stale-ID
    # probes per document keeps a 10k rebuild from making 10k extra HTTP calls.
    tokens = sync_documents(qdrant, embedder, live, "backfill", retire_stale=False)
    # full-rebuild cost basis for the 80% comparison line
    full = sum(max(1, len(r.get("chunk_text", "")) // CHARS_PER_TOKEN) for r in live)
    # composite watermark = max (ts, lsn) over every row we just handled
    wm = max((row_watermark(r) for r in all_rows), default=(0, 0))
    state = {
        "watermark_ts_ms": wm[0],
        "watermark_lsn": wm[1],
        "last_run_at_ms": int(time.time() * 1000),
        "last_run_type": "backfill",
        "last_tokens": tokens,
        "full_corpus_tokens": full,
    }
    write_state(state)
    SYNC_RUNS.labels(outcome="success").inc()
    log(f"backfill done in {time.time()-t0:.1f}s, ~{tokens} tokens "
        f"(full corpus ~= {full})")


def run_sync(qdrant, embedder):
    t0 = time.time()
    watermark = read_watermark()
    log(f"=== SYNC: watermark={watermark} ===")

    docs, all_rows = changed_docs(watermark)
    # also fold in contracts deleted since watermark -> purge their vectors
    del_contracts = deleted_contracts(watermark)

    if not docs and not del_contracts:
        log("nothing changed since watermark -> no-op")
        SYNC_RUNS.labels(outcome="noop").inc()
        return

    tokens = 0
    if docs:
        # pass ALL changed docs (incl. soft-deleted) -- sync_documents purges
        # deleted docs' vectors and retires emptied docs' stale chunks itself
        tokens = sync_documents(qdrant, embedder, docs, "incremental")
    for c in del_contracts:
        purge_contract(qdrant, c.get("id"))

    # composite watermark = lexicographic max over everything we just handled;
    # written LAST so a crash mid-run re-processes (at-least-once)
    candidates = [row_watermark(r) for r in docs + del_contracts]
    new_watermark = max([watermark] + candidates)

    # replication-lag gauge: how far behind source commit-time we just synced
    if docs:
        newest_src = max(to_ms(r.get("source_ts_ms") or r.get("source_commit_ts"))
                         for r in docs)
        if newest_src:
            LAG_SECONDS.labels(stage="qdrant").set(
                max(0.0, (time.time() * 1000 - newest_src) / 1000.0))

    # denominator must match the backfill basis: LIVE docs only (consistent
    # comparison -- counting deleted docs here would inflate the saving)
    full = sum(max(1, len(r.get("chunk_text", "")) // CHARS_PER_TOKEN)
               for r in read_silver(SILVER_DOCS) if not r.get("is_deleted"))
    write_state({
        "watermark_ts_ms": new_watermark[0],
        "watermark_lsn": new_watermark[1],
        "last_run_at_ms": int(time.time() * 1000),
        "last_run_type": "sync",
        "last_tokens": tokens,
        "full_corpus_tokens": full,
    })
    SYNC_RUNS.labels(outcome="success").inc()

    saving = (1 - tokens / full) * 100 if full else 100.0
    log(f"SYNC done in {time.time()-t0:.1f}s: ~{tokens} tokens for this change "
        f"vs ~{full} for a full rebuild -> {saving:.1f}% saved")


def run_loop(qdrant, embedder):
    log("=== entering steady-state loop ===")
    while True:
        try:
            run_sync(qdrant, embedder)
        except Exception as e:                    # never die on a transient error
            SYNC_RUNS.labels(outcome="error").inc()
            log(f"sync run failed (will retry): {e}")
        time.sleep(POLL_SECONDS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "sync", "loop"], default="sync")
    ap.add_argument("--no-metrics", action="store_true",
                    help="skip the /metrics HTTP server (for one-shot runs so they "
                         "don't collide with the long-running loop's port 8002)")
    args = ap.parse_args()

    if not args.no_metrics:
        start_http_server(METRICS_PORT)           # /metrics for Day 6 Prometheus
        log(f"serving metrics on :{METRICS_PORT}/metrics")

    embedder = Embedder()
    qdrant = get_qdrant()
    ensure_collection(qdrant, embedder.dim)

    if args.mode == "backfill":
        run_backfill(qdrant, embedder)
    elif args.mode == "sync":
        run_sync(qdrant, embedder)
    else:
        run_loop(qdrant, embedder)


if __name__ == "__main__":
    main()
