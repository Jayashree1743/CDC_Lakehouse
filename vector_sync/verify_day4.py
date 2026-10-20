#!/usr/bin/env python3
"""
DAY 4 verifier - proves the incremental vector sync actually works.
Run inside the vector-sync container:  python3 /verify_day4.py

Shows:
  1. Qdrant collection stats (points, dim)
  2. The deterministic-ID property (same doc+index -> same vector id)
  3. Per-document chunk counts (so "only N chunks re-embedded" is checkable)
  4. The watermark currently held in sync_state
  5. A semantic search sanity check (fuzzy question -> right clause)
"""
import hashlib
import os
import uuid

from deltalake import DeltaTable
from qdrant_client import QdrantClient

LAKEHOUSE = os.environ.get("LAKEHOUSE", "/lakehouse")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "contracts")


def chunk_id(document_id, index):
    digest = hashlib.sha256(f"{document_id}:{index}".encode()).hexdigest()
    return str(uuid.UUID(digest[:32]))


def main():
    qd = QdrantClient(url=QDRANT_URL)

    print("=" * 64)
    print("1. QDRANT COLLECTION")
    print("=" * 64)
    if qd.collection_exists(COLLECTION):
        info = qd.get_collection(COLLECTION)
        print(f"  collection '{COLLECTION}': points={info.points_count}, "
              f"dim={info.config.params.vectors.size}")
    else:
        print("  collection does not exist yet")
        return

    print()
    print("=" * 64)
    print("2. POINTS PER DOCUMENT (chunk counts)")
    print("=" * 64)
    # scroll all points, group by contract_id in payload
    offset = None
    by_contract, total = {}, 0
    while True:
        points, offset = qd.scroll(collection_name=COLLECTION, limit=100,
                                   offset=offset, with_payload=["contract_id", "chunk_index"])
        if not points:
            break
        for p in points:
            total += 1
            cid = p.payload.get("contract_id")
            by_contract[cid] = by_contract.get(cid, 0) + 1
        if offset is None:
            break
    print(f"  total points: {total}")
    for cid in sorted(by_contract, key=lambda x: (x is None, x)):
        print(f"    contract_id={cid}: {by_contract[cid]} chunk vectors")

    print()
    print("=" * 64)
    print("3. DETERMINISTIC-ID PROOF")
    print("=" * 64)
    sample_doc, sample_idx = "4", 0   # doc id 4 (Acme doc), chunk 0
    expected = chunk_id(sample_doc, sample_idx)
    got = qd.retrieve(collection_name=COLLECTION, ids=[expected])
    print(f"  sha256('{sample_doc}:{sample_idx}') = {expected[:24]}...")
    print(f"  present in Qdrant: {bool(got)}  "
          f"(recomputing always yields this same id -> upsert is idempotent)")

    print()
    print("=" * 64)
    print("4. WATERMARK (sync_state)")
    print("=" * 64)
    try:
        rows = DeltaTable(f"{LAKEHOUSE}/silver.db/sync_state").to_pyarrow_table().to_pylist()
        for r in rows:
            print(f"  {r}")
    except Exception as e:
        print(f"  (no sync_state yet: {e})")

    print()
    print("=" * 64)
    print("5. SEMANTIC SEARCH SANITY CHECK")
    print("=" * 64)
    try:
        from fastembed import TextEmbedding
        import numpy as np
        model = TextEmbedding(os.environ.get("EMBED_MODEL", "BAAI/bge-small-en-v1.5"))
        q = "can I get my money back if I cancel?"
        qv = np.asarray(next(iter(model.embed([q]))), dtype=np.float32).tolist()
        hits = qd.query_points(collection_name=COLLECTION, query=qv, limit=3,
                               score_threshold=0.25).points
        print(f"  query: {q!r}")
        for h in hits:
            print(f"    score={h.score:.3f} contract={h.payload.get('contract_id')} "
                  f":: {h.payload.get('text','')[:70]}...")
    except Exception as e:
        print(f"  search failed: {e}")


if __name__ == "__main__":
    main()
