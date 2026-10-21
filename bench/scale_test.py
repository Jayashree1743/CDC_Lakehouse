#!/usr/bin/env python3
"""Reproducible scale test for the CDC lakehouse vector-sync path.

This script intentionally uses a separate Qdrant collection, worker watermark,
and API container. It does not touch the demo `contracts` collection.

Example (10,000 documents):

    pip install -r bench/requirements.txt
    python3 bench/scale_test.py --documents 10000

What it measures:
  - full rebuild: worker wall time until all live documents are in Qdrant
  - incremental edit: Postgres commit -> first search hit containing the marker
  - API latency at selected concurrency levels (optional)

The source Postgres and Silver tables are shared with the demo stack. The
script stops the demo vector-sync worker while it runs so the demo collection is
not polluted, and restarts it afterwards.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "cdc-lakehouse"
NETWORK = f"{PROJECT}_default"
POSTGRES = "cdc-postgres"
DEMO_WORKER = "cdc-vector-sync"
WORKER_IMAGE = "cdc-lakehouse-vector-sync:latest"
API_IMAGE = "cdc-lakehouse-api:latest"
LAKEHOUSE = ROOT / "lakehouse"
PSQL = [
    "docker", "exec", "-i", POSTGRES, "psql", "-U", "demo", "-d", "inventory",
    "-v", "ON_ERROR_STOP=1", "-q", "-tA",
]


def run(cmd: list[str], *, check: bool = True, **kwargs: Any) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, **kwargs)


def psql(sql: str) -> str:
    result = run(PSQL + ["-c", sql], check=True, capture_output=True)
    return result.stdout.strip()


def qdrant_points(collection: str) -> int | None:
    try:
        response = requests.get(
            f"http://localhost:6333/collections/{collection}", timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return int(response.json()["result"].get("points_count") or 0)
    except requests.RequestException:
        return None


def delete_collection(collection: str) -> None:
    try:
        requests.delete(
            f"http://localhost:6333/collections/{collection}", timeout=10)
    except requests.RequestException:
        pass


def remove_worker(name: str) -> None:
    run(["docker", "rm", "-f", name], check=False, capture_output=True)


def remove_path(path: str) -> None:
    """Remove a root-created path inside the lakehouse mount."""
    run(
        [
            "docker", "run", "--rm", "-v", f"{ROOT}:/work", "alpine:3",
            "sh", "-c", f"rm -rf '/work/{path}'",
        ],
        check=False,
        capture_output=True,
    )


def silver_counts() -> tuple[int, int]:
    code = (
        "from deltalake import DeltaTable\n"
        "rows = DeltaTable('/lakehouse/silver.db/contract_documents')"
        ".to_pyarrow_table().to_pylist()\n"
        "print(len(rows), sum(1 for row in rows if not row.get('is_deleted')))\n"
    )
    result = run(
        [
            "docker", "run", "--rm", "-v", f"{LAKEHOUSE}:/lakehouse",
            "--entrypoint", "python", API_IMAGE, "-c", code,
        ],
        check=True,
        capture_output=True,
    )
    total, live = result.stdout.strip().split()[-2:]
    return int(total), int(live)


def silver_doc_count() -> int:
    return silver_counts()[0]


def wait_for_silver(expected: int, timeout: int = 300) -> None:
    print(f"waiting for Silver to reach {expected} document rows ...", flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            count = silver_doc_count()
        except subprocess.CalledProcessError:
            count = -1
        if count >= expected:
            print(f"  Silver rows: {count}", flush=True)
            return
        print(f"  Silver rows: {count}", flush=True)
        time.sleep(5)
    raise TimeoutError(f"Silver did not reach {expected} rows within {timeout}s")


def wait_for_api(port: int, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request = requests.get(f"http://localhost:{port}/health", timeout=5)
            if request.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"bench API did not become healthy on :{port}")


def search(port: int, query: str, top_k: int = 50) -> list[dict[str, Any]]:
    response = requests.post(
        f"http://localhost:{port}/search",
        headers={"x-api-key": "demo-key"},
        json={"query": query, "top_k": top_k, "min_score": 0.0},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("hits", [])


def seed_corpus(contracts: int, docs_per_contract: int) -> int:
    total = contracts * docs_per_contract
    print(f"seeding {contracts} contracts x {docs_per_contract} documents = {total}", flush=True)

    # Only remove rows this harness owns. Deleting them also exercises the CDC
    # delete path, but the demo worker is stopped so it cannot pollute the demo
    # collection.
    psql(
        "DELETE FROM contract_documents WHERE content_hash LIKE 'scale-%';"
        "DELETE FROM contracts WHERE title LIKE 'SCALE-%';"
    )
    psql(
        "INSERT INTO contracts (customer_id, title, monthly_price, status) "
        "SELECT 1 + (g % 2), 'SCALE-' || lpad(g::text, 5, '0'), 100 + g, 'active' "
        f"FROM generate_series(1, {contracts}) g;"
    )
    psql(
        "INSERT INTO contract_documents (contract_id, chunk_text, content_hash) "
        "SELECT c.id, "
        "  'Scale benchmark document ' || lpad(((c.rn - 1) * "
        f"{docs_per_contract} + d)::text, 6, '0') || ' for ' || c.title || '. ' || "
        "  'The supplier shall provide enterprise support with a fifteen minute "
        "response target for severity one incidents and continuous engagement "
        "until restoration. The customer shall pay invoices within thirty days, "
        "the renewal price increase is capped at five percent, the supplier shall "
        "encrypt data at rest and in transit, report confirmed breaches within "
        "seventy two hours, and return or delete customer data within ninety days. "
        "Unique marker scale-doc-' || lpad(((c.rn - 1) * "
        f"{docs_per_contract} + d)::text, 6, '0') || '.', "
        "  'scale-' || lpad(((c.rn - 1) * "
        f"{docs_per_contract} + d)::text, 6, '0') "
        "FROM (SELECT id, title, row_number() OVER (ORDER BY id) AS rn "
        "      FROM contracts WHERE title LIKE 'SCALE-%') c "
        f"CROSS JOIN generate_series(1, {docs_per_contract}) d;"
    )
    return total


def start_bench_worker(collection: str, sync_state: str, name: str) -> None:
    remove_worker(name)
    remove_path(sync_state.removeprefix("/lakehouse/"))
    command = [
        "docker", "run", "--rm", "-d", "--name", name,
        "--network", NETWORK,
        "-v", f"{LAKEHOUSE}:/lakehouse",
        "-v", f"{ROOT / 'vector_sync' / 'worker.py'}:/worker.py",
        "-e", "QDRANT_URL=http://qdrant:6333",
        "-e", "LAKEHOUSE=/lakehouse",
        "-e", f"QDRANT_COLLECTION={collection}",
        "-e", f"SYNC_STATE_PATH={sync_state}",
        "-e", "EMBED_MODEL=BAAI/bge-small-en-v1.5",
        "-e", "POLL_SECONDS=2",
        WORKER_IMAGE,
        "python3", "/worker.py", "--mode", "loop", "--no-metrics",
    ]
    run(command, check=True)


def start_bench_api(collection: str, port: int, name: str) -> None:
    remove_worker(name)
    run(
        [
            "docker", "run", "--rm", "-d", "--name", name,
            "--network", NETWORK, "-p", f"{port}:8000",
            "-v", f"{LAKEHOUSE}:/lakehouse",
            "-e", "QDRANT_URL=http://qdrant:6333",
            "-e", "LAKEHOUSE=/lakehouse",
            "-e", f"QDRANT_COLLECTION={collection}",
            "-e", "EMBED_MODEL=BAAI/bge-small-en-v1.5",
            "-e", "API_KEY=demo-key",
            API_IMAGE,
        ],
        check=True,
    )


def full_rebuild(collection: str, sync_state: str, expected_points: int, name: str) -> dict[str, Any]:
    remove_worker(name)
    print("starting full rebuild (backfill) ...", flush=True)
    started = time.perf_counter()
    result = run(
        [
            "docker", "run", "--rm", "--name", name,
            "--network", NETWORK,
            "-v", f"{LAKEHOUSE}:/lakehouse",
            "-v", f"{ROOT / 'vector_sync' / 'worker.py'}:/worker.py",
            "-e", "QDRANT_URL=http://qdrant:6333",
            "-e", "LAKEHOUSE=/lakehouse",
            "-e", f"QDRANT_COLLECTION={collection}",
            "-e", f"SYNC_STATE_PATH={sync_state}",
            "-e", "EMBED_MODEL=BAAI/bge-small-en-v1.5",
            WORKER_IMAGE,
            "python3", "/worker.py", "--mode", "backfill", "--no-metrics",
        ],
        check=False,
        capture_output=True,
    )
    elapsed = time.perf_counter() - started
    if result.returncode != 0:
        raise RuntimeError(
            "backfill failed:\n" + (result.stdout or "")[-4000:] + (result.stderr or "")[-4000:]
        )

    points = qdrant_points(collection)
    if points is not None and points < expected_points:
        raise RuntimeError(f"backfill ended with {points} points; expected {expected_points}")
    print(f"full rebuild: {elapsed:.1f}s wall, {points} points", flush=True)
    return {
        "wall_seconds": round(elapsed, 2),
        "points": points,
        "worker_log_tail": (result.stdout or "").splitlines()[-8:],
    }


def incremental_edit(port: int, worker_name: str) -> dict[str, Any]:
    token = f"scale-edit-{int(time.time())}"
    # Repeat the marker so it is not drowned out by 10k structurally similar
    # documents. The point of the check is "did the new text become searchable",
    # not whether one random token happens to rank in the top 50.
    marker = " ".join([token] * 8)
    psql(
        "UPDATE contract_documents SET "
        f"chunk_text = chunk_text || ' scale edit marker {marker}.', updated_at = now() "
        "WHERE content_hash IN ("
        "  SELECT content_hash FROM contract_documents "
        "  WHERE content_hash LIKE 'scale-%' ORDER BY content_hash LIMIT 10);"
    )
    committed = time.perf_counter()

    visible_at = None
    last_hits: list[dict[str, Any]] = []
    while time.perf_counter() - committed < 90:
        try:
            last_hits = search(port, f"scale edit marker {marker}", top_k=200)
            if any(token in (hit.get("text") or "") for hit in last_hits):
                visible_at = time.perf_counter()
                break
        except requests.RequestException:
            pass
        time.sleep(0.2)
    if visible_at is None:
        logs = run(
            ["docker", "logs", "--tail", "40", worker_name],
            check=False, capture_output=True,
        )
        detail = (logs.stdout or "") + (logs.stderr or "")
        raise TimeoutError(
            "incremental edit was not visible within 90s; "
            f"last hit count={len(last_hits)}; worker log tail:\n{detail[-3000:]}"
        )

    ttv = visible_at - committed
    print(f"incremental edit: time-to-visible {ttv:.2f}s", flush=True)
    return {"token": token, "time_to_visible_seconds": round(ttv, 2)}


def latency(port: int, concurrency_levels: list[int], requests_per_level: int) -> dict[str, Any]:
    query = "enterprise support response target for severity one incidents"

    def one() -> float:
        start = time.perf_counter()
        search(port, query, top_k=5)
        return (time.perf_counter() - start) * 1000.0

    results: dict[str, Any] = {}
    for concurrency in concurrency_levels:
        samples: list[float] = []
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(one) for _ in range(requests_per_level)]
            for future in as_completed(futures):
                samples.append(future.result())
        wall = time.perf_counter() - started
        samples.sort()
        p50 = statistics.median(samples)
        p95 = samples[min(len(samples) - 1, math.ceil(len(samples) * 0.95) - 1)]
        results[str(concurrency)] = {
            "requests": len(samples),
            "rps": round(len(samples) / wall, 1),
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
        }
        print(f"latency c={concurrency}: p50={p50:.1f}ms p95={p95:.1f}ms", flush=True)
    return results


def cleanup(collection: str, sync_state: str, port: int, worker_name: str, api_name: str, demo_was_running: bool) -> None:
    print("cleaning up benchmark resources ...", flush=True)
    remove_worker(worker_name)
    remove_worker(api_name)
    delete_collection(collection)
    remove_path(sync_state.removeprefix("/lakehouse/"))
    psql(
        "DELETE FROM contract_documents WHERE content_hash LIKE 'scale-%';"
        "DELETE FROM contracts WHERE title LIKE 'SCALE-%';"
    )
    if demo_was_running:
        run(["docker", "start", DEMO_WORKER], check=False, capture_output=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a reproducible scale test")
    parser.add_argument("--documents", type=int, help="Total documents; overrides --contracts")
    parser.add_argument("--contracts", type=int, default=500)
    parser.add_argument("--docs-per-contract", type=int, default=20)
    parser.add_argument("--collection", default="contracts_scale")
    parser.add_argument("--sync-state", default="/lakehouse/silver.db/sync_state_scale")
    parser.add_argument("--api-port", type=int, default=8011)
    parser.add_argument("--worker-name", default="cdc-vector-sync-bench")
    parser.add_argument("--api-name", default="cdc-api-bench")
    parser.add_argument("--latency-requests", type=int, default=100)
    parser.add_argument("--latency-concurrency", default="1,10")
    parser.add_argument("--skip-latency", action="store_true")
    parser.add_argument("--keep", action="store_true", help="Keep bench data and containers")
    parser.add_argument("--output", default="bench/results/scale.json")
    args = parser.parse_args()

    if args.documents is not None:
        if args.documents <= 0 or args.documents % args.docs_per_contract != 0:
            raise SystemExit("--documents must be a positive multiple of --docs-per-contract")
        args.contracts = args.documents // args.docs_per_contract

    if shutil.which("docker") is None:
        raise SystemExit("docker is required")
    if not (ROOT / "lakehouse").exists():
        raise SystemExit("lakehouse/ is missing; run ./run_all.sh first")

    total_docs = args.contracts * args.docs_per_contract
    concurrency_levels = [int(x) for x in args.latency_concurrency.split(",") if x.strip()]
    demo_was_running = run(
        ["docker", "inspect", "-f", "{{.State.Running}}", DEMO_WORKER],
        check=False, capture_output=True,
    ).stdout.strip() == "true"

    started_at = time.time()
    report: dict[str, Any] = {
        "config": {
            "contracts": args.contracts,
            "documents": total_docs,
            "collection": args.collection,
            "sync_state": args.sync_state,
            "latency_requests_per_level": args.latency_requests,
            "latency_concurrency": concurrency_levels,
            "demo_worker_running_at_start": demo_was_running,
        }
    }

    try:
        if demo_was_running:
            print("stopping demo vector-sync so the demo collection stays untouched", flush=True)
            run(["docker", "stop", DEMO_WORKER], check=False, capture_output=True)

        baseline_silver, baseline_live = silver_counts()
        report["config"]["baseline_silver_rows"] = baseline_silver
        report["config"]["baseline_silver_live_rows"] = baseline_live
        remove_path(args.sync_state.removeprefix("/lakehouse/"))
        delete_collection(args.collection)

        inserted = seed_corpus(args.contracts, args.docs_per_contract)
        wait_for_silver(baseline_silver + inserted)
        _, live_after_ingest = silver_counts()

        report["config"]["silver_live_rows_after_ingest"] = live_after_ingest
        report["full_rebuild"] = full_rebuild(
            args.collection, args.sync_state, live_after_ingest, args.worker_name)

        start_bench_worker(args.collection, args.sync_state, args.worker_name)
        start_bench_api(args.collection, args.api_port, args.api_name)
        wait_for_api(args.api_port)
        time.sleep(2)

        report["incremental"] = incremental_edit(args.api_port, args.worker_name)
        if not args.skip_latency:
            report["latency"] = latency(
                args.api_port, concurrency_levels, args.latency_requests)
    finally:
        if not args.keep:
            cleanup(
                args.collection, args.sync_state, args.api_port,
                args.worker_name, args.api_name, demo_was_running)

    report["elapsed_seconds"] = round(time.time() - started_at, 1)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nwrote {output}", flush=True)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise
