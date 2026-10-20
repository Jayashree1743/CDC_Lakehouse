#!/usr/bin/env python3
"""
LIVE DEMO SAMPLER - runs 3 inserts and samples the metrics Grafana panels read.
Run:  python3 live_demo.py

Shows which dashboard panel each insert lights up:
  1. contracts UPDATE   -> CDC events (op=uc) + lag stat
  2. document change    -> vector chunks embedded + tokens + lag drop
  3. bad price insert   -> DLQ counter (panel goes red)
"""
import subprocess, time, sys, urllib.parse, urllib.request

PROM = "http://localhost:9090/api/v1/query"
PSQL = ["docker", "exec", "-i", "cdc-postgres", "psql", "-U", "demo", "-d", "inventory", "-q"]


def q(expr: str) -> str:
    """Instant PromQL query, returns value or 'n/a'."""
    url = f"{PROM}?query={urllib.parse.quote(expr)}"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            import json
            res = json.load(r)["data"]["result"]
        return res[0]["value"][1] if res else "n/a"
    except Exception:
        return "err"


def sql(stmt: str) -> None:
    subprocess.run(PSQL + ["-c", stmt], capture_output=True, text=True, timeout=30)


def snapshot(label: str) -> None:
    print(
        f"  {label:<10} cdc={q('sum(cdc_events_total)'):>6} "
        f"chunks={q('sum(vector_chunks_embedded_total)'):>5} "
        f"dlq={q('dlq_records_total'):>4} "
        f"lag={q('replication_lag_seconds{stage=\"qdrant\"}')}"
    )
    sys.stdout.flush()


def watch(seconds: int, step: int = 5) -> None:
    elapsed = 0
    while elapsed < seconds:
        time.sleep(step)
        elapsed += step
        snapshot(f"t+{elapsed}s")


def banner(n: int, title: str, panel: str) -> None:
    print(f"\n{'#' * 72}\n### DEMO {n}: {title}\n### Watch in Grafana: {panel}\n{'#' * 72}")


def main() -> None:
    print("=" * 72)
    print("BASELINE (before any inserts)")
    print("=" * 72)
    snapshot("start")

    # ---- DEMO 1: contracts UPDATE -> CDC events panel + lag stat ----
    banner(1, "normal contracts UPDATE", "panel 'CDC events processed (by op)' + lag stat")
    sql("UPDATE contracts SET monthly_price=1850.00, updated_at=now() WHERE id=1;")
    print("  INSERTED: contracts id=1 monthly_price=1850")
    watch(40)

    # ---- DEMO 2: document change -> vector chunks + tokens + lag drop ----
    banner(2, "contract DOCUMENT change", "'Vector chunks embedded' + 'Tokens embedded' + lag stat")
    sql(
        "UPDATE contract_documents SET chunk_text='Liability capped at 3x annual fees. "
        "Confidentiality survives termination for 7 years. Governing law: England.', "
        "content_hash='grafana-demo-doc', updated_at=now() WHERE id=1;"
    )
    print("  INSERTED: contract_documents id=1 (content_hash=grafana-demo-doc)")
    watch(50)

    # ---- DEMO 3: BAD DATA -> DLQ panel goes red ----
    banner(3, "BAD DATA insert (negative price)", "'DLQ records (total)' stat -> turns red")
    print("  monthly_price=-50 passes Postgres CHECK but FAILS Great Expectations (between 0.01..)")
    sql(
        "INSERT INTO contracts (customer_id, title, monthly_price, status) "
        "VALUES (1, 'Bad Data DLQ Demo', -50.00, 'active');"
    )
    print("  INSERTED: contracts row with monthly_price=-50 (should be REJECTED)")
    watch(45)

    print("\n" + "=" * 72)
    print("FINAL STATE")
    print("=" * 72)
    snapshot("final")
    print("\nOpen Grafana: http://localhost:3000/d/cdc-lakehouse-lag/cdc-lakehouse-replication-lag-and-vector-sync")


if __name__ == "__main__":
    main()
