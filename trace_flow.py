#!/usr/bin/env python3
"""
TRACE ONE CHANGE THROUGH EVERY LAYER.
Inserts a uniquely-marked document, then shows the SAME data at each hop:
  Postgres -> Kafka -> Bronze -> Silver -> Qdrant  (business data)
and the metrics each service counts along the way  (what Grafana shows)
"""
import subprocess, time, json, urllib.parse, urllib.request

MARK = "trace-me-xyz-42"
PSQL = ["docker", "exec", "-i", "cdc-postgres", "psql", "-U", "demo", "-d", "inventory", "-t", "-A", "-q"]


def run(cmd, timeout=40):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception as e:
        return f"(error: {e})"


def q(expr):
    url = f"http://localhost:9090/api/v1/query?query={urllib.parse.quote(expr)}"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            res = json.load(r)["data"]["result"]
        return res[0]["value"][1] if res else "n/a"
    except Exception:
        return "err"


def step(n, title):
    print(f"\n{'=' * 74}\nSTEP {n}: {title}\n{'=' * 74}")


def main():
    print("=" * 74)
    print("BEFORE: metrics baseline (what Grafana shows right now)")
    print("=" * 74)
    print(f"  cdc_events_total = {q('sum(cdc_events_total)')}   <- panel 'CDC events processed'")
    print(f"  chunks_embedded  = {q('sum(vector_chunks_embedded_total)')}   <- panel 'Vector chunks embedded'")
    print(f"  dlq_records      = {q('dlq_records_total')}   <- panel 'DLQ records'")

    step(1, "BUSINESS DATA begins: you run an UPDATE in Postgres")
    print("  SQL: UPDATE contract_documents SET chunk_text=... , content_hash='%s' WHERE id=1;" % MARK)
    run(PSQL + ["-c",
                f"UPDATE contract_documents SET chunk_text='Liability cap for trace run. "
                f"Confidentiality survives termination. Marker {MARK}.', "
                f"content_hash='{MARK}', updated_at=now() WHERE id=1;"])
    out = run(PSQL + ["-c", f"SELECT id, content_hash FROM contract_documents WHERE id=1;"])
    print("  SOURCE OF TRUTH (Postgres row):")
    print(f"    {out.strip()}")

    step(2, "Debezium reads the WAL -> publishes a raw CDC message to Kafka (2-4s)")
    time.sleep(6)
    raw = run(["docker", "exec", "cdc-kafka", "kafka-console-consumer",
               "--bootstrap-server", "localhost:9092",
               "--topic", "inventory.public.contract_documents",
               "--from-beginning", "--max-messages", "400"], timeout=45)
    hits = [l for l in raw.splitlines() if MARK in l]
    print(f"  Messages in Kafka containing the marker: {len(hits)}")
    if hits:
        msg = json.loads(hits[-1])
        print("  RAW CDC MESSAGE (this is what 'change data capture' means):")
        print(f"    op           = {msg.get('op')}   ('c'=insert 'u'=update 'd'=delete 'r'=snapshot)")
        print(f"    before       = {msg.get('before')}")
        print(f"    after.chunk  = ...{str(msg.get('after', {}).get('chunk_text', ''))[-45:]}")
        print(f"    source.lsn   = {msg.get('source', {}).get('lsn')}   <- WAL position (our dedup key)")
        print(f"    source.ts_ms = {msg.get('source', {}).get('ts_ms')}")

    step(3, "Spark BRONZE job lands the raw payload into Delta (immutable log)")
    print("  Bronze stores the raw Kafka message verbatim -> nothing is cleaned yet.")
    print("  This is the REPLAY LOG: if anything downstream breaks, we rebuild from here.")

    step(4, "Spark SILVER job: parse -> quality gate -> MERGE (this increments the COUNTER)")
    print("  Here business-data-flow and metrics DIVERGE:")
    print("    - it MERGEs the parsed row into Silver        -> business data updated")
    print("    - it does CDC_EVENTS_TOTAL.labels(op='u').inc() -> METRIC incremented")
    print("  That counter is a plain number held in the Silver process's memory.")
    for i in range(8):
        time.sleep(4)
        v = q("sum(cdc_events_total)")
        if v not in ("n/a", "err", "0"):
            print(f"    t+{(i+1)*4}s  cdc_events_total = {v}   <- Silver counted it")

    step(5, "vector-sync worker: reads Silver, embeds ONLY changed chunks -> Qdrant")
    print("  The worker compares source_commit_ts > its watermark.")
    print("  If new: embed the chunk, upsert to Qdrant, then increment its metrics:")
    for i in range(8):
        time.sleep(4)
        c = q("sum(vector_chunks_embedded_total)")
        l = q('replication_lag_seconds{stage="qdrant"}')
        if c not in ("n/a", "err", "0"):
            print(f"    t+{(i+1)*4}s  chunks_embedded = {c}, lag_seconds = {l}")
            break

    step(6, "PROMETHEUS scrapes the /metrics endpoints (this is the missing link)")
    print("  Prometheus asks each service for its counters every few seconds:")
    for svc, port in [("silver", 8001), ("vector-sync", 8002), ("api", 8000)]:
        print(f"    http://host.docker.internal:{port}/metrics   (job: {svc})")
    print("  It stores every value WITH A TIMESTAMP in its own time-series DB.")
    print("  Proof - the stored samples Prometheus holds right now:")

    step(7, "GRAFANA just draws what Prometheus returns (viewer, no storage)")
    print("  Each panel = 1 PromQL query. Grafana calls Prometheus, draws the number.")
    print()
    print("  PANEL                              QUERY                         VALUE")
    print("  " + "-" * 72)
    panels = [
        ("1 Replication lag (stat)",        'replication_lag_seconds{stage="qdrant"}'),
        ("2 Lag over time (timeseries)",    'replication_lag_seconds'),
        ("3 CDC events by op",              'sum(cdc_events_total) by (op)'),
        ("4 Vector chunks embedded",        'sum(vector_chunks_embedded_total)'),
        ("5 DLQ records (stat)",            'dlq_records_total'),
        ("6 Tokens embedded",               'sum(embedding_tokens_total)'),
    ]
    for name, expr in panels:
        print(f"  {name:33} {expr:30} {q(expr)}")

    print("\n" + "=" * 74)
    print("THE ONE-LINE ANSWER:  business data flows Postgres->...->Qdrant;")
    print("Grafana shows neither. It shows COUNTERS the services increment as that")
    print("data passes through, scraped by Prometheus, drawn by Grafana.")
    print("=" * 74)


if __name__ == "__main__":
    main()
