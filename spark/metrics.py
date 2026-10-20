"""
DAY 6 METRICS (built early on Day 3 so the DLQ can increment a counter)
========================================================================
Prometheus client helpers shared by the Silver streaming job.

Exposition format (what Prometheus scrapes from :8001/metrics):

    # TYPE cdc_events_total counter
    cdc_events_total{op="u"} 142          <- COUNTER: monotonic, only goes up
    # TYPE replication_lag_seconds gauge
    replication_lag_seconds{stage="silver"} 1.8   <- GAUGE: point in time, up AND down
    # TYPE dlq_records_total counter
    dlq_records_total 2

Why start_http_server inside the Spark DRIVER: the driver is a long-lived
Python process, so it can serve /metrics while the streams run. In production
you would rather use a Pushgateway or OpenTelemetry, but the data model
(counters/gauges/labels) is identical.
"""
import os
import threading

from prometheus_client import Counter, Gauge, start_http_server

# --- metric definitions (Day 6 build task 1) --------------------------------
CDC_EVENTS_TOTAL = Counter(
    "cdc_events_total", "CDC events processed by Silver", labelnames=["op"])

DLQ_RECORDS_TOTAL = Counter(
    "dlq_records_total", "Records rejected by quality gates and parked in the DLQ")

REPLICATION_LAG_SECONDS = Gauge(
    "replication_lag_seconds",
    "Seconds between the Postgres commit time (source.ts_ms) and downstream availability",
    labelnames=["stage"])     # stage = silver | gold | qdrant

CHUNKS_SYNCED_TOTAL = Counter(
    "chunks_synced_total", "Vector chunks upserted/deleted in Qdrant (Day 4 worker)")

EMBEDDING_TOKENS_TOTAL = Counter(
    "embedding_tokens_total", "Estimated embedding tokens spent (Day 4 worker)")

_started = False
_lock = threading.Lock()


def start_metrics_server(port: int = None):
    """Idempotent: start the /metrics HTTP server once per process."""
    global _started
    with _lock:
        if _started:
            return
        port = int(port or os.environ.get("METRICS_PORT", "8001"))
        try:
            start_http_server(port)
            _started = True
            print(f"[metrics] serving http://0.0.0.0:{port}/metrics", flush=True)
        except Exception as e:                      # never kill the job for metrics
            print(f"[metrics] server not started: {e}", flush=True)
