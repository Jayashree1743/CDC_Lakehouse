#!/usr/bin/env python3
"""
DAY 2 - SILVER JOB (Medallion layer 2: CLEANSED CURRENT STATE)
================================================================
Bronze (raw Delta)  ->  parse Debezium JSON  ->  MERGE (upsert)  ->  Silver

Silver rules (memorize for interviews):
  * One CURRENT row per business key (id) - "latest state" table.
  * MERGE semantics:  op = c/u -> upsert the AFTER image
                      op = d   -> soft-delete (is_deleted = true) using the
                                  BEFORE image (works because REPLICA IDENTITY FULL)
  * At-least-once safety: dedupe (kafka_partition, kafka_offset) + MERGE is an
    upsert -> reprocessing the same batch changes nothing (idempotent).
  * Schema evolution ON: a new source column shows up in Silver next run.

Run:
  docker exec cdc-spark /opt/spark/bin/spark-submit \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      /jobs/silver_job.py
"""
import sys

sys.path.insert(0, "/jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from cdc_common import (
    CDC_TOPICS, ROW_SCHEMAS, bronze_path, build_spark, envelope_schema,
    silver_path, table_short, KAFKA_BOOTSTRAP,
)
from delta.tables import DeltaTable
from quality import validate_batch, write_dlq, ensure_dlq_table
from metrics import start_metrics_server, CDC_EVENTS_TOTAL


def transform(bronze_df, topic):
    """Parse the raw CDC JSON into one flattened row per event."""
    envelope = F.from_json(F.col("kafka_value"), envelope_schema(ROW_SCHEMAS[topic]))
    row_img = F.coalesce(envelope.getField("after"), envelope.getField("before"))

    return (
        bronze_df
        # DROP TOMBSTONES: after a DELETE, Kafka carries a second message with a
        # NULL value ("this key is gone"). Bronze keeps it raw; Silver must NOT
        # parse it or every column becomes NULL. (Lesson learned the hard way.)
        .filter(F.col("kafka_value").isNotNull())
        .withColumn("evt", envelope)
        .withColumn("row", row_img)
        # Materialize the dedup key as a REAL column first. (dropDuplicates in
        # this Spark build takes column-NAME strings, not Column expressions --
        # passing F.col(...).alias(...) crashes with "Column is not iterable".)
        .withColumn("_lsn", F.col("evt.source.lsn").cast("string"))
        # AT-LEAST-ONCE DEDUP -- key on (LSN, kafka_key), NOT the Kafka offset.
        # Why not offset: a Debezium-connector/topic restart can REUSE low Kafka
        # offsets, and then offset-dedup silently DISCARDS new data that collides
        # with old offsets (we watched live updates vanish as "duplicates").
        # Why LSN + key: the Postgres LSN is a monotonic WAL position, so the same
        # change always replays to the same LSN (safe across restarts). But every
        # row in an initial SNAPSHOT shares one LSN, so we add kafka_key (the PK)
        # to keep distinct snapshot rows from collapsing into one.
        .dropDuplicates(["_lsn", "kafka_key"])
        .select(
            F.col("row.*"),                                  # business columns
            F.col("evt.op").alias("cdc_op"),                 # c / u / d
            (F.col("evt.op") == "d").alias("is_deleted"),
            F.col("evt.source.ts_ms").alias("source_ts_ms"),
            F.col("evt.source.lsn").alias("source_lsn"),
            F.col("evt.source.txId").alias("source_tx_id"),
            (F.col("evt.source.ts_ms") / 1000).cast("timestamp").alias("source_commit_ts"),
            "kafka_partition", "kafka_offset",
            F.col("topic").alias("source_topic"),
            "ingest_date",
            F.current_timestamp().alias("silver_ingested_at"),
        )
    )


def upsert_silver(spark, batch_df, batch_id, topic):
    """MERGE micro-batch into the Silver table (SCD Type-1 style upsert)."""
    name = table_short(topic)
    path = silver_path(topic)

    # Bootstrap on FIRST run: Delta MERGE requires an existing table, so create
    # it empty (with the batch's schema) before merging. Runs once ever.
    if not DeltaTable.isDeltaTable(spark, path):
        (batch_df.limit(0)                       # schema only, no rows
         .write.format("delta").save(path))
        print(f"[silver batch {batch_id}] created silver.{name}", flush=True)

    # Within ONE batch a key can appear many times (insert+update+delete...).
    # Keep only its LATEST event, otherwise Delta MERGE errors:
    # "multiple source rows matched the same target row".
    latest = Window.partitionBy("id").orderBy(
        F.col("source_ts_ms").desc_nulls_last(),
        # Tie-break on the Postgres LSN (monotonic WAL position), NOT the
        # Kafka offset: a connector restart can REUSE offsets (the exact bug
        # this project hit and fixed in dedup), so the offset is not a
        # trustworthy "which is newest" signal.
        F.col("source_lsn").desc_nulls_last(),
    )
    latest_per_key = batch_df.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")

    # ---------------- DAY 3 QUALITY GATE (circuit breaker) ------------------
    # Great Expectations suite runs HERE: rows that break a rule are pulled OUT
    # of the MERGE and parked in the DLQ (Delta + Kafka) with their reason.
    # Result: bad data never reaches Silver -> never reaches Gold.
    validated_df, rejected = validate_batch(latest_per_key, name)
    n_bad = len(rejected)
    if n_bad:
        write_dlq(spark, rejected, name, KAFKA_BOOTSTRAP)
    n_good = validated_df.count()
    print(f"[quality {batch_id}] {name}: {n_good} passed, {n_bad} parked in DLQ", flush=True)
    if n_good == 0:
        print(f"[quality {batch_id}] {name}: all rows rejected -> MERGE skipped", flush=True)
        return

    # count processed CDC events by op (metric for Day 6 dashboard)
    for row in validated_df.groupBy("cdc_op").count().collect():
        CDC_EVENTS_TOTAL.labels(op=row["cdc_op"]).inc(row["count"])

    target = DeltaTable.forPath(spark, path)

    # is_deleted is already computed in the transform ((op == "d")), so ONE
    # unconditional update branch is correct: it carries the soft-delete flag
    # for 'd' events AND the fresh image for 'c'/'u' events. (An earlier
    # `whenMatchedUpdateAll(condition="s.cdc_op='d'")` branch was dead code --
    # it did exactly the same thing as the unconditional branch below.)
    (target.alias("t")
          .merge(validated_df.alias("s"), "t.id = s.id")
          .whenMatchedUpdateAll()                            # c/u/d: new image
          .whenNotMatchedInsertAll()                          # brand new key
          .execute())

    print(f"[silver batch {batch_id}] merged into silver.{name}", flush=True)


def main():
    spark = build_spark("cdc-silver")

    # Create the shared DLQ table ONCE before any stream starts. Without this the
    # 3 streams race to create it and Delta kills them with DELTA_PROTOCOL_CHANGED.
    ensure_dlq_table(spark)

    # expose Prometheus metrics on :8001/metrics (Day 6)
    from metrics import start_metrics_server
    start_metrics_server()

    queries = []
    for topic in CDC_TOPICS:
        source = (
            spark.readStream
            .format("delta")
            .option("ignoreChanges", "true")   # bronze is append-only; tolerate maintenance
            .load(bronze_path(topic))
        )
        clean = transform(source, topic)

        q = (
            clean.writeStream
            .queryName(f"silver-{table_short(topic)}")
            .option("checkpointLocation", f"/lakehouse/_checkpoints/silver_{table_short(topic)}")
            .trigger(processingTime="10 seconds")
            .foreachBatch(lambda df, bid, t=topic: upsert_silver(spark, df, bid, t))
            .start()
        )
        queries.append(q)

    print(f"[silver] started {len(queries)} streams", flush=True)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
