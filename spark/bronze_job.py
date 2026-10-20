#!/usr/bin/env python3
"""
DAY 2 - BRONZE JOB (Medallion layer 1: RAW)
=============================================
Kafka CDC events  ->  Delta tables lakehouse/bronze.db/<table>_cdc

Bronze rules (memorize for interviews):
  * APPEND-ONLY. Never update, never delete, never "fix".
  * Keep the raw Kafka value byte-for-byte + ALL metadata needed to replay:
    topic, partition, offset, kafka timestamp, ingest time, source batch id.
  * Partitioned by ingest_date (cheap pruning for "what landed yesterday").
  * Idempotent writes via txnAppId/txnVersion -> a replayed batch can never
    duplicate rows (exactly-once effect on the table, even without a checkpoint).

Run:
  docker exec cdc-spark /opt/spark/bin/spark-submit \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      /jobs/bronze_job.py
"""
import sys

sys.path.insert(0, "/jobs")

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

from cdc_common import (
    CDC_TOPICS, KAFKA_BOOTSTRAP, bronze_path, build_spark, table_short,
)

CHECKPOINT = "/lakehouse/_checkpoints/bronze"
TRIGGER = "10 seconds"          # micro-batch every 10s (use "availableNow" for prod-ish)


def write_bronze_batch(batch_df, batch_id):
    """One micro-batch -> split by topic -> append to that table's Delta file."""
    # persist(): the batch is read ONCE, then written once per topic (3 writes).
    batch_df.persist()
    try:
        topics = [r.topic for r in batch_df.select("topic").distinct().collect()]
        if not topics:
            print(f"[bronze batch {batch_id}] empty", flush=True)
            return
        for topic in topics:
            subset = batch_df.filter(F.col("topic") == topic)
            (subset.write
                  .format("delta")
                  .mode("append")
                  .option("mergeSchema", "true")       # keep any new source columns
                  .option("txnAppId", f"bronze-{table_short(topic)}")  # IDEMPOTENCY KEY
                  .option("txnVersion", batch_id)      # batch id = transaction version
                  .partitionBy("ingest_date")
                  .save(bronze_path(topic)))
            print(f"[bronze batch {batch_id}] wrote {subset.count()} rows -> "
                  f"bronze.{table_short(topic)}_cdc", flush=True)
    finally:
        batch_df.unpersist()


def main():
    spark = build_spark("cdc-bronze")

    # ---- readStream: unbounded DataFrame over ALL CDC topics -------------
    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(CDC_TOPICS))
        .option("startingOffsets", "earliest")   # drain yesterday's backlog on first run
        .option("failOnDataLoss", "false")       # lab tolerance (learn what it means!)
        .load()
    )

    # ---- bronze projection: raw bytes + replay metadata (NO JSON parsing!) -
    bronze = (
        events
        .withColumn("kafka_key", F.col("key").cast("string"))
        .withColumn("kafka_value", F.col("value").cast("string"))  # raw Debezium JSON
        .withColumn("kafka_timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("event_time", F.current_timestamp())
        .select(
            "topic", "partition", "offset",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            "kafka_key", "kafka_value", "kafka_timestamp", "event_time",
            F.current_timestamp().alias("ingest_ts"),
            F.current_date().alias("ingest_date").cast(DateType()),
        )
    )

    query = (
        bronze.writeStream
        .queryName("bronze-ingest")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .foreachBatch(write_bronze_batch)
        .start()
    )
    print(f"[bronze] streaming started: {query.id}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
