#!/usr/bin/env python3
"""
DAY 2 - LAKEHOUSE VERIFIER (batch job)
=======================================
1. Bronze row counts (raw events landed)
2. DESCRIBE HISTORY  -> Delta versions (ACID proof)
3. Time travel       -> read an OLD version of Silver (VERSION AS OF)
4. Silver current state + operation-type audit
5. First "dbt-style" analytics SQL on Silver

Run:
  docker exec cdc-spark /opt/spark/bin/spark-submit \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      /jobs/query_lakehouse.py
"""
import sys

sys.path.insert(0, "/jobs")

from pyspark.sql import functions as F

from cdc_common import CDC_TOPICS, bronze_path, build_spark, silver_path, table_short

SEP = "=" * 78


def title(msg):
    print(f"\n{SEP}\n  {msg}\n{SEP}", flush=True)


def main():
    spark = build_spark("lakehouse-query")

    # ---------------------------------------------------------------- 1
    title("1) BRONZE - raw CDC events per table (append-only)")
    for topic in CDC_TOPICS:
        name = table_short(topic)
        try:
            cnt = (spark.read.format("delta").load(bronze_path(topic)).count())
            print(f"  bronze.{name}_cdc : {cnt} events")
        except Exception as e:  # table may not exist on a fresh run
            print(f"  bronze.{name}_cdc : NOT CREATED YET ({type(e).__name__})")

    # ---------------------------------------------------------------- 2
    title("2) DESCRIBE HISTORY silver.contracts (Delta version list)")
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path('inventory.public.contracts')}`")
    hist.select("version", "operation", "operationMetrics.numOutputRows",
                "timestamp").show(20, truncate=False)

    versions = [r.version for r in hist.collect()]
    if len(versions) < 2:
        print("  (need >= 2 versions for a time-travel demo - run more changes first)")

    # ---------------------------------------------------------------- 3
    title("3) TIME TRAVEL - same table, OLD version vs current version")
    cur = spark.read.format("delta").load(silver_path("inventory.public.contracts"))
    if versions:
        older = max(v for v in versions if v < max(versions)) if len(versions) > 1 else min(versions)
        print(f"  AS OF version {older} (the state BEFORE the latest changes):")
        (spark.read.format("delta")
              .option("versionAsOf", older)
              .load(silver_path("inventory.public.contracts"))
              .select("id", "title", "monthly_price", "is_deleted", "cdc_op")
              .orderBy("id").show(20))
        print(f"  CURRENT version {max(versions)}:")
        cur.select("id", "title", "monthly_price", "is_deleted", "cdc_op").orderBy("id").show(20)

    # ---------------------------------------------------------------- 4
    title("4) SILVER - latest state with operation types + CDC audit")
    silver = {t: spark.read.format("delta").load(silver_path(t)) for t in CDC_TOPICS}
    silver["inventory.public.contracts"].select(
        "id", "title", "monthly_price", "status", "cdc_op", "is_deleted",
        "source_lsn",
    ).orderBy("id").show(20, truncate=False)

    print("  Operation mix per Silver table (proves c / u / d ALL flowed):")
    for t, df in silver.items():
        name = table_short(t)
        print(f"\n  silver.{name}:")
        (df.groupBy("cdc_op", "is_deleted").count()
           .orderBy("cdc_op").show(truncate=False))

    print("  Latest state of the OTHER two tables:")
    silver["inventory.public.customers"].select(
        "id", "name", "email", "cdc_op", "is_deleted").orderBy("id").show(truncate=False)
    silver["inventory.public.contract_documents"].select(
        "id", "contract_id", "chunk_text", "cdc_op", "is_deleted").orderBy("id").show(10, truncate=False)

    # ---------------------------------------------------------------- 5
    # Register temp views first so plain SQL can reference the Silver tables.
    for t, df in silver.items():
        df.createOrReplaceTempView(f"silver_{table_short(t)}")

    title("5) ANALYTICS (dbt-style SQL on Silver) - active MRR by status")
    spark.sql("""
        SELECT status,
               COUNT(*)              AS contracts,
               ROUND(SUM(CAST(monthly_price AS DECIMAL(10,2))), 2) AS mrr
        FROM silver_contracts
        WHERE is_deleted = false
        GROUP BY status
        ORDER BY mrr DESC
    """).show(truncate=False)

    # ---------------------------------------------------------------- 6
    title("6) END-TO-END LAG (Postgres commit -> Silver ingest) per event")
    (silver["inventory.public.contracts"]
     .select(
         F.round(
             (F.unix_timestamp("silver_ingested_at") * 1000 - F.col("source_ts_ms")) / 1000.0, 2
         ).alias("lag_seconds"),
     )
     .agg(F.avg("lag_seconds").alias("avg_lag_s"),
          F.max("lag_seconds").alias("max_lag_s"))
     .show())

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
