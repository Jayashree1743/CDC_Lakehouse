#!/usr/bin/env python3
"""DAY 3 verifier: DLQ contents + Silver prices + Gold fact row counts."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import json

s = (SparkSession.builder.appName("day3-verify")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .getOrCreate())

print("=" * 60)
print("DLQ (dead-letter queue)")
print("=" * 60)
p = "/lakehouse/quality.db/dlq"
if DeltaTable.isDeltaTable(s, p):
    df = s.read.format("delta").load(p)
    print(f"rows in DLQ: {df.count()}")
    for r in df.orderBy("failed_at_ms", ascending=False).collect():
        print(f"- table={r.table} id={r.business_id} op={r.cdc_op}")
        for e in json.loads(r.failed_expectations):
            print(f"    FAILED: {e['expectation']} col={e.get('column')} "
                  f"values={e.get('unexpected_values')}")
else:
    print("no DLQ table yet")

print()
print("=" * 60)
print("SILVER contracts (bad price must NOT be here)")
print("=" * 60)
s.read.format("delta").load("/lakehouse/silver.db/contracts") \
    .select("id", "monthly_price", "status", "cdc_op", "is_deleted") \
    .orderBy("id").show(truncate=False)

print("=" * 60)
print("SILVER customers (bad email must NOT be here)")
print("=" * 60)
s.read.format("delta").load("/lakehouse/silver.db/customers") \
    .select("id", "email", "cdc_op", "is_deleted").orderBy("id").show(truncate=False)

print("=" * 60)
print("GOLD fct_contract_changes (changes per hour)")
print("=" * 60)
s.read.format("delta").load("/lakehouse/gold.db/fct_contract_changes") \
    .groupBy("change_hour", "cdc_op").count() \
    .orderBy(F.col("change_hour").desc()).show(20, truncate=False)
