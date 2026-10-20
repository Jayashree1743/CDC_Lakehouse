"""
Shared helpers for the Day 2 lakehouse jobs (Bronze + Silver).

Everything here is plain PySpark - open DAY2_STEPS.md for the concept map.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Paths & settings (all inside the cdc-spark container)
# ---------------------------------------------------------------------------
LAKEHOUSE = os.environ.get("LAKEHOUSE_DIR", "/lakehouse")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
BRONZE_DB = f"{LAKEHOUSE}/bronze.db"
SILVER_DB = f"{LAKEHOUSE}/silver.db"
CHECKPOINT_ROOT = f"{LAKEHOUSE}/_checkpoints"

# The 3 CDC tables we capture (topic = inventory.public.<table>)
CDC_TOPICS = [
    "inventory.public.customers",
    "inventory.public.contracts",
    "inventory.public.contract_documents",
]


def table_short(topic: str) -> str:
    """inventory.public.contracts -> contracts"""
    return topic.rsplit(".", 1)[-1]


def bronze_path(topic: str) -> str:
    return f"{BRONZE_DB}/{table_short(topic)}_cdc"


def silver_path(topic: str) -> str:
    return f"{SILVER_DB}/{table_short(topic)}"


def build_spark(app_name: str) -> SparkSession:
    """SparkSession with Delta Lake wired in (2 config lines = the whole Delta setup)."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Allow Silver MERGE to ADD new columns automatically (schema evolution):
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# CDC event schema (the Day 1 Kafka envelope)
# NOTE: we build StructType objects (NOT DDL strings) - Spark 3.5's from_json
# rejects strings like "STRUCT<...>" and expects a typed schema or JSON schema.
# ---------------------------------------------------------------------------
def envelope_schema(row: StructType) -> StructType:
    """
    The raw Debezium event:
        {before: <row>, after: <row>, source: {...}, op: 'c|u|d', ts_ms: ...}
    """
    return StructType([
        StructField("before", row),     # null on INSERT
        StructField("after", row),      # null on DELETE
        StructField("source", StructType([
            StructField("ts_ms", LongType()),    # Postgres commit time (lag metric!)
            StructField("ts_us", LongType()),
            StructField("lsn", LongType()),      # WAL position
            StructField("txId", LongType()),
            StructField("table", StringType()),
            StructField("db", StringType()),
        ])),
        StructField("op", StringType()),         # c / u / d
        StructField("ts_ms", LongType()),        # Kafka publish time
    ])


# Row schemas mirror postgres/init.sql (numeric -> string: decimal.handling.mode=string)
ROW_SCHEMAS = {
    "inventory.public.customers": StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]),
    "inventory.public.contracts": StructType([
        StructField("id", LongType()),
        StructField("customer_id", LongType()),
        StructField("title", StringType()),
        StructField("monthly_price", StringType()),   # numeric came as string
        StructField("status", StringType()),
        StructField("updated_at", StringType()),
        StructField("region", StringType()),          # added Day 2: schema evolution demo
    ]),
    "inventory.public.contract_documents": StructType([
        StructField("id", LongType()),
        StructField("contract_id", LongType()),
        StructField("chunk_text", StringType()),
        StructField("content_hash", StringType()),
        StructField("updated_at", StringType()),
    ]),
}
