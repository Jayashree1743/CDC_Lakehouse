"""
DAY 3 - QUALITY GATES (Great Expectations + DLQ)
=================================================
"Trust" layer between CDC ingestion and the lakehouse:

    CDC event  ->  EXPECTATION SUITE  --pass-->  Silver MERGE (Gold stays clean)
                             |
                             +--fail-->  DLQ (Delta table + Kafka topic)
                                         with the exact reason per row

Interview vocabulary (memorize):
  * Expectation Suite  - a named list of declarative data rules (below: SUITES).
  * Validation Result  - what you get after running a suite against one batch:
                         which expectations passed, which rows broke them.
  * Circuit breaker    - a failing row does NOT flow downstream; it is parked.
  * DLQ                - dead-letter queue: bad rows kept WITH their error
                         context so a human can fix the source and REPLAY them
                         instead of silently losing data or corrupting Gold.

Design notes:
  * We validate the micro-batch AFTER dedup/latest-per-key, BEFORE MERGE.
  * Batch -> pandas: lab volumes are tiny; in production you would use GE's
    SparkDF integration so validation stays distributed.
  * result_format=COMPLETE returns unexpected_index_list = exact failing rows.
"""
import json
import time

import pandas as pd
from pyspark.sql import functions as F

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset import PandasDataset

from cdc_common import LAKEHOUSE

# ---------------------------------------------------------------------------
# THE SUITE (build task 1): declarative rules per source table.
# Each entry = (expectation_type, kwargs) -> GE expectation on a pandas batch.
# ---------------------------------------------------------------------------
SUITES = {
    "contracts": "silver_contracts_suite",
    "customers": "silver_customers_suite",
    "contract_documents": "silver_documents_suite",
}

def expectations_for(short_table: str):
    if short_table == "contracts":
        return [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id"}),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "customer_id"}),
            # monthly_price must be PRESENT after numeric coercion. GE's `between`
            # ignores NULLs by default, so without this a corrupt base64 price
            # (e.g. 'AYY8' from Day 1) coerces to NaN and slips through.
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "monthly_price"}),
            # the money rule: price must be a sane positive number < 1M
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "monthly_price", "min_value": 0.01,
                        "max_value": 1000000}),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "status", "value_set": ["active", "cancelled"]}),
            # clock-skew guard: source commit time must not be in the future
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "source_ts_ms", "min_value": 0,
                        "max_value": int(time.time() * 1000) + 3_600_000}),
        ]
    if short_table == "customers":
        return [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id"}),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "email"}),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={"column": "email",
                        "regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}),
        ]
    if short_table == "contract_documents":
        return [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id"}),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "chunk_text"}),
            # every chunk must carry its content hash (Day 4 skip-if-unchanged)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "content_hash"}),
        ]
    return []


def validate_batch(batch_df, short_table):
    """
    Run the suite against one micro-batch.

    Returns (passing_df, rejected_rows):
      passing_df  - Spark DF of rows allowed to MERGE into Silver
      rejected_rows - list of dicts, one per failing row, each carrying the
                      full record + the list of broken expectations (DLQ payload)
    """
    pdf = batch_df.toPandas().reset_index(drop=True)
    if pdf.empty:
        return batch_df.limit(0), []

    expectations = expectations_for(short_table)
    if not expectations:                # unknown table -> no gate (fail open)
        return batch_df, []

    # GE's validate() wants an ExpectationSuite object (a named collection of
    # ExpectationConfigurations) - that's the "Expectation Suite" from the plan.
    suite = ExpectationSuite(
        expectation_suite_name=f"silver_{short_table}_suite",
        expectations=expectations,
    )

    # --- validation-only type coercion -------------------------------------
    # Silver stores monthly_price as a STRING (decimal.handling.mode=string keeps
    # money exact/readable through Kafka). GE's `between` needs a numeric column,
    # otherwise it raises TypeError instead of flagging rows. So we cast in a
    # COPY used purely for validation -> the row that gets merged into Silver is
    # untouched. Bonus: a corrupt base64 price like 'AYY8' coerces to NaN and is
    # then caught by the not_null expectation below (real bug we hit on Day 1).
    for col in ("monthly_price",):
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    ds = PandasDataset(pdf)
    result = ds.validate(
        expectation_suite=suite,        # NOTE: kwarg is `expectation_suite`, not `expectations`
        result_format={"result_format": "COMPLETE"},   # full unexpected_index_list
        catch_exceptions=True,
    )

    # map: row index -> [reason, ...]
    failures = {}
    for r in result.results:
        if r.success:
            continue
        exp = r.expectation_config
        unexpected = r.result.get("unexpected_index_list") or []
        unexpected_values = r.result.get("unexpected_list") or []
        reason = {
            "expectation": exp.expectation_type,
            "column": exp.kwargs.get("column"),
            "unexpected_values": [str(v) for v in unexpected_values[:10]],
            "observed": {k: str(v) for k, v in r.result.items()
                         if k in ("unexpected_count", "observed_value")},
        }
        for idx in unexpected:
            failures.setdefault(int(idx), []).append(reason)
        if not unexpected:             # e.g. observed_value mismatch on no rows
            failures.setdefault("ALL", []).append(reason)

    if not failures:
        return batch_df, []

    # ALL-rows failure (e.g. a suite-level observed_value mismatch): reject everything.
    if "ALL" in failures:
        bad_idx = set(range(len(pdf)))
    else:
        bad_idx = set(int(i) for i in failures)

    # toPandas() preserves Spark row order, so positional alignment is safe here.
    rows = batch_df.collect()
    good_rows = [r for i, r in enumerate(rows) if i not in bad_idx]
    bad_entries = [(i, r) for i, r in enumerate(rows) if i in bad_idx]
    passing_df = batch_df.sparkSession.createDataFrame(good_rows, schema=batch_df.schema)

    rejected = []
    for i, r in bad_entries:
        as_dict = r.asDict(recursive=True)
        rejected.append({
            "table": short_table,
            "business_id": as_dict.get("id"),
            "cdc_op": as_dict.get("cdc_op"),
            "kafka_partition": as_dict.get("kafka_partition"),
            "kafka_offset": as_dict.get("kafka_offset"),
            "source_ts_ms": as_dict.get("source_ts_ms"),
            "failed_expectations": failures.get(i, failures.get("ALL", [])),
            "record": {k: str(v) for k, v in as_dict.items()},
            "failed_at_ms": int(time.time() * 1000),
        })
    return passing_df, rejected


def dlq_schema():
    """Single source of truth for the DLQ table schema (used by writer AND pre-create)."""
    from pyspark.sql import types as T

    return T.StructType([
        T.StructField("table", T.StringType()),
        T.StructField("business_id", T.LongType()),
        T.StructField("cdc_op", T.StringType()),
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()),
        T.StructField("source_ts_ms", T.LongType()),
        T.StructField("failed_expectations", T.StringType()),   # JSON array
        T.StructField("record", T.StringType()),                # JSON object
        T.StructField("failed_at_ms", T.LongType()),
        T.StructField("replayed_at_ms", T.LongType()),          # NULL until replayed
    ])


def ensure_dlq_table(spark):
    """
    Create the DLQ Delta table ONCE at job startup, before any stream starts.

    WHY THIS EXISTS (real bug hit on Day 3): all 3 Silver streams share one DLQ
    table. If two micro-batches both fail validation in the same tick, they race to
    CREATE the table -> Delta throws DELTA_PROTOCOL_CHANGED ("multiple writers
    writing to an empty directory") and KILLS the streaming query. Delta's own
    error message gives the fix: create the table ahead of time. After this, the
    streams only ever do blind appends, which Delta's optimistic concurrency
    handles safely.
    """
    from delta.tables import DeltaTable

    path = f"{LAKEHOUSE}/quality.db/dlq"
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], dlq_schema()).write.format("delta").save(path)
        print(f"[quality] pre-created DLQ table at {path}", flush=True)
    return path


def dlq_dataframe(spark, rejected):
    """Rejected rows -> Spark DF matching the DLQ Delta schema (created by writer)."""
    schema = dlq_schema()
    data = [
        (r["table"], r["business_id"], r["cdc_op"], r["kafka_partition"],
         r["kafka_offset"], r["source_ts_ms"],
         json.dumps(r["failed_expectations"]), json.dumps(r["record"]),
         r["failed_at_ms"], None)
        for r in rejected
    ]
    return spark.createDataFrame(data, schema)


def write_dlq(spark, rejected, short_table, kafka_bootstrap):
    """
    CIRCUIT-BREAKER side effects for a failing batch (Day 3 build task 2):
      1. Append the rejected rows to the DLQ Delta table  (durable, queryable)
      2. Publish them to a DLQ Kafka topic                (so a repair worker can replay)
      3. Increment the dlq_records_total Prometheus metric (so Grafana can alert)

    Returns the number of rows parked.
    """
    if not rejected:
        return 0

    from metrics import DLQ_RECORDS_TOTAL

    dlq_df = dlq_dataframe(spark, rejected)
    dlq_path = f"{LAKEHOUSE}/quality.db/dlq"

    # --- 1. durable copy in the lakehouse -----------------------------------
    (dlq_df.write.format("delta").mode("append").save(dlq_path))
    print(f"[dlq] parked {len(rejected)} {short_table} row(s) -> {dlq_path}", flush=True)

    # --- 2. side channel on Kafka (repair-and-replay) ------------------------
    # Each rejected row becomes one JSON message on inventory.dlq.<table>.
    # The Kafka connector jar must be on the --packages list for this job.
    try:
        (dlq_df
         .select(F.expr("to_json(struct(*))").alias("value"))
         .write.format("kafka")
         .option("kafka.bootstrap.servers", kafka_bootstrap)
         .option("topic", f"inventory.dlq.{short_table}")
         .mode("append")
         .save())
        print(f"[dlq] published {len(rejected)} message(s) to inventory.dlq.{short_table}",
              flush=True)
    except Exception as e:                       # never fail the whole batch on DLQ publish
        print(f"[dlq] Kafka publish failed (rows are still in Delta): {e}", flush=True)

    # --- 3. metric -----------------------------------------------------------
    DLQ_RECORDS_TOTAL.inc(len(rejected))
    return len(rejected)
