# Spark image with Day 3 tools baked in:
#   great_expectations  -> data quality suite + validation results
#   dbt-core + dbt-spark -> Silver -> Gold SQL models (method: session, uses THIS container's Spark)
#
# Build happens automatically via: docker compose -p cdc-lakehouse up -d --build
FROM apache/spark:3.5.3

USER root

# Python 3.8 in this image -> all pins must still support 3.8.
# `python3 -m pip` (NOT bare `pip`): upgrading pip via bare `pip` installs the new
# pip to /usr/local but the NEXT bare `pip` still resolves to the OLD /usr/bin/pip20
# -> it crashes with `InvalidVersion: '2004d'` on modern wheel metadata. `python3 -m
# pip` always uses the module we just upgraded.
#
# PINS / WORKAROUNDS (see the troubleshooting section in README.md):
#   notebook<7        -> GE 0.18 depends on notebook, which drags in jupyterlab>=4.4.9
#                        (Python 3.9+ only). Pin to the last 3.8-compatible notebook.
#   dbt-spark (plain) -> the [session] extra pip-installs the ~300MB pyspark wheel.
#                        We already SHIP pyspark inside this image, so install it
#                        bare and expose it through PYTHONPATH below instead.
#   protobuf<5        -> dbt-core 1.7 calls MessageToJson(including_default_value_fields=)
#                        which protobuf 5.29 REMOVED -> TypeError on every `dbt` command.
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir \
      "great_expectations==0.18.19" \
      "notebook<7" \
      "dbt-core==1.7.1" \
      "dbt-spark==1.7.1" \
      "prometheus-client==0.20.0" && \
    # protobuf pin MUST be its own final step: when it shares a command with
    # the packages above, pip's resolver silently upgrades it to 5.x to satisfy
    # THEIR dependencies -> dbt crashes with "MessageToJson() got an unexpected
    # keyword argument 'including_default_value_fields'" (removed in protobuf 5).
    python3 -m pip install --no-cache-dir --force-reinstall "protobuf<5"

# dbt profile (session connection -> local SparkSession inside this container)
RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml

# Spark config picked up by BOTH spark-submit and dbt's own pyspark session:
#  - Delta SQL extension + catalog
#  - Delta jars (they live in the mounted ivy cache, present at runtime)
#  - warehouse dir for dbt Gold output
RUN mkdir -p /opt/spark/conf && printf '%s\n' \
      'spark.sql.extensions                 io.delta.sql.DeltaSparkSessionExtension' \
      'spark.sql.catalog.spark_catalog      org.apache.spark.sql.delta.catalog.DeltaCatalog' \
      'spark.jars                           /root/.ivy2/jars/io.delta_delta-spark_2.12-3.2.0.jar,/root/.ivy2/jars/io.delta_delta-storage-3.2.0.jar,/root/.ivy2/jars/org.antlr_antlr4-runtime-4.9.3.jar' \
      'spark.sql.warehouse.dir              /lakehouse' \
      'spark.driver.memory                  1g' \
      > /opt/spark/conf/spark-defaults.conf

ENV SPARK_CONF_DIR=/opt/spark/conf

# dbt-spark "session" method does `import pyspark` -> reuse the pyspark that ships
# with this image (no 300MB pip download). py4j must be on the path too.
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
