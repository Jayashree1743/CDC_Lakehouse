# =============================================================================
# DAY 3 - dbt MACROS
# =============================================================================
# Our Silver/Bronze tables are Delta files at /lakehouse/<db>/<table> written by
# Spark. They are NOT registered in any metastore, so dbt's source()/ref() can't
# see them yet. This hook runs BEFORE each `dbt build` and registers them as
# EXTERNAL tables pointing at those exact paths -> lineage just works.
#
# on-run-start = dbt lifecycle hook: runs once at the start of `dbt build/run`.

{% macro create_external_sources() %}
    {#- schemas live under spark.sql.warehouse.dir = /lakehouse -#}
    create schema if not exists silver;
    create schema if not exists bronze;

    {#- register Silver Delta tables as external tables at their real paths -#}
    create table if not exists silver.contracts
        using delta location '/lakehouse/silver.db/contracts';
    create table if not exists silver.customers
        using delta location '/lakehouse/silver.db/customers';
    create table if not exists silver.contract_documents
        using delta location '/lakehouse/silver.db/contract_documents';

    {#- register Bronze append-only CDC log -#}
    create table if not exists bronze.contracts_cdc
        using delta location '/lakehouse/bronze.db/contracts_cdc';
{% endmacro %}
