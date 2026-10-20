-- =============================================================================
-- DAY 3 - STAGING: stg_contracts (Silver -> cleaned, 1:1)
-- =============================================================================
-- `ref()`/`source()` build the DAG: dbt sees this depends on silver.contracts
-- and orders the run accordingly. Materialized as a VIEW (see dbt_project.yml),
-- so it is always the freshest Silver data with zero extra storage.
--
-- Cleaning: monthly_price arrives as a STRING (we run the connector with
-- decimal.handling.mode=string to keep money exact/readable). Cast it here,
-- once, so every downstream model can trust the type.
--
-- SOFT-DELETED ROWS ARE KEPT (is_deleted flag exposed) -- this was bug 2.2c:
-- we used to filter is_deleted=false here, but fct_contract_changes keeps
-- HISTORY rows for contracts that were later deleted (correct for an event
-- fact), so the relationships test saw contract_id=2 in Gold but not in this
-- model -> test failure on correct data. A dimension/current-state model
-- should expose the delete flag and let the CONSUMER decide.
select
    id                      as contract_id,
    customer_id,
    title,
    cast(monthly_price as decimal(10,2)) as monthly_price,
    status,
    region,
    is_deleted,
    source_commit_ts,
    updated_at
from {{ source('silver', 'contracts') }}
