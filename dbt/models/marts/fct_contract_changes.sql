-- =============================================================================
-- DAY 3 - MART: fct_contract_changes  (INCREMENTAL - the key concept for Day 4)
-- =============================================================================
-- Grain: ONE row per contract change event (the fact).
-- Question it answers: "How many contract changes per hour today?"
--
-- MATERIALIZATION = incremental:
--   * First run  -> full build (is_incremental() is false).
--   * Every run  -> dbt only SELECTs source rows NEWER than what is already in
--                   {{ this }} (the existing table), then MERGEs them in.
--   * This mirrors EXACTLY the Day-4 vector strategy: process only what changed
--     since the watermark instead of re-doing the whole corpus.
--
-- unique_key = change_uid -> the MERGE upserts on it, so re-running the same
--   batch is idempotent (at-least-once safe, same as our Silver layer).

{{ config(
    materialized = 'incremental',
    unique_key   = 'change_uid',
    file_format  = 'delta',
    incremental_strategy = 'merge'
) }}

with changes as (
    select *
    from {{ ref('stg_contract_changes') }}

    {% if is_incremental() %}
    -- WATERMARK: only events committed at or after the latest one we already
    -- have. >= (not >) because two Postgres commits can share a millisecond;
    -- reprocessing the boundary row is harmless since unique_key=change_uid
    -- makes this MERGE idempotent.
    -- source_commit_ts comes from Postgres (source.ts_ms), NOT local clock,
    -- so late-arriving events and clock skew cannot drop rows.
    where source_commit_ts >= (
        select coalesce(max(source_commit_ts), timestamp'1970-01-01')
        from {{ this }}
    )
    {% endif %}
)

select
    change_uid,
    contract_id,
    customer_id,
    title,
    cast(monthly_price_raw as decimal(10,2)) as monthly_price,
    status,
    cdc_op,
    source_ts_ms,
    source_commit_ts,
    -- bucket for the "changes per hour" question
    date_trunc('hour', source_commit_ts) as change_hour
from changes
