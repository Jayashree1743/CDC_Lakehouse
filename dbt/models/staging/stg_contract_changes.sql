-- =============================================================================
-- DAY 3 - STAGING: stg_contract_changes (Bronze -> one row PER change event)
-- =============================================================================
-- Silver only holds the LATEST state of each contract, so it cannot answer
-- "how many changes per hour?". The change HISTORY lives in Bronze's append-only
-- CDC log. We parse the raw Debezium envelope here into typed columns.
--
-- get_json_object() pulls fields out of the raw kafka_value without needing a
-- declared schema (Spark parses on read) -> tolerant to source schema drift.
--
-- TWO LESSONS THAT COST US A BROKEN BUILD (see NOTES.md):
--   1. A DELETE event has after = NULL. Reading only $.after.* made every
--      deleted contract NULL -> not_null + relationships tests failed.
--      REPLICA IDENTITY FULL guarantees the BEFORE image on deletes, so we
--      coalesce($.after.x, $.before.x) -- correct for c/u/d alike.
--   2. change_uid used to be (kafka_partition, kafka_offset). A connector
--      restart can REUSE low offsets (the exact bug Silver fixed via LSN
--      dedup) -> two different events shared one "unique" id.
--      Now: (business key, Postgres LSN) -- the LSN is a monotonic WAL
--      position, so the same change always maps to the same id, and two
--      different changes never collide.
with parsed as (
    select
        kafka_partition,
        kafka_offset,
        get_json_object(kafka_value, '$.op')            as cdc_op,      -- c/u/d/r
        cast(get_json_object(kafka_value, '$.source.ts_ms') as bigint)
                                                        as source_ts_ms,
        cast(get_json_object(kafka_value, '$.source.lsn') as bigint)
                                                        as source_lsn,  -- monotonic WAL pos
        cast(cast(get_json_object(kafka_value, '$.source.ts_ms') as double) / 1000
             as timestamp)                              as source_commit_ts,
        cast(coalesce(get_json_object(kafka_value, '$.after.id'),
                      get_json_object(kafka_value, '$.before.id')) as bigint)
                                                        as contract_id,
        cast(coalesce(get_json_object(kafka_value, '$.after.customer_id'),
                      get_json_object(kafka_value, '$.before.customer_id')) as bigint)
                                                        as customer_id,
        coalesce(get_json_object(kafka_value, '$.after.title'),
                 get_json_object(kafka_value, '$.before.title'))
                                                        as title,
        coalesce(get_json_object(kafka_value, '$.after.monthly_price'),
                 get_json_object(kafka_value, '$.before.monthly_price'))
                                                        as monthly_price_raw,
        coalesce(get_json_object(kafka_value, '$.after.status'),
                 get_json_object(kafka_value, '$.before.status'))
                                                        as status
    from {{ source('bronze', 'contracts_cdc') }}
    where get_json_object(kafka_value, '$.op') is not null   -- drop tombstones
),
keyed as (
    select
        -- unique identity of this change event (dbt incremental dedup/merge key)
        concat(contract_id, '-', source_lsn)             as change_uid,
        parsed.*
    from parsed
),
deduped as (
    -- at-least-once delivery can replay the SAME change at a different kafka
    -- offset; (business key, lsn) makes both copies identical, so keep one.
    select
        *,
        row_number() over (partition by change_uid
                           order by kafka_offset desc)   as rn
    from keyed
)
select
    change_uid,
    kafka_partition,
    kafka_offset,
    cdc_op,
    source_ts_ms,
    source_lsn,
    source_commit_ts,
    contract_id,
    customer_id,
    title,
    monthly_price_raw,
    status
from deduped
where rn = 1
