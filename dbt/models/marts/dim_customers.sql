-- =============================================================================
-- DAY 3 - MART: dim_customers (conformed dimension from Silver customers)
-- =============================================================================
-- A "dimension" = descriptive reference entity you JOIN facts to for slicing.
-- Grain: one row per customer. Materialized as table (stable, small).
select
    id           as customer_id,
    name,
    email,
    city,
    updated_at
from {{ source('silver', 'customers') }}
where is_deleted = false
