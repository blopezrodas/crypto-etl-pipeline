/*
Fact table for cryptocurrency prices
- fct_crypto_prices → table (pre-calculated metrics, ready for BI)
- physical table good for fact tables, aggregates, or large tables you query frequently
*/

-- Create a physical table
{{ config(materialized='table') }}

with staged as (
    select *
    from {{ ref('stg_crypto_prices') }}
),

calculations as (
    select
        coin_id,
        timestamp_utc,
        price,
        currency,
        extracted_at,
        price - lag(price) over (partition by coin_id order by timestamp_utc) as return_1d,
        avg(price) over (partition by coin_id order by timestamp_utc rows between 6 preceding and current row) as sma_7d
    from staged
)

select *
from calculations


/* 
Notes on dbt ref():
- references another dbt model (like the staging view)
*/