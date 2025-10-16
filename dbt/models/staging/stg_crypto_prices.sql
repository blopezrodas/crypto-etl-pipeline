/*
Staging view for cryptocurrency prices
- stg_crypto_prices → view (cheap to recompute, good for small tables or staging)
- Source table is loaded from S3 into Snowflake via load_snowflake.py
- Purpose: lightweight cleaning and renaming
*/

-- Create a Snowflake view
{{ config(materialized='view') }}

select
    coin_id,
    to_timestamp_ntz(timestamp_ms / 1000) as timestamp_utc,
    price,
    currency,
    extracted_at
from {{ source('crypto', 'crypto_prices_staging') }}

/* 
Notes on dbt source():
- references table that already exsists in database (raw/staging table)

{{ source('<source_name>', '<table_name>') }}
- source_name → the source defined in schema.yml, usually a logical grouping (e.g., 'crypto').
- table_name → the actual table in Snowflake (e.g., 'crypto_prices_staging').
*/