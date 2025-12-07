{{ config(
    materialized='incremental',
    unique_key='trading_date || \'-\' || ticker',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        DATE                AS trading_date,
        OPEN                AS open_price,
        HIGH                AS high_price,
        LOW                 AS low_price,
        CLOSE               AS close_price,
        ADJUSTED_CLOSE      AS adjusted_close_price,
        VOLUME              AS volume,
        DIVIDEND_AMOUNT     AS dividend_amount,
        SPLIT_COEFFICIENT   AS split_coefficient,
        TICKER              AS ticker,
        LOAD_TIME
    FROM {{ source('stock_data', 'stock_price_data_raw') }}

),

-- ❗ FIX: Snowflake requires aggregates to occur in SELECT/HAVING, not WHERE.
-- So we separate MAX(load_time) into its own CTE.
most_recent AS (
    SELECT MAX(load_time) AS max_load_time
    FROM raw
),

filtered AS (
    SELECT r.*
    FROM raw r
    JOIN most_recent m
        ON r.load_time = m.max_load_time
)

SELECT *
FROM filtered

{% if is_incremental() %}
-- Only process records newer than the max existing trading_date
WHERE trading_date > (
        SELECT COALESCE(MAX(trading_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}


