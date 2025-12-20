{{ config(materialized='table') }}

select
    p.TICKER,
    p.TRADING_DATE,
    p.OPEN_PRICE,
    p.HIGH_PRICE,
    p.LOW_PRICE,
    p.CLOSE_PRICE,
    p.ADJUSTED_CLOSE_PRICE,
    p.VOLUME,
    p.DIVIDEND_AMOUNT,
    p.SPLIT_COEFFICIENT,
    p.LOAD_TIME
from {{ ref('stg_stockpricedata') }} p
