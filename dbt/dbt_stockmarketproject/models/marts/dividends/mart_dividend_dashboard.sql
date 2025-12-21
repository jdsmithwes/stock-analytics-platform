{{ config(materialized='view') }}

select
    p.TICKER,
    p.TRADING_DATE,
    p.CLOSE_PRICE,
    p.DIVIDEND_AMOUNT,
    case
        when p.CLOSE_PRICE is null or p.CLOSE_PRICE = 0 then null
        else p.DIVIDEND_AMOUNT / p.CLOSE_PRICE
    end as DIVIDEND_YIELD_DAILY
from {{ ref('stg_stockpricedata') }} p
where p.DIVIDEND_AMOUNT > 0