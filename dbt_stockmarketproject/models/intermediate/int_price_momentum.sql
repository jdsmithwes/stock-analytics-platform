{{ config(materialized='table') }}

with base as (
    select
        p.TICKER,
        p.TRADING_DATE,
        p.CLOSE_PRICE
    from {{ ref('stg_stockpricedata') }} p
)

select
    b.*,
    (b.CLOSE_PRICE / nullif(lag(b.CLOSE_PRICE, 21)
        over (partition by b.TICKER order by b.TRADING_DATE), 0)) - 1
        as ONE_MONTH_RETURN,

    (b.CLOSE_PRICE / nullif(lag(b.CLOSE_PRICE, 63)
        over (partition by b.TICKER order by b.TRADING_DATE), 0)) - 1
        as THREE_MONTH_RETURN,

    (b.CLOSE_PRICE / nullif(lag(b.CLOSE_PRICE, 126)
        over (partition by b.TICKER order by b.TRADING_DATE), 0)) - 1
        as SIX_MONTH_RETURN
from base b