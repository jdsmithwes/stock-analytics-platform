{{ config(materialized='view') }}

select
    p.TICKER,
    p.TRADING_DATE,
    p.CLOSE_PRICE,
    p.ADJUSTED_CLOSE_PRICE,
    p.VOLUME,
    s.DAILY_RETURN,
    m.ONE_MONTH_RETURN,
    m.THREE_MONTH_RETURN,
    m.SIX_MONTH_RETURN
from {{ ref('stg_stockpricedata') }} p
left join {{ ref('int_stock_stats') }} s
    on p.TICKER = s.TICKER
   and p.TRADING_DATE = s.TRADING_DATE
left join {{ ref('int_price_momentum') }} m
    on p.TICKER = m.TICKER
   and p.TRADING_DATE = m.TRADING_DATE