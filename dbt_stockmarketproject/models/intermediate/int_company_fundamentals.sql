{{ config(materialized='table') }}

with fundamentals as (
    select *
    from {{ ref('int_stock_overview') }}
),

latest_price as (
    select
        p.TICKER,
        p.TRADING_DATE,
        p.CLOSE_PRICE,
        row_number()
            over (partition by p.TICKER order by p.TRADING_DATE desc) as RN
    from {{ ref('stg_stockpricedata') }} p
)

select
    f.*,
    lp.TRADING_DATE      as LATEST_TRADING_DATE,
    lp.CLOSE_PRICE       as LATEST_CLOSE_PRICE
from fundamentals f
left join latest_price lp
    on f.TICKER = lp.TICKER
   and lp.RN = 1