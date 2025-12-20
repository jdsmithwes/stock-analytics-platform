{{ config(materialized='table') }}

with base as (
    select
        p.TICKER,
        p.TRADING_DATE,
        p.CLOSE_PRICE,
        p.VOLUME
    from {{ ref('stg_stockpricedata') }} p
),

stats as (
    select
        b.*,
        lag(b.CLOSE_PRICE)
            over (partition by b.TICKER order by b.TRADING_DATE) as PREV_CLOSE_PRICE,

        case
            when lag(b.CLOSE_PRICE)
                 over (partition by b.TICKER order by b.TRADING_DATE) = 0 then null
            else
                (b.CLOSE_PRICE /
                 lag(b.CLOSE_PRICE)
                 over (partition by b.TICKER order by b.TRADING_DATE)) - 1
        end as DAILY_RETURN
    from base b
)

select *
from stats