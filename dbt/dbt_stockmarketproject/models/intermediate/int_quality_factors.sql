{{ config(materialized='table') }}

with fundamentals as (
    select
        TICKER,
        RETURNONEQUITY,
        RETURNONASSETS,
        PROFITMARGIN,
        OPERATINGMARGINTTM,
        GROSSPROFITTTM,
        REVENUETTM,
        BETA
    from {{ ref('stg_stockoverview') }}
),

volatility as (
    select
        s.TICKER,

        -- 30-day rolling volatility of daily returns
        stddev(s.DAILY_RETURN)
            over (
                partition by s.TICKER
                order by s.TRADING_DATE
                rows between 29 preceding and current row
            ) as DAILY_VOLATILITY

    from {{ ref('int_stock_stats') }} s
)

select
    f.TICKER,

    f.RETURNONEQUITY       as return_on_equity,
    f.RETURNONASSETS       as return_on_assets,
    f.PROFITMARGIN         as profit_margin,
    f.OPERATINGMARGINTTM   as operating_margin_ttm,

    -- Gross margin (safe)
    f.GROSSPROFITTTM / nullif(f.REVENUETTM, 0) as gross_margin,

    v.DAILY_VOLATILITY,
    f.BETA,

    -- Unified quality score
    (
        coalesce(f.RETURNONEQUITY, 0) +
        coalesce(f.RETURNONASSETS, 0) +
        coalesce(f.PROFITMARGIN, 0) +
        coalesce(f.OPERATINGMARGINTTM, 0)
    ) as quality_score

from fundamentals f
left join volatility v
    on f.TICKER = v.TICKER