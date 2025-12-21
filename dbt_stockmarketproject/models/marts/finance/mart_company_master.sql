{{ config(
    materialized = 'view'
) }}

with fundamentals as (

    select
        ticker,
        name as company_name,
        sector,
        industry,
        marketcapitalization,
        ebitda,
        peratio,
        eps,
        evtoebitda
    from {{ ref('stg_stockoverview') }}

),

price_metrics as (

    select
        ticker,
        trading_date,
        one_month_return,
        three_month_return
    from {{ ref('fct_price_metrics') }}

),

latest_price_metrics as (

    select
        ticker,
        one_month_return,
        three_month_return
    from price_metrics
    qualify trading_date = max(trading_date) over (partition by ticker)

)

select
    f.ticker,
    f.company_name,
    f.sector,
    f.industry,
    f.marketcapitalization,
    f.ebitda,
    f.peratio,
    f.eps,
    f.evtoebitda,

    p.one_month_return,
    p.three_month_return,

    /* derived business metric – mart-only */
    (
        coalesce(p.one_month_return, 0) * 0.5
      + coalesce(p.three_month_return, 0) * 0.5
    ) as blended_growth_score

from fundamentals f
left join latest_price_metrics p
  on f.ticker = p.ticker