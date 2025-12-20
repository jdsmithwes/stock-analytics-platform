{{ config(
    materialized = 'view'
) }}

select
    ticker,
    company_name,
    sector,
    industry,

    -- company fundamentals
    marketcapitalization,
    ebitda,
    peratio,
    eps,
    evtoebitda,

    -- price-based metrics
    one_month_return,
    three_month_return,

    -- derived metric from company_master
    blended_growth_score

from {{ ref('mart_company_master') }}