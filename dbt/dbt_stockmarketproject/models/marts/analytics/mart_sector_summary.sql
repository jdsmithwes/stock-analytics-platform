{{ config(
    materialized = 'view'
) }}

with master as (
    select *
    from {{ ref('mart_company_master') }}
)

select
    sector,
    count(distinct ticker) as number_of_companies,

    avg(marketcapitalization) as avg_market_cap,
    sum(marketcapitalization) as total_market_cap,

    avg(ebitda) as avg_ebitda,
    avg(peratio) as avg_pe_ratio,

    avg(one_month_return) as avg_1m_return,
    avg(three_month_return) as avg_3m_return,

    avg(blended_growth_score) as avg_growth_score

from master
group by sector
order by total_market_cap desc