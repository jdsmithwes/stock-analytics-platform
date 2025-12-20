{{ config(materialized='view') }}

select
    c.ticker,
    c.company_name,
    c.sector,
    c.industry,

    f.revenue_growth_yoy,
    f.eps_growth_yoy,

    p.one_month_return,
    p.three_month_return
from {{ ref('dim_company') }} c
left join {{ ref('int_fundamentals') }} f
    on c.ticker = f.ticker
left join {{ ref('fct_price_metrics') }} p
    on c.ticker = p.ticker