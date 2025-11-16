WITH master AS (
    SELECT *
    FROM {{ ref('mart_company_master') }}
)

SELECT
    sector,
    COUNT(DISTINCT ticker) AS number_of_companies,

    AVG(market_cap) AS avg_market_cap,
    SUM(market_cap) AS total_market_cap,

    AVG(ebitda) AS avg_ebitda,
    AVG(revenue_ttm) AS avg_revenue_ttm,
    AVG(gross_profit_ttm) AS avg_gross_profit_ttm,
    AVG(profit_margin) AS avg_profit_margin,
    AVG(operating_margin_ttm) AS avg_operating_margin,
    AVG(gross_margin) AS avg_gross_margin,

    AVG(pe_ratio) AS avg_pe_ratio,
    AVG(price_to_sales_ttm) AS avg_price_to_sales,
    AVG(price_to_book_ratio) AS avg_price_to_book,

    AVG(one_month_return) AS avg_1m_return,
    AVG(three_month_return) AS avg_3m_return,
    AVG(six_month_return) AS avg_6m_return,
    AVG(daily_volatility) AS avg_volatility,

    AVG(quality_score) AS avg_quality_score,
    AVG(blended_growth_score) AS avg_growth_score

FROM master
GROUP BY sector
ORDER BY total_market_cap DESC