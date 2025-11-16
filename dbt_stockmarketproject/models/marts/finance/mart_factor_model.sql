WITH master AS (
    SELECT *
    FROM {{ ref('mart_company_master') }}
)

SELECT
    ticker,
    name,
    sector,
    industry,
    latest_close_price,
    market_cap,

    ----------------------------
    -- VALUE FACTORS
    ----------------------------
    pe_ratio,
    price_to_sales_ttm,
    price_to_book_ratio,
    ev_to_ebitda,
    ev_to_revenue,

    ----------------------------
    -- GROWTH FACTORS
    ----------------------------
    quarterly_earnings_growth_yoy,
    quarterly_revenue_growth_yoy,
    blended_growth_score,

    ----------------------------
    -- MOMENTUM FACTORS
    ----------------------------
    one_month_return,
    three_month_return,
    six_month_return,

    ----------------------------
    -- QUALITY FACTORS
    ----------------------------
    return_on_equity,
    return_on_assets,
    gross_margin,
    quality_score,

    ----------------------------
    -- COMPOSITE SCORE
    ----------------------------
    (
        0.30 * (100 - momentum_rank) +
        0.25 * (100 - growth_rank) +
        0.25 * (100 - quality_rank) +
        0.20 * (100 - market_cap_rank)
    ) AS composite_factor_score

FROM master
ORDER BY composite_factor_score DESC