{{ config(materialized='table') }}

WITH fundamentals AS (
    SELECT *
    FROM {{ ref('int_company_fundamentals') }}
),

momentum AS (
    SELECT
        ticker,
        avg_daily_return,
        daily_volatility,
        one_month_return,
        three_month_return,
        six_month_return
    FROM {{ ref('int_price_momentum') }}
),

sectors AS (
    SELECT *
    FROM {{ ref('int_sectors') }}
),

growth AS (
    SELECT
        ticker,
        earnings_growth_yoy AS quarterly_earnings_growth_yoy,
        revenue_growth_yoy AS quarterly_revenue_growth_yoy,
        blended_growth_score_weighted AS blended_growth_score
    FROM {{ ref('int_growth_factors') }}
),

quality AS (
    SELECT
        ticker,
        gross_margin,
        quality_score
    FROM {{ ref('int_quality_factors') }}
)

SELECT
    f.ticker,
    f.name,
    f.sector,
    f.industry,

    --------------------------
    -- PRICING
    --------------------------
    f.latest_close_price,
    f.latest_volume,
    f.latest_price_date,

    --------------------------
    -- FUNDAMENTALS
    --------------------------
    f.MARKETCAPITALIZATION          AS market_cap,
    f.EBITDA                        AS ebitda,
    f.PERATIO                       AS pe_ratio,
    f.PEGRATIO                      AS peg_ratio,
    f.BOOKVALUE                     AS book_value,
    f.DIVIDENDPERSHARE              AS dividend_per_share,
    f.DIVIDENDYIELD                 AS dividend_yield,
    f.EPS                           AS eps,
    f.REVENUEPERSHARETTM            AS revenue_per_share_ttm,
    f.PROFITMARGIN                  AS profit_margin,
    f.OPERATINGMARGINTTM            AS operating_margin_ttm,
    f.RETURNONASSETS                AS return_on_assets,
    f.RETURNONEQUITY                AS return_on_equity,
    f.REVENUETTM                    AS revenue_ttm,
    f.GROSSPROFITTTM                AS gross_profit_ttm,
    f.DILUTEDEPSTTM                 AS diluted_eps_ttm,
    f.ANALYSTTARGETPRICE            AS analyst_target_price,
    f.TRAILINGPE                    AS trailing_pe,
    f.FORWARDPE                     AS forward_pe,
    f.PRICETOSALESTTM               AS price_to_sales_ttm,
    f.PRICETOBOOKRATIO              AS price_to_book_ratio,
    f.EVTOREVENUE                   AS ev_to_revenue,
    f.EVTOEBITDA                    AS ev_to_ebitda,
    f.BETA                          AS beta,

    --------------------------
    -- MOMENTUM
    --------------------------
    m.one_month_return,
    m.three_month_return,
    m.six_month_return,
    m.avg_daily_return,
    m.daily_volatility,

    --------------------------
    -- GROWTH
    --------------------------
    g.quarterly_earnings_growth_yoy,
    g.quarterly_revenue_growth_yoy,
    g.blended_growth_score,

    --------------------------
    -- QUALITY
    --------------------------
    q.gross_margin,
    q.quality_score,

    --------------------------
    -- DERIVED RANKS
    --------------------------
    RANK() OVER (ORDER BY f.MARKETCAPITALIZATION DESC) AS market_cap_rank,
    RANK() OVER (ORDER BY m.six_month_return DESC NULLS LAST) AS momentum_rank,
    RANK() OVER (ORDER BY g.blended_growth_score DESC NULLS LAST) AS growth_rank,
    RANK() OVER (ORDER BY q.quality_score DESC NULLS LAST) AS quality_rank

FROM fundamentals f

LEFT JOIN momentum m
    ON f.ticker = m.ticker

LEFT JOIN growth g
    ON f.ticker = g.ticker

LEFT JOIN quality q
    ON f.ticker = q.ticker

ORDER BY f.MARKETCAPITALIZATION DESC