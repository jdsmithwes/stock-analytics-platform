WITH fundamentals AS (
    SELECT
        ticker,
        name,
        sector,
        industry,
        dividend_yield,
        dividend_per_share,
        dividend_date,
        ex_dividend_date,
        eps
    FROM {{ ref('stg_stockoverview') }}
),

latest_price AS (
    SELECT
        stock_ticker AS ticker,
        close_price AS latest_close_price
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stock_ticker ORDER BY trading_date DESC
    ) = 1
)

SELECT
    f.ticker,
    f.name,
    f.sector,
    f.industry,

    f.dividend_yield,
    f.dividend_per_share,
    f.dividend_date,
    f.ex_dividend_date,

    CASE WHEN f.eps IS NOT NULL AND f.eps != 0
         THEN f.dividend_per_share / f.eps END AS payout_ratio,

    CASE WHEN l.latest_close_price IS NOT NULL AND f.dividend_per_share IS NOT NULL
         THEN f.dividend_per_share / l.latest_close_price END AS dividend_yield_from_price,

    l.latest_close_price

FROM fundamentals f
LEFT JOIN latest_price l
    ON f.ticker = l.ticker
