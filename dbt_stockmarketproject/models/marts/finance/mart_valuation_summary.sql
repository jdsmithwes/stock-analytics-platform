WITH fundamentals AS (
    SELECT
        ticker,
        name,
        sector,
        industry,
        market_cap,
        pe_ratio,
        price_to_sales_ttm,
        price_to_book_ratio,
        ev_to_ebitda,
        ev_to_revenue,
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
    f.industry,     -- 🔥 FIXED
    f.market_cap,
    f.pe_ratio,
    f.price_to_sales_ttm,
    f.price_to_book_ratio,
    f.ev_to_ebitda,
    f.ev_to_revenue,

    l.latest_close_price,

    CASE WHEN f.eps IS NOT NULL AND f.eps != 0
         THEN l.latest_close_price / f.eps END AS price_to_earnings_latest
FROM fundamentals f
LEFT JOIN latest_price l
    ON f.ticker = l.ticker