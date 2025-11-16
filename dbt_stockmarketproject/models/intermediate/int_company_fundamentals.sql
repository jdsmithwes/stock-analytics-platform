WITH fundamentals AS (
    SELECT *
    FROM {{ ref('stg_stockoverview') }}
),

latest_price AS (
    SELECT
        stock_ticker AS ticker,
        close_price,
        trading_volume,
        trading_date
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stock_ticker ORDER BY trading_date DESC
    ) = 1
)

SELECT
    f.*,
    lp.close_price AS latest_close_price,
    lp.trading_volume AS latest_volume,
    lp.trading_date  AS latest_price_date

FROM fundamentals f
LEFT JOIN latest_price lp
    ON f.ticker = lp.ticker