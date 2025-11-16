WITH prices AS (
    SELECT
        stock_ticker,
        trading_date,
        close_price,
        trading_volume,
        LAG(close_price) OVER (
            PARTITION BY stock_ticker
            ORDER BY trading_date
        ) AS prev_close
    FROM {{ ref('stg_stockpricedata') }}
),

returns AS (
    SELECT
        stock_ticker,
        trading_date,
        close_price,
        trading_volume,
        (close_price - prev_close) / prev_close AS daily_return
    FROM prices
    WHERE prev_close IS NOT NULL
),

rolling AS (
    SELECT
        stock_ticker,
        trading_date,
        close_price,
        trading_volume,
        daily_return,

        AVG(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS avg_1m_return,
        STDDEV(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS vol_1m,

        AVG(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS avg_3m_return,
        STDDEV(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS vol_3m,

        AVG(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 120 PRECEDING AND CURRENT ROW) AS avg_6m_return,
        STDDEV(daily_return) OVER (PARTITION BY stock_ticker ORDER BY trading_date ROWS BETWEEN 120 PRECEDING AND CURRENT ROW) AS vol_6m
    FROM returns
)

SELECT
    r.*,
    o.sector,
    o.industry,
    o.market_cap,
    o.beta
FROM rolling r
LEFT JOIN {{ ref('stg_stockoverview') }} o
    ON r.stock_ticker = o.ticker