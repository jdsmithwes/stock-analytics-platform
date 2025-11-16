WITH daily_prices AS (
    SELECT
        stock_ticker,
        trading_date,
        close_price,
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
        (close_price - prev_close) / prev_close AS daily_return
    FROM daily_prices
    WHERE prev_close IS NOT NULL
),

aggregates AS (
    SELECT
        stock_ticker,

        AVG(daily_return) AS avg_daily_return,
        STDDEV(daily_return) AS daily_volatility,

        -- ⭐ FIXED: Snowflake-friendly window return calcs
        SUM(IFF(trading_date >= DATEADD(month, -1, CURRENT_DATE), daily_return, 0)) AS one_month_return,
        SUM(IFF(trading_date >= DATEADD(month, -3, CURRENT_DATE), daily_return, 0)) AS three_month_return,
        SUM(IFF(trading_date >= DATEADD(month, -6, CURRENT_DATE), daily_return, 0)) AS six_month_return

    FROM returns
    GROUP BY stock_ticker
)

SELECT *
FROM aggregates

