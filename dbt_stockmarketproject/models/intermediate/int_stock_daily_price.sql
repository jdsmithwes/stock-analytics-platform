WITH base AS (
    SELECT *
    FROM {{ ref('stg_stockpricedata') }}
),

calendar AS (
    SELECT
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM base
),

dates AS (
    SELECT
        DATEADD(day, seq4(), (SELECT start_date FROM calendar)) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 20000))
    WHERE date <= (SELECT end_date FROM calendar)
),

tickers AS (
    SELECT DISTINCT ticker FROM base
),

grid AS (
    SELECT
        t.ticker,
        d.date
    FROM tickers t
    CROSS JOIN dates d
),

joined AS (
    SELECT
        g.ticker,
        g.date,
        b.open,
        b.high,
        b.low,
        b.close,
        b.adjusted_close,
        b.volume,
        b.dividend_amount,
        b.split_coefficient,
        b.load_time
    FROM grid g
    LEFT JOIN base b
        ON g.ticker = b.ticker
       AND g.date = b.date
),

filled AS (
    SELECT
        ticker,
        date,
        LAST_VALUE(open IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS open,
        LAST_VALUE(high IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS high,
        LAST_VALUE(low IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS low,
        LAST_VALUE(close IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS close,
        LAST_VALUE(adjusted_close IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS adjusted_close,
        0 AS volume,  -- weekends/holidays
        0 AS dividend_amount,
        LAST_VALUE(split_coefficient IGNORE NULLS) OVER (PARTITION BY ticker ORDER BY date) AS split_coefficient,
        MAX(load_time) OVER (PARTITION BY ticker) AS last_load_time
    FROM joined
)

SELECT * FROM filled;
