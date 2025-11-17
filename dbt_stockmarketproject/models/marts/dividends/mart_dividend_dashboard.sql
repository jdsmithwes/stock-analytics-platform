WITH overview AS (
    SELECT
        TICKER,
        NAME,
        SECTOR,
        INDUSTRY,
        DIVIDENDPERSHARE,
        DIVIDENDYIELD,        -- 👈 FIXED
        DIVIDENDDATE,
        EXDIVIDENDDATE
    FROM {{ ref('stg_stockoverview') }}
),

latest_price AS (
    SELECT
        STOCK_TICKER AS TICKER,
        CLOSE_PRICE AS LATEST_PRICE
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY STOCK_TICKER ORDER BY TRADING_DATE DESC
    ) = 1
)

SELECT
    o.TICKER,
    o.NAME,
    o.SECTOR,
    o.INDUSTRY,
    o.DIVIDENDPERSHARE,
    o.DIVIDENDYIELD,
    o.DIVIDENDDATE,
    o.EXDIVIDENDDATE,
    l.LATEST_PRICE,
    CASE 
        WHEN o.DIVIDENDPERSHARE IS NOT NULL AND o.DIVIDENDPERSHARE != 0
            THEN o.DIVIDENDPERSHARE / NULLIF(l.LATEST_PRICE,0)
    END AS DIVIDEND_PAYOUT_RATIO
FROM overview o
LEFT JOIN latest_price l
    ON o.TICKER = l.TICKER