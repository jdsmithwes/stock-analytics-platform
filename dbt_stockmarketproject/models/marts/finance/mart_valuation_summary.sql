WITH fundamentals AS (
    SELECT
        TICKER,
        NAME,
        SECTOR,
        INDUSTRY,
        MARKETCAPITALIZATION      AS MARKET_CAP,
        PERATIO                   AS PE_RATIO,
        PRICETOSALESTTM           AS PRICE_TO_SALES_TTM,
        PRICETOBOOKRATIO          AS PRICE_TO_BOOK_RATIO,
        EVTOEBITDA,
        EVTOREVENUE,
        EPS
    FROM {{ ref('stg_stockoverview') }}
),

latest_price AS (
    SELECT
        STOCK_TICKER                  AS TICKER,
        CLOSE_PRICE                   AS LATEST_CLOSE_PRICE
    FROM {{ ref('stg_stockpricedata') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY STOCK_TICKER
        ORDER BY TRADING_DATE DESC
    ) = 1
)

SELECT
    f.TICKER,
    f.NAME,
    f.SECTOR,
    f.INDUSTRY,
    f.MARKET_CAP,
    f.PE_RATIO,
    f.PRICE_TO_SALES_TTM,
    f.PRICE_TO_BOOK_RATIO,
    f.EVTOEBITDA,
    f.EVTOREVENUE,
    l.LATEST_CLOSE_PRICE,

    CASE 
        WHEN f.EPS IS NOT NULL AND f.EPS != 0
            THEN l.LATEST_CLOSE_PRICE / f.EPS
    END AS PRICE_TO_EARNINGS_LATEST

FROM fundamentals f
LEFT JOIN latest_price l
    ON f.TICKER = l.TICKER