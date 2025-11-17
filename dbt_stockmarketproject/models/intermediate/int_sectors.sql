{{ config(materialized='table') }}

WITH context_info AS (

    SELECT
        INDUSTRY,
        TICKER,

        -- Safe null handling for numeric fields
        CAST(NULLIF(MARKETCAPITALIZATION, 'None') AS FLOAT) AS market_capitalization,
        CAST(NULLIF(EBITDA, 'None') AS FLOAT) AS ebitda,
        CAST(NULLIF(PERATIO, 'None') AS FLOAT) AS pe_ratio,
        CAST(NULLIF(REVENUETTM, 'None') AS FLOAT) AS revenue_ttm,
        CAST(NULLIF(GROSSPROFITTTM, 'None') AS FLOAT) AS gross_profit_ttm

    FROM {{ ref('stg_stockoverview') }}

)

SELECT
    INDUSTRY,
    COUNT(DISTINCT TICKER)                         AS number_of_companies,
    AVG(market_capitalization)                     AS avg_market_cap,
    AVG(ebitda)                                     AS avg_ebitda,
    AVG(pe_ratio)                                   AS avg_pe_ratio,
    AVG(revenue_ttm)                                AS avg_revenue_ttm,
    AVG(gross_profit_ttm)                           AS avg_gross_profit_ttm

FROM context_info
GROUP BY INDUSTRY
ORDER BY number_of_companies DESC
