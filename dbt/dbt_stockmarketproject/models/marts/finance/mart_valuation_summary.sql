{{ config(materialized='view') }}

select
    f.TICKER,
    f.NAME,
    f.SECTOR,
    f.INDUSTRY,
    f.LATEST_TRADING_DATE,
    f.LATEST_CLOSE_PRICE,
    f.MARKETCAPITALIZATION,
    f.PERATIO
from {{ ref('int_company_fundamentals') }} f