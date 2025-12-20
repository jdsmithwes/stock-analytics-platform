{{ config(
    materialized = 'table'
) }}

select
    ticker,
    name as company_name,
    sector,
    industry,
    country,
    exchange
from {{ ref('stg_stockoverview') }}