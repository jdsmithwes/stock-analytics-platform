{{ config(
    materialized = 'table'
) }}

select
    ticker,
    company_name,
    sector,
    industry,
    country,
    exchange
from {{ ref('stg_company_overview') }}