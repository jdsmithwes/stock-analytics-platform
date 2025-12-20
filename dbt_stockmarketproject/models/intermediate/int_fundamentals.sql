{{ config(
    materialized = 'incremental',
    unique_key = 'ticker',
    incremental_strategy = 'merge'
) }}

select
    ticker,
    marketcapitalization,
    ebitda,
    peratio,
    eps,
    RevenueTTM,
    EVToEBITDA
from {{ ref('stg_stockoverview') }}
