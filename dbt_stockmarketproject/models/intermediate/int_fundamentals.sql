{{ config(
    materialized = 'incremental',
    unique_key = 'ticker',
    incremental_strategy = 'merge'
) }}

select
    ticker,
    revenue_growth_yoy,
    eps_growth_yoy,
    load_time
from {{ ref('stg_fundamentals') }}

{% if is_incremental() %}
where load_time > (
    select coalesce(max(load_time), '1900-01-01') from {{ this }}
)
{% endif %}