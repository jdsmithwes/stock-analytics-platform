{{ config(
    materialized = 'incremental',
    unique_key = ['ticker','trade_date'],
    incremental_strategy = 'merge'
) }}

select
    ticker,
    trade_date,
    daily_return,
    one_month_return,
    three_month_return,
    load_time
from {{ ref('int_price_returns') }}

{% if is_incremental() %}
where trade_date > (
    select max(trade_date) from {{ this }}
)
{% endif %}