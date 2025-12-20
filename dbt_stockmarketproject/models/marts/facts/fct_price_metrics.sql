{{ config(
    materialized = 'incremental',
    unique_key = ['ticker','trading_date'],
    incremental_strategy = 'merge'
) }}

select
    ticker,
    trading_date,
    one_month_return,
    three_month_return
from {{ ref('int_price_momentum') }}

{% if is_incremental() %}
where trading_date > (
    select max(trading_date) from {{ this }}
)
{% endif %}
