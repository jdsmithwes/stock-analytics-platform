{{
    config(
        materialized='incremental',
        unique_key = ['TICKER', 'TRADING_DATE']
    )
}}

with raw as (

    select
        r.TICKER                                   as TICKER,
        r.DATE::date                              as TRADING_DATE,

        -- canonical price fields
        r.OPEN::float                             as OPEN_PRICE,
        r.HIGH::float                             as HIGH_PRICE,
        r.LOW::float                              as LOW_PRICE,
        r.CLOSE::float                            as CLOSE_PRICE,
        r.ADJUSTED_CLOSE::float                   as ADJUSTED_CLOSE_PRICE,

        r.VOLUME::number                          as VOLUME,
        r.DIVIDEND_AMOUNT::float                  as DIVIDEND_AMOUNT,
        r.SPLIT_COEFFICIENT::float                as SPLIT_COEFFICIENT,

        r.LOAD_TIME                               as LOAD_TIME

    from {{ source('stock_data', 'stock_price_data_raw') }} r

    {% if is_incremental() %}
        where r.LOAD_TIME >
            (
                select coalesce(max(t.LOAD_TIME), '1900-01-01'::timestamp_ntz)
                from {{ this }} t
            )
    {% endif %}

)

select *
from raw
where TICKER is not null
