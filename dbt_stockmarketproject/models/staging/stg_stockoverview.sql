with src as (

    select
        raw,
        metadata$filename as file_name,
        load_time
    from {{ source('stock_data', 'company_overview_json_raw') }}

),

parsed as (

    select
        raw:"Symbol"::string                           as symbol,
        raw:"AssetType"::string                        as asset_type,
        raw:"Name"::string                             as company_name,
        raw:"Description"::string                      as description,
        raw:"CIK"::string                              as cik,
        raw:"Exchange"::string                         as exchange,
        raw:"Currency"::string                         as currency,
        raw:"Country"::string                          as country,
        raw:"Sector"::string                           as sector,
        raw:"Industry"::string                         as industry,
        
        raw:"MarketCapitalization"::number             as market_cap,
        raw:"EBITDA"::number                           as ebitda,
        raw:"PERatio"::float                           as pe_ratio,
        raw:"PEGRatio"::float                          as peg_ratio,
        raw:"BookValue"::float                         as book_value,
        raw:"DividendPerShare"::float                  as dividend_per_share,
        raw:"DividendYield"::float                     as dividend_yield,
        raw:"EPS"::float                               as eps,
        raw:"RevenueTTM"::number                       as revenue_ttm,
        raw:"GrossProfitTTM"::number                   as gross_profit_ttm,
        raw:"AnalystTargetPrice"::float                as analyst_target_price,
        raw:"52WeekHigh"::float                        as fifty_two_week_high,
        raw:"52WeekLow"::float                         as fifty_two_week_low,
        raw:"DividendDate"::string                     as dividend_date,
        raw:"ExDividendDate"::string                   as ex_dividend_date,

        file_name,
        load_time
    from src

)

select * from parsed