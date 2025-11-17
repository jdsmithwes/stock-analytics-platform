{{ config(materialized='view') }}

with base as (

    select
        TICKER,
        NAME,
        SECTOR,
        INDUSTRY,
        cast(REVENUETTM as float) as revenue_ttm,
        cast(GROSSPROFITTTM as float) as gross_profit_ttm,
        cast(QUARTERLYEARNINGSGROWTHYOY as float) as earnings_growth_yoy,
        cast(QUARTERLYREVENUEGROWTHYOY as float) as revenue_growth_yoy,
        cast(REVENUEPERSHARETTM as float) as revenue_per_share_ttm,
        cast(EPS as float) as eps,
        cast(DILUTEDEPSTTM as float) as diluted_eps_ttm,
        LOAD_TIME
    from {{ ref('stg_stockoverview') }}
),

growth_factors as (

    select
        *,
        
        -- 1. Normalize growth metrics into multipliers
        1 + (revenue_growth_yoy / 100) as revenue_growth_factor,
        1 + (earnings_growth_yoy / 100) as earnings_growth_factor,

        -- 2. Revenue per share and EPS ratios
        nullif(revenue_per_share_ttm, 0) as revenue_per_share,
        nullif(eps, 0) as eps_val,
        nullif(diluted_eps_ttm, 0) as diluted_eps_val,

        -- 3. EPS growth signal (derived)
        case 
            when eps_val is not null and diluted_eps_val is not null 
                and diluted_eps_val <> 0
            then eps_val / diluted_eps_val
        end as eps_growth_factor,

        -- 4. Blended growth rate (simple average)
        (
            coalesce(revenue_growth_yoy, 0) +
            coalesce(earnings_growth_yoy, 0)
        ) / 2.0 as blended_growth_rate_simple,

        -- 5. Blended growth factor (multiplicative)
        (
            coalesce(1 + revenue_growth_yoy / 100, 1) *
            coalesce(1 + earnings_growth_yoy / 100, 1)
        ) - 1 as blended_growth_factor_mult,

        -- 6. Full weighted blended score
        -- Revenue growth = 40%
        -- Earnings growth = 40%
        -- EPS growth = 20%
        (
              0.4 * coalesce(revenue_growth_yoy, 0)
            + 0.4 * coalesce(earnings_growth_yoy, 0)
            + 0.2 * coalesce(eps_growth_factor * 100 - 100, 0)
        ) as blended_growth_score_weighted

    from base
)

select * 
from growth_factors
