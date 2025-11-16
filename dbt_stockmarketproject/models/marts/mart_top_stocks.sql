WITH master AS (
    SELECT *
    FROM {{ ref('mart_company_master') }}
),

scoring AS (
    SELECT
        ticker,
        name,
        sector,
        industry,
        latest_close_price,
        market_cap,

        -- NORMALIZED FACTORS (0–100)
        100 - (market_cap_rank * 100.0 / MAX(market_cap_rank) OVER ()) AS value_factor,
        100 - (momentum_rank * 100.0 / MAX(momentum_rank) OVER ()) AS momentum_factor,
        100 - (growth_rank * 100.0 / MAX(growth_rank) OVER ()) AS growth_factor,
        100 - (quality_rank * 100.0 / MAX(quality_rank) OVER ()) AS quality_factor,

        -- COMPOSITE SCORE
        (
            0.30 * (100 - momentum_rank) +
            0.25 * (100 - growth_rank) +
            0.25 * (100 - quality_rank) +
            0.20 * (100 - market_cap_rank)
        ) AS composite_score

    FROM master
)

SELECT *
FROM scoring
ORDER BY composite_score DESC
LIMIT 50