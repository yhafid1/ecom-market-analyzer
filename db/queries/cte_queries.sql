-- ─────────────────────────────────────────────
-- cte_queries.sql
-- Complex multi-source analysis using CTEs
-- Powers: Niche Finder, Product Deep Dive
-- ─────────────────────────────────────────────


-- 1. Full niche opportunity score breakdown
--    Pulls all signals together into one ranked table
--    This is the core query behind the Niche Finder tab
WITH trend_signals AS (
    SELECT
        category_name,
        ROUND(AVG(interest_score), 2)       AS avg_interest,
        ROUND(MAX(interest_score) - MIN(interest_score), 2) AS interest_range
    FROM search_signals
    WHERE
        geo = 'US'
        AND snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY category_name
),
buzz_signals AS (
    SELECT
        category_name,
        SUM(mention_count)                  AS total_mentions,
        ROUND(AVG(avg_score), 2)            AS avg_post_score,
        COUNT(DISTINCT subreddit)           AS subreddit_spread
    FROM social_buzz
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY category_name
),
demand_signals AS (
    SELECT
        category_name,
        ROUND(AVG(sell_through) * 100, 2)   AS avg_sell_through_pct,
        ROUND(AVG(avg_price), 2)            AS avg_price,
        SUM(listing_count)                  AS total_listings,
        SUM(sold_count)                     AS total_sold
    FROM category_trends
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY category_name
),
spend_signals AS (
    SELECT
        category_name,
        ROUND(AVG(yoy_change) * 100, 2)     AS avg_spend_growth_pct,
        MAX(avg_annual_spend)               AS latest_spend
    FROM consumer_spend
    WHERE year >= EXTRACT(YEAR FROM NOW())::INT - 3
    GROUP BY category_name
),
channel_signals AS (
    SELECT
        category_name,
        ROUND(AVG(ecomm_share) * 100, 2)    AS ecomm_share_pct,
        ROUND(AVG(ecomm_growth) * 100, 2)   AS ecomm_growth_pct
    FROM retail_vs_ecomm
    WHERE period_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY category_name
)
SELECT
    d.category_name,
    -- Raw signals
    COALESCE(t.avg_interest, 0)             AS trend_interest,
    COALESCE(b.total_mentions, 0)           AS buzz_mentions,
    COALESCE(b.subreddit_spread, 0)         AS subreddit_spread,
    COALESCE(d.avg_sell_through_pct, 0)     AS sell_through_pct,
    COALESCE(d.avg_price, 0)               AS avg_price,
    COALESCE(d.total_listings, 0)          AS total_listings,
    COALESCE(s.avg_spend_growth_pct, 0)    AS spend_growth_pct,
    COALESCE(s.latest_spend, 0)            AS annual_spend_usd,
    COALESCE(c.ecomm_share_pct, 0)         AS ecomm_share_pct,
    COALESCE(c.ecomm_growth_pct, 0)        AS ecomm_growth_pct,
    -- Composite score from niche_scores (pre-computed by pipeline)
    ns.opportunity_score,
    ns.trend_score,
    ns.buzz_score,
    ns.demand_score,
    ns.spend_score,
    ns.competition_score,
    ns.channel_edge,
    ns.recommendation
FROM demand_signals d
LEFT JOIN trend_signals   t  ON t.category_name  = d.category_name
LEFT JOIN buzz_signals    b  ON b.category_name  = d.category_name
LEFT JOIN spend_signals   s  ON s.category_name  = d.category_name
LEFT JOIN channel_signals c  ON c.category_name  = d.category_name
LEFT JOIN niche_scores    ns ON ns.category_name = d.category_name
    AND ns.scored_at = (
        SELECT MAX(scored_at) FROM niche_scores
    )
ORDER BY ns.opportunity_score DESC NULLS LAST;


-- 2. Rising categories — strong momentum across multiple signals
--    Categories where at least 3 of 4 signals are positive
WITH signal_directions AS (
    SELECT
        ns.category_name,
        ns.opportunity_score,
        ns.recommendation,
        -- is search interest trending up?
        CASE WHEN ss_delta.interest_delta > 0 THEN 1 ELSE 0 END AS trend_up,
        -- is buzz growing?
        CASE WHEN buzz_delta.mention_delta > 0 THEN 1 ELSE 0 END AS buzz_up,
        -- is sell-through above median?
        CASE WHEN ct.sell_through > 0.15 THEN 1 ELSE 0 END AS demand_up,
        -- is consumer spend growing?
        CASE WHEN cs.yoy_change > 0 THEN 1 ELSE 0 END AS spend_up
    FROM niche_scores ns
    LEFT JOIN (
        SELECT category_name,
            AVG(interest_score) - LAG(AVG(interest_score)) OVER (
                PARTITION BY category_name ORDER BY snapshot_date
            ) AS interest_delta
        FROM search_signals
        WHERE snapshot_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY category_name, snapshot_date
    ) ss_delta ON ss_delta.category_name = ns.category_name
    LEFT JOIN (
        SELECT category_name,
            SUM(mention_count) - LAG(SUM(mention_count)) OVER (
                PARTITION BY category_name ORDER BY snapshot_date
            ) AS mention_delta
        FROM social_buzz
        WHERE snapshot_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY category_name, snapshot_date
    ) buzz_delta ON buzz_delta.category_name = ns.category_name
    LEFT JOIN category_trends ct
        ON ct.category_name = ns.category_name
        AND ct.snapshot_date = CURRENT_DATE
    LEFT JOIN consumer_spend cs
        ON cs.category_name = ns.category_name
        AND cs.year = EXTRACT(YEAR FROM NOW())::INT - 1
    WHERE ns.scored_at = (SELECT MAX(scored_at) FROM niche_scores)
)
SELECT
    category_name,
    opportunity_score,
    recommendation,
    trend_up + buzz_up + demand_up + spend_up   AS signals_positive,
    CASE
        WHEN trend_up + buzz_up + demand_up + spend_up >= 3 THEN 'Rising'
        WHEN trend_up + buzz_up + demand_up + spend_up <= 1 THEN 'Declining'
        ELSE 'Mixed'
    END AS momentum_label
FROM signal_directions
ORDER BY signals_positive DESC, opportunity_score DESC;


-- 3. Retail vs e-commerce gap analysis
--    Finds categories where e-comm is taking share fast
--    High ecomm_growth + low current ecomm_share = early mover opportunity
WITH channel_trends AS (
    SELECT
        category_name,
        period_date,
        ecomm_share,
        retail_share,
        ecomm_growth,
        LAG(ecomm_share) OVER (
            PARTITION BY category_name ORDER BY period_date
        ) AS prev_ecomm_share
    FROM retail_vs_ecomm
),
channel_summary AS (
    SELECT
        category_name,
        ROUND(AVG(ecomm_share) * 100, 1)    AS avg_ecomm_share_pct,
        ROUND(AVG(retail_share) * 100, 1)   AS avg_retail_share_pct,
        ROUND(AVG(ecomm_growth) * 100, 2)   AS avg_ecomm_growth_pct,
        ROUND(MAX(ecomm_share) - MIN(ecomm_share), 4) AS ecomm_share_gain,
        COUNT(*) AS periods_tracked
    FROM channel_trends
    GROUP BY category_name
)
SELECT
    category_name,
    avg_ecomm_share_pct,
    avg_retail_share_pct,
    avg_ecomm_growth_pct,
    ROUND(ecomm_share_gain * 100, 2)        AS share_gain_pct,
    CASE
        WHEN avg_ecomm_share_pct > 55 THEN 'E-comm dominant'
        WHEN avg_ecomm_share_pct < 30 THEN 'Retail dominant'
        ELSE 'Contested'
    END AS channel_status,
    CASE
        WHEN avg_ecomm_growth_pct > 5
            AND avg_ecomm_share_pct < 45
        THEN 'Early mover opportunity'
        WHEN avg_ecomm_growth_pct > 5
            AND avg_ecomm_share_pct >= 45
        THEN 'Growing — competitive'
        WHEN avg_ecomm_growth_pct < 0
        THEN 'E-comm contracting'
        ELSE 'Stable'
    END AS opportunity_label
FROM channel_summary
ORDER BY avg_ecomm_growth_pct DESC;


-- 4. Product deep dive — all signals for a single category
--    Parameterized: replace :category with e.g. 'Electronics'
WITH weekly_demand AS (
    SELECT
        snapshot_date,
        listing_count,
        sold_count,
        avg_price,
        ROUND(sell_through * 100, 2)        AS sell_through_pct,
        source
    FROM category_trends
    WHERE category_name = :category
    ORDER BY snapshot_date DESC
    LIMIT 12
),
weekly_interest AS (
    SELECT
        snapshot_date,
        ROUND(AVG(interest_score), 1)       AS avg_interest
    FROM search_signals
    WHERE category_name = :category AND geo = 'US'
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    LIMIT 12
),
weekly_buzz AS (
    SELECT
        snapshot_date,
        SUM(mention_count)                  AS total_mentions,
        ROUND(AVG(avg_score), 1)            AS avg_post_score
    FROM social_buzz
    WHERE category_name = :category
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    LIMIT 12
),
latest_score AS (
    SELECT
        opportunity_score,
        trend_score,
        buzz_score,
        demand_score,
        spend_score,
        competition_score,
        channel_edge,
        recommendation
    FROM niche_scores
    WHERE category_name = :category
    ORDER BY scored_at DESC
    LIMIT 1
)
SELECT
    'demand'                                AS signal_type,
    wd.snapshot_date                        AS period,
    wd.sell_through_pct                     AS value,
    wd.avg_price                            AS secondary_value,
    ls.opportunity_score,
    ls.recommendation
FROM weekly_demand wd
CROSS JOIN latest_score ls
UNION ALL
SELECT
    'interest',
    wi.snapshot_date,
    wi.avg_interest,
    NULL,
    ls.opportunity_score,
    ls.recommendation
FROM weekly_interest wi
CROSS JOIN latest_score ls
UNION ALL
SELECT
    'buzz',
    wb.snapshot_date,
    wb.total_mentions,
    wb.avg_post_score,
    ls.opportunity_score,
    ls.recommendation
FROM weekly_buzz wb
CROSS JOIN latest_score ls
ORDER BY signal_type, period DESC;


-- 5. Top entry opportunities right now
--    Filters to categories that score well on all dimensions
--    The "what should I sell today" query
SELECT
    ns.category_name,
    ns.opportunity_score,
    ns.demand_score,
    ns.trend_score,
    ns.buzz_score,
    ns.competition_score,
    ns.channel_edge,
    ct.avg_price,
    ct.sell_through,
    rve.ecomm_share,
    rve.ecomm_growth
FROM niche_scores ns
LEFT JOIN category_trends ct
    ON ct.category_name = ns.category_name
    AND ct.snapshot_date = CURRENT_DATE
    AND ct.source = 'ebay'
LEFT JOIN retail_vs_ecomm rve
    ON rve.category_name = ns.category_name
    AND rve.period_date = (
        SELECT MAX(period_date) FROM retail_vs_ecomm
    )
WHERE
    ns.scored_at = (SELECT MAX(scored_at) FROM niche_scores)
    AND ns.recommendation = 'enter'
    AND ns.opportunity_score >= 60
    AND ct.avg_price BETWEEN 15 AND 200   -- practical seller price range
ORDER BY ns.opportunity_score DESC;
