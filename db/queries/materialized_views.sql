-- ─────────────────────────────────────────────
-- materialized_views.sql
-- Precomputed views refreshed after each pipeline run
-- Dashboard queries these instead of running heavy CTEs live
-- Run once to create, then REFRESH after each pipeline run
-- ─────────────────────────────────────────────


-- 1. mv_category_leaderboard
--    Powers the Rising vs Declining tab
--    Full ranked list with momentum labels, refreshed weekly
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_leaderboard AS
WITH latest_scores AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        opportunity_score,
        trend_score,
        buzz_score,
        demand_score,
        spend_score,
        competition_score,
        channel_edge,
        recommendation,
        scored_at
    FROM niche_scores
    ORDER BY category_name, scored_at DESC
),
prev_scores AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        opportunity_score AS prev_opportunity_score,
        scored_at AS prev_scored_at
    FROM niche_scores
    WHERE scored_at < (SELECT MAX(scored_at) FROM niche_scores)
    ORDER BY category_name, scored_at DESC
),
demand_latest AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        sell_through,
        avg_price,
        listing_count
    FROM category_trends
    ORDER BY category_name, snapshot_date DESC
)
SELECT
    ls.category_name,
    ls.opportunity_score,
    ls.trend_score,
    ls.buzz_score,
    ls.demand_score,
    ls.spend_score,
    ls.competition_score,
    ls.channel_edge,
    ls.recommendation,
    ls.scored_at,
    ROUND(ls.opportunity_score - COALESCE(ps.prev_opportunity_score, ls.opportunity_score), 2) AS score_delta,
    RANK() OVER (ORDER BY ls.opportunity_score DESC) AS current_rank,
    dl.sell_through,
    dl.avg_price,
    dl.listing_count,
    CASE
        WHEN ls.opportunity_score - COALESCE(ps.prev_opportunity_score, ls.opportunity_score) > 5  THEN 'Rising'
        WHEN ls.opportunity_score - COALESCE(ps.prev_opportunity_score, ls.opportunity_score) < -5 THEN 'Declining'
        ELSE 'Stable'
    END AS momentum
FROM latest_scores ls
LEFT JOIN prev_scores   ps ON ps.category_name = ls.category_name
LEFT JOIN demand_latest dl ON dl.category_name = ls.category_name
ORDER BY ls.opportunity_score DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_leaderboard_category
    ON mv_category_leaderboard (category_name);


-- 2. mv_channel_comparison
--    Powers the Retail vs E-commerce tab
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_channel_comparison AS
SELECT
    rve.category_name,
    rve.period_date,
    ROUND(rve.ecomm_share * 100, 1)     AS ecomm_share_pct,
    ROUND(rve.retail_share * 100, 1)    AS retail_share_pct,
    ROUND(rve.ecomm_growth * 100, 2)    AS ecomm_growth_pct,
    ROUND(rve.retail_growth * 100, 2)   AS retail_growth_pct,
    CASE
        WHEN rve.ecomm_share > 0.55 THEN 'E-comm dominant'
        WHEN rve.ecomm_share < 0.30 THEN 'Retail dominant'
        ELSE 'Contested'
    END AS channel_status,
    CASE
        WHEN rve.ecomm_growth > 0.05 AND rve.ecomm_share < 0.45
        THEN 'Early mover opportunity'
        WHEN rve.ecomm_growth > 0.05 AND rve.ecomm_share >= 0.45
        THEN 'Growing — competitive'
        WHEN rve.ecomm_growth < 0 THEN 'E-comm contracting'
        ELSE 'Stable'
    END AS opportunity_label
FROM retail_vs_ecomm rve
ORDER BY rve.period_date DESC, rve.ecomm_growth DESC;

CREATE INDEX IF NOT EXISTS idx_mv_channel_category
    ON mv_channel_comparison (category_name, period_date);


-- 3. mv_niche_finder
--    Powers the Niche Finder tab — all signals in one flat row per category
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_niche_finder AS
WITH latest_demand AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        avg_price,
        listing_count,
        sold_count,
        ROUND(sell_through * 100, 2) AS sell_through_pct
    FROM category_trends
    WHERE source = 'ebay'
    ORDER BY category_name, snapshot_date DESC
),
latest_interest AS (
    SELECT
        category_name,
        ROUND(AVG(interest_score), 1) AS avg_interest_30d
    FROM search_signals
    WHERE
        geo = 'US'
        AND snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY category_name
),
latest_buzz AS (
    SELECT
        category_name,
        SUM(mention_count)              AS weekly_mentions,
        COUNT(DISTINCT subreddit)       AS subreddit_count
    FROM social_buzz
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY category_name
),
latest_spend AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        avg_annual_spend,
        ROUND(yoy_change * 100, 2) AS spend_growth_pct
    FROM consumer_spend
    ORDER BY category_name, year DESC
),
latest_scores AS (
    SELECT DISTINCT ON (category_name)
        category_name,
        opportunity_score,
        trend_score,
        buzz_score,
        demand_score,
        spend_score,
        competition_score,
        channel_edge,
        recommendation,
        scored_at
    FROM niche_scores
    ORDER BY category_name, scored_at DESC
)
SELECT
    ns.category_name,
    ns.opportunity_score,
    ns.trend_score,
    ns.buzz_score,
    ns.demand_score,
    ns.spend_score,
    ns.competition_score,
    ns.channel_edge,
    ns.recommendation,
    ns.scored_at,
    ld.avg_price,
    ld.listing_count,
    ld.sell_through_pct,
    li.avg_interest_30d,
    lb.weekly_mentions,
    lb.subreddit_count,
    ls.avg_annual_spend,
    ls.spend_growth_pct
FROM latest_scores ns
LEFT JOIN latest_demand   ld ON ld.category_name = ns.category_name
LEFT JOIN latest_interest li ON li.category_name = ns.category_name
LEFT JOIN latest_buzz     lb ON lb.category_name = ns.category_name
LEFT JOIN latest_spend    ls ON ls.category_name = ns.category_name
ORDER BY ns.opportunity_score DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_niche_category
    ON mv_niche_finder (category_name);


-- 4. mv_trends_explorer
--    Powers the Trends Explorer tab — time series for all signals
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trends_explorer AS
SELECT
    ct.category_name,
    ct.snapshot_date,
    ROUND(ct.sell_through * 100, 2)     AS sell_through_pct,
    ct.avg_price,
    ct.listing_count,
    ct.sold_count,
    COALESCE(ss.avg_interest, 0)        AS search_interest,
    COALESCE(sb.total_mentions, 0)      AS reddit_mentions,
    ct.source
FROM category_trends ct
LEFT JOIN (
    SELECT category_name, snapshot_date, ROUND(AVG(interest_score), 1) AS avg_interest
    FROM search_signals
    WHERE geo = 'US'
    GROUP BY category_name, snapshot_date
) ss ON ss.category_name = ct.category_name AND ss.snapshot_date = ct.snapshot_date
LEFT JOIN (
    SELECT category_name, snapshot_date, SUM(mention_count) AS total_mentions
    FROM social_buzz
    GROUP BY category_name, snapshot_date
) sb ON sb.category_name = ct.category_name AND sb.snapshot_date = ct.snapshot_date
ORDER BY ct.category_name, ct.snapshot_date DESC;

CREATE INDEX IF NOT EXISTS idx_mv_trends_category_date
    ON mv_trends_explorer (category_name, snapshot_date);


-- ─────────────────────────────────────────────
-- REFRESH COMMAND
-- Run this after each pipeline execution
-- Add to pipeline.py after load step completes
-- ─────────────────────────────────────────────

-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_category_leaderboard;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_channel_comparison;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_niche_finder;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_trends_explorer;
