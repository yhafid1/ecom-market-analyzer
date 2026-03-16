-- ─────────────────────────────────────────────
-- window_functions.sql
-- Tracks category momentum over time using window functions
-- Powers: Rising vs Declining leaderboard, Trends Explorer
-- ─────────────────────────────────────────────


-- 1. Week-over-week rank delta for eBay categories
--    Positive delta = rising, negative = falling
SELECT
    category_name,
    snapshot_date,
    listing_count,
    sold_count,
    sell_through,
    LAG(sell_through) OVER (
        PARTITION BY category_name
        ORDER BY snapshot_date
    ) AS prev_sell_through,
    ROUND(
        (sell_through - LAG(sell_through) OVER (
            PARTITION BY category_name ORDER BY snapshot_date
        )) * 100, 2
    ) AS sell_through_delta,
    LAG(listing_count) OVER (
        PARTITION BY category_name ORDER BY snapshot_date
    ) AS prev_listing_count,
    listing_count - LAG(listing_count) OVER (
        PARTITION BY category_name ORDER BY snapshot_date
    ) AS listing_delta
FROM category_trends
WHERE source = 'ebay'
ORDER BY category_name, snapshot_date DESC;


-- 2. 4-week moving average of sell-through rate per category
--    Smooths out noise for cleaner trend lines
SELECT
    category_name,
    snapshot_date,
    sell_through,
    ROUND(
        AVG(sell_through) OVER (
            PARTITION BY category_name
            ORDER BY snapshot_date
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ), 4
    ) AS moving_avg_4w,
    ROUND(
        AVG(sell_through) OVER (
            PARTITION BY category_name
            ORDER BY snapshot_date
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ), 4
    ) AS moving_avg_8w
FROM category_trends
WHERE source = 'ebay'
ORDER BY category_name, snapshot_date DESC;


-- 3. Google Trends momentum — interest score delta over 4 weeks
SELECT
    category_name,
    snapshot_date,
    AVG(interest_score) AS avg_interest,
    ROUND(
        AVG(interest_score) - LAG(AVG(interest_score)) OVER (
            PARTITION BY category_name
            ORDER BY snapshot_date
        ), 2
    ) AS interest_delta,
    ROUND(
        AVG(AVG(interest_score)) OVER (
            PARTITION BY category_name
            ORDER BY snapshot_date
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_interest
FROM search_signals
WHERE geo = 'US'
GROUP BY category_name, snapshot_date
ORDER BY category_name, snapshot_date DESC;


-- 4. Reddit buzz momentum — mention count growth week over week
SELECT
    category_name,
    snapshot_date,
    SUM(mention_count) AS total_mentions,
    LAG(SUM(mention_count)) OVER (
        PARTITION BY category_name
        ORDER BY snapshot_date
    ) AS prev_mentions,
    SUM(mention_count) - LAG(SUM(mention_count)) OVER (
        PARTITION BY category_name ORDER BY snapshot_date
    ) AS mention_delta,
    ROUND(
        CASE
            WHEN LAG(SUM(mention_count)) OVER (
                PARTITION BY category_name ORDER BY snapshot_date
            ) > 0
            THEN (
                SUM(mention_count)::NUMERIC /
                LAG(SUM(mention_count)) OVER (
                    PARTITION BY category_name ORDER BY snapshot_date
                ) - 1
            ) * 100
            ELSE NULL
        END, 2
    ) AS mention_growth_pct
FROM social_buzz
GROUP BY category_name, snapshot_date
ORDER BY category_name, snapshot_date DESC;


-- 5. Category rank by opportunity score over time
--    Shows which categories are climbing vs dropping in rank
SELECT
    category_name,
    scored_at,
    opportunity_score,
    RANK() OVER (
        PARTITION BY scored_at
        ORDER BY opportunity_score DESC
    ) AS rank_this_week,
    LAG(RANK() OVER (
        PARTITION BY scored_at
        ORDER BY opportunity_score DESC
    )) OVER (
        PARTITION BY category_name
        ORDER BY scored_at
    ) AS rank_last_week,
    RANK() OVER (
        PARTITION BY scored_at ORDER BY opportunity_score DESC
    ) -
    LAG(RANK() OVER (
        PARTITION BY scored_at ORDER BY opportunity_score DESC
    )) OVER (
        PARTITION BY category_name ORDER BY scored_at
    ) AS rank_change
FROM niche_scores
ORDER BY scored_at DESC, rank_this_week;


-- 6. Percentile ranking of categories by sell-through
--    Useful for finding categories in top 25% of demand
SELECT
    category_name,
    snapshot_date,
    sell_through,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY snapshot_date
            ORDER BY sell_through
        ) * 100, 1
    ) AS sell_through_percentile,
    NTILE(4) OVER (
        PARTITION BY snapshot_date
        ORDER BY sell_through
    ) AS demand_quartile  -- 4 = top, 1 = bottom
FROM category_trends
WHERE snapshot_date = CURRENT_DATE
ORDER BY sell_through_percentile DESC;
