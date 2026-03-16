-- E-Commerce Intelligence System
-- Schema: run once on fresh DB or mounted via docker-entrypoint-initdb.d

-- ─────────────────────────────────────────────
-- 1. category_trends
--    Weekly snapshots of eBay category performance
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category_trends (
    id              SERIAL PRIMARY KEY,
    category_name   TEXT NOT NULL,
    category_id     TEXT,
    source          TEXT NOT NULL DEFAULT 'ebay',
    snapshot_date   DATE NOT NULL,
    listing_count   INTEGER,
    avg_price       NUMERIC(10, 2),
    sold_count      INTEGER,
    sell_through    NUMERIC(5, 4),      -- sold / listed ratio
    rank_position   INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (category_name, source, snapshot_date)
);

-- ─────────────────────────────────────────────
-- 2. retail_vs_ecomm
--    Channel comparison by product category over time
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS retail_vs_ecomm (
    id              SERIAL PRIMARY KEY,
    category_name   TEXT NOT NULL,
    period_date     DATE NOT NULL,
    ecomm_share     NUMERIC(5, 4),      -- 0.0 to 1.0
    retail_share    NUMERIC(5, 4),
    ecomm_growth    NUMERIC(8, 4),      -- YoY % change
    retail_growth   NUMERIC(8, 4),
    source          TEXT NOT NULL DEFAULT 'bls',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (category_name, period_date, source)
);

-- ─────────────────────────────────────────────
-- 3. search_signals
--    Google Trends interest scores per keyword/category
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS search_signals (
    id              SERIAL PRIMARY KEY,
    keyword         TEXT NOT NULL,
    category_name   TEXT,
    snapshot_date   DATE NOT NULL,
    interest_score  INTEGER,            -- 0-100 Google Trends scale
    geo             TEXT DEFAULT 'US',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (keyword, snapshot_date, geo)
);

-- ─────────────────────────────────────────────
-- 4. social_buzz
--    Reddit mention volume in buying/selling subs
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS social_buzz (
    id              SERIAL PRIMARY KEY,
    category_name   TEXT NOT NULL,
    keyword         TEXT NOT NULL,
    subreddit       TEXT NOT NULL,
    snapshot_date   DATE NOT NULL,
    mention_count   INTEGER DEFAULT 0,
    avg_score       NUMERIC(8, 2),      -- avg upvote score of posts
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (keyword, subreddit, snapshot_date)
);

-- ─────────────────────────────────────────────
-- 5. consumer_spend
--    BLS Consumer Expenditure Survey by category
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS consumer_spend (
    id              SERIAL PRIMARY KEY,
    category_name   TEXT NOT NULL,
    bls_category    TEXT,
    year            INTEGER NOT NULL,
    avg_annual_spend NUMERIC(12, 2),    -- avg household spend USD
    yoy_change      NUMERIC(8, 4),      -- YoY % change
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (category_name, year)
);

-- ─────────────────────────────────────────────
-- 6. niche_scores
--    Composite opportunity score (computed, refreshed weekly)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS niche_scores (
    id                  SERIAL PRIMARY KEY,
    category_name       TEXT NOT NULL,
    scored_at           DATE NOT NULL,
    trend_score         NUMERIC(5, 2),  -- Google Trends momentum (0-100)
    buzz_score          NUMERIC(5, 2),  -- Reddit growth signal (0-100)
    demand_score        NUMERIC(5, 2),  -- eBay sell-through rate (0-100)
    spend_score         NUMERIC(5, 2),  -- BLS spend trajectory (0-100)
    competition_score   NUMERIC(5, 2),  -- inverse of listing growth (0-100)
    opportunity_score   NUMERIC(5, 2),  -- weighted composite (0-100)
    channel_edge        TEXT,           -- 'ecomm', 'retail', or 'mixed'
    recommendation      TEXT,           -- 'enter', 'watch', 'avoid'
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (category_name, scored_at)
);

-- ─────────────────────────────────────────────
-- Indexes for common query patterns
-- ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ct_category_date   ON category_trends (category_name, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ss_keyword_date    ON search_signals (keyword, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_sb_category_date   ON social_buzz (category_name, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ns_score_date      ON niche_scores (opportunity_score DESC, scored_at);
CREATE INDEX IF NOT EXISTS idx_rve_category_date  ON retail_vs_ecomm (category_name, period_date);
