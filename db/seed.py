"""
db/seed.py — Seed a small, synthetic, publicly-shareable demo dataset.

Purpose
-------
Populates the 6 base tables defined in db/schema.sql with a few days of
realistic-looking (but fully synthetic) data, so the Streamlit dashboard
renders meaningful charts on a fresh hosted Postgres instance without
needing any live eBay / Google Trends / Reddit / BLS API keys.

This is intended for:
  - Local demo/dev (`docker compose up`, then run this script once)
  - A hosted Postgres instance (Neon/Supabase) used for a public,
    read-only demo deployment of the dashboard (see DEPLOYMENT.md)

It does NOT touch etl/ingest.py, etl/transform.py or etl/load.py — the
real ETL pipeline is untouched. This script writes directly to the base
tables using the same connection module (db/connection.py), so it works
against both local Docker Postgres and any DATABASE_URL-configured
hosted Postgres.

Usage
-----
    python -m db.seed                # seed with default settings
    python -m db.seed --reset        # TRUNCATE all tables first, then seed
    python -m db.seed --days 14      # seed 14 days of history (default: 10)

After seeding, this script also creates (if missing) and refreshes the
4 materialized views the dashboard reads from (mv_category_leaderboard,
mv_channel_comparison, mv_niche_finder, mv_trends_explorer), by executing
db/queries/materialized_views.sql and then a non-concurrent REFRESH
(CONCURRENTLY requires the view to already have data + a unique index
populated, which isn't guaranteed on a brand-new database).
"""

import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db.connection import get_engine, health_check
from etl.load import (
    load_category_trends,
    load_search_signals,
    load_social_buzz,
    load_consumer_spend,
    load_retail_vs_ecomm,
    load_niche_scores,
)

SCHEMA_DIR = Path(__file__).resolve().parent
MATERIALIZED_VIEWS_SQL = SCHEMA_DIR / "queries" / "materialized_views.sql"

RANDOM_SEED = 42

# ─────────────────────────────────────────────
# Reference data — mirrors etl/ingest.py category list so the demo
# dataset is shaped exactly like real pipeline output would be.
# ─────────────────────────────────────────────

CATEGORIES = [
    "Electronics",
    "Home & Garden",
    "Clothing & Accessories",
    "Sporting Goods",
    "Toys & Hobbies",
    "Health & Beauty",
    "Pet Supplies",
    "Automotive Parts",
    "Musical Instruments",
    "Collectibles",
]

CATEGORY_IDS = {
    "Electronics": "58058",
    "Home & Garden": "11700",
    "Clothing & Accessories": "11450",
    "Sporting Goods": "888",
    "Toys & Hobbies": "220",
    "Health & Beauty": "26395",
    "Pet Supplies": "1281",
    "Automotive Parts": "6000",
    "Musical Instruments": "619",
    "Collectibles": "1",
}

# Baseline per-category economics so the synthetic numbers look plausible
# and internally consistent (e.g. Electronics = higher price, lower
# sell-through; Collectibles = high e-comm share, etc).
CATEGORY_BASELINES = {
    "Electronics":             dict(price=145.0, listings=4200, sell_through=0.18, ecomm_share=0.62, interest=68, mentions=40, spend=2450.0),
    "Home & Garden":           dict(price=62.0,  listings=3100, sell_through=0.14, ecomm_share=0.33, interest=52, mentions=22, spend=1080.0),
    "Clothing & Accessories":  dict(price=34.0,  listings=6800, sell_through=0.22, ecomm_share=0.38, interest=58, mentions=30, spend=1980.0),
    "Sporting Goods":          dict(price=58.0,  listings=2600, sell_through=0.16, ecomm_share=0.30, interest=47, mentions=18, spend=920.0),
    "Toys & Hobbies":          dict(price=27.0,  listings=3900, sell_through=0.25, ecomm_share=0.51, interest=61, mentions=35, spend=760.0),
    "Health & Beauty":         dict(price=22.0,  listings=5200, sell_through=0.20, ecomm_share=0.42, interest=55, mentions=26, spend=680.0),
    "Pet Supplies":            dict(price=31.0,  listings=2400, sell_through=0.19, ecomm_share=0.28, interest=44, mentions=20, spend=560.0),
    "Automotive Parts":        dict(price=78.0,  listings=3300, sell_through=0.12, ecomm_share=0.22, interest=39, mentions=14, spend=1340.0),
    "Musical Instruments":     dict(price=210.0, listings=980,  sell_through=0.15, ecomm_share=0.45, interest=41, mentions=12, spend=410.0),
    "Collectibles":            dict(price=95.0,  listings=1800, sell_through=0.28, ecomm_share=0.70, interest=63, mentions=45, spend=390.0),
}

TREND_KEYWORDS = {
    "Electronics":            ["consumer electronics", "gadgets online"],
    "Home & Garden":          ["home decor", "garden supplies"],
    "Clothing & Accessories": ["online clothing", "fashion accessories"],
    "Sporting Goods":         ["sports equipment", "fitness gear"],
    "Toys & Hobbies":         ["toys online", "hobby supplies"],
    "Health & Beauty":        ["health products", "beauty supplies"],
    "Pet Supplies":           ["pet products", "pet accessories"],
    "Automotive Parts":       ["auto parts online", "car accessories"],
    "Musical Instruments":    ["musical instruments", "guitar accessories"],
    "Collectibles":           ["collectibles buy", "rare items online"],
}

REDDIT_SUBREDDITS = ["flipping", "entrepreneur", "ecommerce", "amazonseller", "Ebay", "dropship"]

REDDIT_SEARCH_TERMS = {
    "Electronics":            "electronics",
    "Home & Garden":          "home garden",
    "Clothing & Accessories": "clothing fashion",
    "Sporting Goods":         "sporting goods fitness",
    "Toys & Hobbies":         "toys hobbies",
    "Health & Beauty":        "health beauty",
    "Pet Supplies":           "pet supplies",
    "Automotive Parts":       "auto parts car",
    "Musical Instruments":    "musical instruments",
    "Collectibles":           "collectibles vintage",
}

BLS_CATEGORY_MAP = {
    "Clothing & Accessories":  "Apparel and services",
    "Toys & Hobbies":          "Entertainment",
    "Health & Beauty":         "Personal care products",
    "Home & Garden":           "Household furnishings & equipment",
    "Automotive Parts":        "Vehicles",
    "Pet Supplies":            "Pets, toys, hobbies",
    "Collectibles":            "Reading",
    "Sporting Goods":          "Sports & recreation equipment",
    "Electronics":             "Electronics and computers",
    "Musical Instruments":     "Entertainment",
}


def _jitter(value: float, pct: float, rng: random.Random) -> float:
    """Return value nudged by up to +/- pct (e.g. 0.08 = +/-8%)."""
    return value * (1 + rng.uniform(-pct, pct))


def build_category_trends(days: int, rng: random.Random) -> pd.DataFrame:
    rows = []
    today = date.today()
    for day_offset in range(days):
        snap_date = today - timedelta(days=days - 1 - day_offset)
        # Mild upward drift over the window so charts show trend/momentum.
        drift = 1 + (day_offset / max(days - 1, 1)) * 0.06
        for cat in CATEGORIES:
            base = CATEGORY_BASELINES[cat]
            listings = max(1, int(_jitter(base["listings"], 0.08, rng) * drift))
            sell_through = min(0.95, max(0.01, _jitter(base["sell_through"], 0.15, rng) * drift))
            sold = int(listings * sell_through)
            rows.append({
                "category_name": cat,
                "category_id": CATEGORY_IDS[cat],
                "source": "ebay",
                "snapshot_date": snap_date,
                "listing_count": listings,
                "avg_price": round(_jitter(base["price"], 0.06, rng), 2),
                "sold_count": sold,
                "sell_through": round(sell_through, 4),
                "rank_position": None,  # explicit None (not NaN) so it binds to SQL NULL cleanly
            })
    return pd.DataFrame(rows)


def build_search_signals(days: int, rng: random.Random) -> pd.DataFrame:
    rows = []
    today = date.today()
    for day_offset in range(days):
        snap_date = today - timedelta(days=days - 1 - day_offset)
        drift = 1 + (day_offset / max(days - 1, 1)) * 0.10
        for cat, keywords in TREND_KEYWORDS.items():
            base_interest = CATEGORY_BASELINES[cat]["interest"]
            for kw in keywords:
                score = int(min(100, max(1, _jitter(base_interest, 0.20, rng) * drift)))
                rows.append({
                    "keyword": kw,
                    "category_name": cat,
                    "snapshot_date": snap_date,
                    "interest_score": score,
                    "geo": "US",
                })
    return pd.DataFrame(rows)


def build_social_buzz(days: int, rng: random.Random) -> pd.DataFrame:
    rows = []
    today = date.today()
    for day_offset in range(days):
        snap_date = today - timedelta(days=days - 1 - day_offset)
        drift = 1 + (day_offset / max(days - 1, 1)) * 0.12
        for cat, term in REDDIT_SEARCH_TERMS.items():
            base_mentions = CATEGORY_BASELINES[cat]["mentions"]
            # Spread mentions across a couple of subreddits per category
            # per day (not all 6, to keep the dataset small).
            active_subs = rng.sample(REDDIT_SUBREDDITS, k=2)
            for sub in active_subs:
                mentions = max(0, int(_jitter(base_mentions / 2, 0.35, rng) * drift))
                rows.append({
                    "category_name": cat,
                    "keyword": term,
                    "subreddit": sub,
                    "snapshot_date": snap_date,
                    "mention_count": mentions,
                    "avg_score": round(max(0.0, _jitter(8.0, 0.5, rng)), 2),
                })
    return pd.DataFrame(rows)


def build_consumer_spend(rng: random.Random) -> pd.DataFrame:
    rows = []
    current_year = date.today().year
    for cat in CATEGORIES:
        base_spend = CATEGORY_BASELINES[cat]["spend"]
        prev_spend = base_spend * 0.94
        for i, year in enumerate([current_year - 2, current_year - 1, current_year]):
            spend = base_spend * (1 + 0.03 * i) * (1 + rng.uniform(-0.02, 0.02))
            yoy = (spend - prev_spend) / prev_spend if prev_spend else 0.0
            rows.append({
                "category_name": cat,
                "bls_category": BLS_CATEGORY_MAP.get(cat, cat),
                "year": year,
                "avg_annual_spend": round(spend, 2),
                "yoy_change": round(yoy, 4),
            })
            prev_spend = spend
    return pd.DataFrame(rows)


def build_retail_vs_ecomm(days: int, rng: random.Random) -> pd.DataFrame:
    rows = []
    today = date.today()
    # A handful of weekly snapshots rather than one per day — this table
    # models slower-moving channel-share data (matches BLS cadence).
    num_snapshots = max(1, days // 3)
    for i in range(num_snapshots):
        period_date = today - timedelta(days=(num_snapshots - 1 - i) * 3)
        drift = 1 + (i / max(num_snapshots - 1, 1)) * 0.05
        for cat in CATEGORIES:
            base = CATEGORY_BASELINES[cat]
            ecomm_share = min(0.95, max(0.05, _jitter(base["ecomm_share"], 0.05, rng) * drift))
            retail_share = round(1 - ecomm_share, 4)
            ecomm_growth = round(rng.uniform(-0.02, 0.09), 4)
            retail_growth = round(rng.uniform(-0.05, 0.02), 4)
            rows.append({
                "category_name": cat,
                "period_date": period_date,
                "ecomm_share": round(ecomm_share, 4),
                "retail_share": retail_share,
                "ecomm_growth": ecomm_growth,
                "retail_growth": retail_growth,
                "source": "derived",
            })
    return pd.DataFrame(rows)


def build_niche_scores(days: int, rng: random.Random) -> pd.DataFrame:
    """
    Synthetic composite scores for each category, one snapshot per day,
    with a slight upward/downward walk so the "Rising vs Declining"
    dashboard tab has momentum to show (score_delta between the two
    most recent scored_at dates).
    """
    rows = []
    today = date.today()
    # Give each category a distinct trajectory (some rising, some flat,
    # some declining) so the dashboard demo looks realistic.
    trajectories = {
        "Electronics":             +0.6,
        "Home & Garden":           -0.3,
        "Clothing & Accessories":  +0.2,
        "Sporting Goods":          -0.1,
        "Toys & Hobbies":          +0.8,
        "Health & Beauty":         +0.1,
        "Pet Supplies":            -0.5,
        "Automotive Parts":        -0.7,
        "Musical Instruments":     +0.0,
        "Collectibles":            +0.4,
    }
    # Only keep the most recent ~5 scoring snapshots — niche_scores is a
    # weekly-refreshed table in the real pipeline, so we don't need one
    # row per day of history like category_trends.
    num_snapshots = min(days, 5)
    base_scores = {cat: rng.uniform(35, 75) for cat in CATEGORIES}

    for i in range(num_snapshots):
        scored_at = today - timedelta(days=(num_snapshots - 1 - i) * 2)
        for cat in CATEGORIES:
            step = trajectories[cat] * i
            trend = min(100, max(0, base_scores[cat] + step + rng.uniform(-3, 3)))
            buzz = min(100, max(0, base_scores[cat] + step * 0.8 + rng.uniform(-4, 4)))
            demand = min(100, max(0, base_scores[cat] + step * 1.2 + rng.uniform(-3, 3)))
            spend = min(100, max(0, base_scores[cat] + step * 0.5 + rng.uniform(-4, 4)))
            competition = min(100, max(0, 100 - base_scores[cat] + rng.uniform(-3, 3)))

            opportunity = round(
                trend * 0.25 + buzz * 0.20 + demand * 0.30 + spend * 0.15 + competition * 0.10, 2
            )
            channel_edge = "ecomm" if demand > 60 else ("retail" if demand < 35 else "mixed")
            recommendation = "enter" if opportunity >= 65 else ("watch" if opportunity >= 40 else "avoid")

            rows.append({
                "category_name": cat,
                "scored_at": scored_at,
                "trend_score": round(trend, 2),
                "buzz_score": round(buzz, 2),
                "demand_score": round(demand, 2),
                "spend_score": round(spend, 2),
                "competition_score": round(competition, 2),
                "opportunity_score": opportunity,
                "channel_edge": channel_edge,
                "recommendation": recommendation,
            })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────

TABLES_IN_TRUNCATE_ORDER = [
    "niche_scores",
    "retail_vs_ecomm",
    "consumer_spend",
    "social_buzz",
    "search_signals",
    "category_trends",
]


def reset_tables():
    engine = get_engine()
    with engine.begin() as conn:
        for table in TABLES_IN_TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
    print("  [seed] existing demo rows truncated")


def _split_sql_statements(sql_text: str) -> list[str]:
    """
    Split a .sql file into individual executable statements on top-level
    semicolons. Good enough for db/queries/materialized_views.sql, which
    contains only CREATE MATERIALIZED VIEW / CREATE INDEX statements and
    `--` line comments (no semicolons inside string literals or $$ blocks).
    """
    statements = []
    for raw_stmt in sql_text.split(";"):
        # Drop full-line `--` comments, then check if anything executable remains.
        lines = [
            line for line in raw_stmt.splitlines()
            if not line.strip().startswith("--")
        ]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


def create_and_refresh_views():
    engine = get_engine()
    sql_text = MATERIALIZED_VIEWS_SQL.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    print(f"  [seed] materialized views created (if not already present) — {len(statements)} statements executed")

    view_names = [
        "mv_category_leaderboard",
        "mv_channel_comparison",
        "mv_niche_finder",
        "mv_trends_explorer",
    ]
    with engine.begin() as conn:
        for view in view_names:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))
            print(f"  [seed] refreshed {view}")


def seed(days: int = 10, do_reset: bool = False):
    rng = random.Random(RANDOM_SEED)

    print(f"\n{'='*50}")
    print("Seeding demo dataset")
    print(f"{'='*50}\n")

    if not health_check():
        print("ERROR: Cannot reach database. Check DATABASE_URL / DB_* env vars.")
        return False

    if do_reset:
        print("[ 0/3 ] Resetting existing demo data...")
        reset_tables()

    print(f"[ 1/3 ] Generating {days} days of synthetic data across 10 categories...")
    category_trends_df = build_category_trends(days, rng)
    search_signals_df   = build_search_signals(days, rng)
    social_buzz_df       = build_social_buzz(days, rng)
    consumer_spend_df    = build_consumer_spend(rng)
    retail_vs_ecomm_df   = build_retail_vs_ecomm(days, rng)
    niche_scores_df       = build_niche_scores(days, rng)

    print("\n[ 2/3 ] Loading into PostgreSQL...")
    load_category_trends(category_trends_df)
    load_search_signals(search_signals_df)
    load_social_buzz(social_buzz_df)
    load_consumer_spend(consumer_spend_df)
    load_retail_vs_ecomm(retail_vs_ecomm_df)
    load_niche_scores(niche_scores_df)

    print("\n[ 3/3 ] Creating & refreshing materialized views...")
    create_and_refresh_views()

    print(f"\n{'='*50}")
    print("Seed complete. The dashboard should now render demo data.")
    print(f"{'='*50}\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed a small synthetic demo dataset into the ecom-market-analyzer schema."
    )
    parser.add_argument(
        "--days", type=int, default=10,
        help="Number of days of category_trends/search_signals/social_buzz history to generate (default: 10)",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="TRUNCATE all 6 tables before seeding (use for a clean demo reset)",
    )
    args = parser.parse_args()
    success = seed(days=args.days, do_reset=args.reset)
    raise SystemExit(0 if success else 1)
