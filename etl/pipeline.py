import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from etl import ingest, transform, load
from db.connection import health_check


def run_pipeline(skip_amazon: bool = False):
    print(f"\n{'='*50}")
    print(f"Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    if not health_check():
        print("ERROR: Cannot reach database. Aborting.")
        return False

    # ── INGEST ────────────────────────────────
    print("[ 1/4 ] Ingesting data...")

    print("  Fetching eBay category data...")
    ebay_raw = ingest.fetch_ebay_category_data()

    amazon_raw = None
    if not skip_amazon:
        print("  Fetching Amazon BSR data (Rainforest)...")
        amazon_raw = ingest.fetch_amazon_bsr_data()

    print("  Fetching Google Trends data...")
    trends_raw = ingest.fetch_google_trends_data()

    print("  Fetching Reddit data...")
    reddit_raw = ingest.fetch_reddit_data()

    print("  Loading BLS consumer expenditure CSV...")
    bls_raw = ingest.fetch_bls_data()

    # ── TRANSFORM ─────────────────────────────
    print("\n[ 2/4 ] Transforming data...")

    import pandas as pd

    all_trends = pd.concat([
        transform.transform_category_trends(ebay_raw),
        transform.transform_category_trends(amazon_raw) if amazon_raw is not None and not amazon_raw.empty else pd.DataFrame(),
    ], ignore_index=True)

    search_clean  = transform.transform_search_signals(trends_raw)
    buzz_clean    = transform.transform_social_buzz(reddit_raw)
    spend_clean   = transform.transform_consumer_spend(bls_raw)
    rve_clean     = transform.build_retail_vs_ecomm(spend_clean, all_trends)

    # ── LOAD ──────────────────────────────────
    print("\n[ 3/4 ] Loading into PostgreSQL...")

    load.load_category_trends(all_trends)
    load.load_search_signals(search_clean)
    load.load_social_buzz(buzz_clean)
    load.load_consumer_spend(spend_clean)
    load.load_retail_vs_ecomm(rve_clean)

    # ── NICHE SCORES ──────────────────────────
    print("\n[ 4/4 ] Computing niche opportunity scores...")
    _compute_and_load_niche_scores()

    print(f"\n{'='*50}")
    print(f"Pipeline complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")
    return True


def _compute_and_load_niche_scores():
    from db.connection import get_engine
    from sqlalchemy import text
    import pandas as pd
    from datetime import date

    engine = get_engine()

    query = text("""
        WITH trend_scores AS (
            SELECT
                category_name,
                AVG(interest_score) AS avg_interest,
                (AVG(interest_score) - LAG(AVG(interest_score)) OVER (
                    PARTITION BY category_name ORDER BY snapshot_date
                )) AS trend_delta
            FROM search_signals
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY category_name, snapshot_date
        ),
        buzz_scores AS (
            SELECT
                category_name,
                SUM(mention_count) AS total_mentions,
                AVG(avg_score) AS avg_post_score
            FROM social_buzz
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY category_name
        ),
        demand_scores AS (
            SELECT
                category_name,
                AVG(sell_through) AS avg_sell_through,
                AVG(listing_count) AS avg_listings
            FROM category_trends
            WHERE snapshot_date = CURRENT_DATE
            GROUP BY category_name
        ),
        spend_scores AS (
            SELECT
                category_name,
                AVG(yoy_change) AS avg_spend_growth
            FROM consumer_spend
            WHERE year >= EXTRACT(YEAR FROM NOW()) - 2
            GROUP BY category_name
        )
        SELECT
            d.category_name,
            COALESCE(t.avg_interest, 0)          AS raw_trend,
            COALESCE(b.total_mentions, 0)         AS raw_buzz,
            COALESCE(d.avg_sell_through, 0) * 100 AS raw_demand,
            COALESCE(s.avg_spend_growth, 0) * 100 AS raw_spend,
            COALESCE(d.avg_listings, 0)           AS avg_listings
        FROM demand_scores d
        LEFT JOIN trend_scores t  ON t.category_name = d.category_name
        LEFT JOIN buzz_scores b   ON b.category_name = d.category_name
        LEFT JOIN spend_scores s  ON s.category_name = d.category_name
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("  No data available for scoring yet")
        return

    def normalize(series):
        mn, mx = series.min(), series.max()
        if mx == mn:
            return pd.Series([50.0] * len(series), index=series.index)
        return ((series - mn) / (mx - mn) * 100).round(2)

    df["trend_score"]       = normalize(df["raw_trend"])
    df["buzz_score"]        = normalize(df["raw_buzz"])
    df["demand_score"]      = normalize(df["raw_demand"])
    df["spend_score"]       = normalize(df["raw_spend"])
    df["competition_score"] = normalize(1 / (df["avg_listings"] + 1) * 10000)

    df["opportunity_score"] = (
        df["trend_score"]       * 0.25 +
        df["buzz_score"]        * 0.20 +
        df["demand_score"]      * 0.30 +
        df["spend_score"]       * 0.15 +
        df["competition_score"] * 0.10
    ).round(2)

    df["scored_at"] = date.today()
    df["channel_edge"] = df["demand_score"].apply(
        lambda x: "ecomm" if x > 60 else ("retail" if x < 35 else "mixed")
    )
    df["recommendation"] = df["opportunity_score"].apply(
        lambda x: "enter" if x >= 65 else ("watch" if x >= 40 else "avoid")
    )

    score_df = df[[
        "category_name", "scored_at", "trend_score", "buzz_score",
        "demand_score", "spend_score", "competition_score",
        "opportunity_score", "channel_edge", "recommendation",
    ]]

    load.load_niche_scores(score_df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the e-commerce intelligence pipeline")
    parser.add_argument("--skip-amazon", action="store_true", help="Skip Rainforest/Amazon BSR fetch")
    args = parser.parse_args()
    run_pipeline(skip_amazon=args.skip_amazon)
