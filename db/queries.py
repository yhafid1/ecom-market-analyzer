import pandas as pd
from sqlalchemy import text
from db.connection import get_engine


def _query(sql: str, params: dict = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ─────────────────────────────────────────────
# Leaderboard — Rising vs Declining tab
# ─────────────────────────────────────────────

def get_leaderboard() -> pd.DataFrame:
    return _query("""
        SELECT
            category_name,
            opportunity_score,
            trend_score,
            buzz_score,
            demand_score,
            spend_score,
            competition_score,
            channel_edge,
            recommendation,
            momentum,
            score_delta,
            current_rank,
            avg_price,
            sell_through,
            listing_count
        FROM mv_category_leaderboard
        ORDER BY opportunity_score DESC
    """)


def get_rising_declining() -> pd.DataFrame:
    return _query("""
        SELECT
            category_name,
            opportunity_score,
            score_delta,
            momentum,
            recommendation,
            channel_edge
        FROM mv_category_leaderboard
        ORDER BY score_delta DESC
    """)


# ─────────────────────────────────────────────
# Channel comparison — Retail vs E-comm tab
# ─────────────────────────────────────────────

def get_channel_comparison() -> pd.DataFrame:
    return _query("""
        SELECT
            category_name,
            period_date,
            ecomm_share_pct,
            retail_share_pct,
            ecomm_growth_pct,
            retail_growth_pct,
            channel_status,
            opportunity_label
        FROM mv_channel_comparison
        ORDER BY period_date DESC, ecomm_growth_pct DESC
    """)


def get_channel_summary() -> pd.DataFrame:
    return _query("""
        SELECT
            category_name,
            ROUND(AVG(ecomm_share_pct), 1)      AS avg_ecomm_share,
            ROUND(AVG(retail_share_pct), 1)     AS avg_retail_share,
            ROUND(AVG(ecomm_growth_pct), 2)     AS avg_ecomm_growth,
            MAX(channel_status)                 AS channel_status,
            MAX(opportunity_label)              AS opportunity_label
        FROM mv_channel_comparison
        GROUP BY category_name
        ORDER BY avg_ecomm_growth DESC
    """)


# ─────────────────────────────────────────────
# Niche Finder tab
# ─────────────────────────────────────────────

def get_niche_finder(
    min_score: float = 0,
    recommendation: str = None,
    channel_edge: str = None,
    max_price: float = None,
) -> pd.DataFrame:
    filters = ["opportunity_score >= :min_score"]
    params = {"min_score": min_score}

    if recommendation:
        filters.append("recommendation = :recommendation")
        params["recommendation"] = recommendation

    if channel_edge:
        filters.append("channel_edge = :channel_edge")
        params["channel_edge"] = channel_edge

    if max_price:
        filters.append("avg_price <= :max_price")
        params["max_price"] = max_price

    where = " AND ".join(filters)

    return _query(f"""
        SELECT
            category_name,
            opportunity_score,
            trend_score,
            buzz_score,
            demand_score,
            spend_score,
            competition_score,
            channel_edge,
            recommendation,
            avg_price,
            sell_through_pct,
            avg_interest_30d,
            weekly_mentions,
            spend_growth_pct,
            listing_count
        FROM mv_niche_finder
        WHERE {where}
        ORDER BY opportunity_score DESC
    """, params)


# ─────────────────────────────────────────────
# Product Deep Dive tab
# ─────────────────────────────────────────────

def get_category_timeseries(category_name: str) -> pd.DataFrame:
    return _query("""
        SELECT
            snapshot_date,
            sell_through_pct,
            avg_price,
            listing_count,
            sold_count,
            search_interest,
            reddit_mentions,
            source
        FROM mv_trends_explorer
        WHERE category_name = :category
        ORDER BY snapshot_date ASC
    """, {"category": category_name})


def get_category_score_breakdown(category_name: str) -> pd.DataFrame:
    return _query("""
        SELECT
            category_name,
            opportunity_score,
            trend_score,
            buzz_score,
            demand_score,
            spend_score,
            competition_score,
            channel_edge,
            recommendation,
            avg_price,
            sell_through_pct,
            avg_interest_30d,
            weekly_mentions,
            spend_growth_pct,
            listing_count
        FROM mv_niche_finder
        WHERE category_name = :category
    """, {"category": category_name})


# ─────────────────────────────────────────────
# Trends Explorer tab
# ─────────────────────────────────────────────

def get_trends_explorer(
    categories: list[str],
    signal: str = "sell_through_pct",
    days: int = 90,
) -> pd.DataFrame:
    valid_signals = {
        "sell_through_pct", "avg_price",
        "listing_count", "search_interest", "reddit_mentions"
    }
    if signal not in valid_signals:
        signal = "sell_through_pct"

    placeholders = ", ".join(f":cat{i}" for i in range(len(categories)))
    params = {"days": days}
    params.update({f"cat{i}": c for i, c in enumerate(categories)})

    return _query(f"""
        SELECT
            category_name,
            snapshot_date,
            {signal} AS value
        FROM mv_trends_explorer
        WHERE
            category_name IN ({placeholders})
            AND snapshot_date >= CURRENT_DATE - INTERVAL ':days days'
        ORDER BY category_name, snapshot_date ASC
    """, params)


def get_all_category_names() -> list[str]:
    df = _query("SELECT DISTINCT category_name FROM mv_niche_finder ORDER BY category_name")
    return df["category_name"].tolist() if not df.empty else []


# ─────────────────────────────────────────────
# Refresh materialized views (called by pipeline)
# ─────────────────────────────────────────────

def refresh_views():
    views = [
        "mv_category_leaderboard",
        "mv_channel_comparison",
        "mv_niche_finder",
        "mv_trends_explorer",
    ]
    with get_engine().begin() as conn:
        for view in views:
            try:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                print(f"  [views] refreshed {view}")
            except Exception as e:
                print(f"  [views] failed to refresh {view}: {e}")
