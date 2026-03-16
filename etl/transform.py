import pandas as pd
import numpy as np
from datetime import date


def _normalize_category_name(name: str) -> str:
    return name.strip().title()


def _clip_score(series: pd.Series) -> pd.Series:
    return series.clip(0, 100)


# ─────────────────────────────────────────────
# category_trends
# ─────────────────────────────────────────────

def transform_category_trends(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["category_name"] = df["category_name"].apply(_normalize_category_name)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce").round(2)
    df["listing_count"] = pd.to_numeric(df["listing_count"], errors="coerce").fillna(0).astype(int)
    df["sold_count"] = pd.to_numeric(df["sold_count"], errors="coerce").fillna(0).astype(int)
    df["sell_through"] = pd.to_numeric(df["sell_through"], errors="coerce").clip(0, 1).round(4)
    df["rank_position"] = pd.to_numeric(df["rank_position"], errors="coerce")
    df = df.drop_duplicates(subset=["category_name", "source", "snapshot_date"])
    return df


# ─────────────────────────────────────────────
# search_signals
# ─────────────────────────────────────────────

def transform_search_signals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["keyword"] = df["keyword"].str.strip().str.lower()
    df["category_name"] = df["category_name"].apply(_normalize_category_name)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["interest_score"] = pd.to_numeric(df["interest_score"], errors="coerce").fillna(0).astype(int)
    df["interest_score"] = df["interest_score"].clip(0, 100)
    df["geo"] = df["geo"].fillna("US").str.upper()
    df = df.drop_duplicates(subset=["keyword", "snapshot_date", "geo"])
    return df


# ─────────────────────────────────────────────
# social_buzz
# ─────────────────────────────────────────────

def transform_social_buzz(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["category_name"] = df["category_name"].apply(_normalize_category_name)
    df["keyword"] = df["keyword"].str.strip().str.lower()
    df["subreddit"] = df["subreddit"].str.strip()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["mention_count"] = pd.to_numeric(df["mention_count"], errors="coerce").fillna(0).astype(int)
    df["avg_score"] = pd.to_numeric(df["avg_score"], errors="coerce").fillna(0).round(2)
    df = df.drop_duplicates(subset=["keyword", "subreddit", "snapshot_date"])
    return df


# ─────────────────────────────────────────────
# consumer_spend
# ─────────────────────────────────────────────

def transform_consumer_spend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["category_name"] = df["category_name"].apply(_normalize_category_name)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["avg_annual_spend"] = pd.to_numeric(df["avg_annual_spend"], errors="coerce").round(2)
    df["yoy_change"] = pd.to_numeric(df["yoy_change"], errors="coerce").round(4)
    df = df[df["year"] > 2000]
    df = df.drop_duplicates(subset=["category_name", "year"])
    return df


# ─────────────────────────────────────────────
# retail_vs_ecomm  (derived — built from consumer_spend + category_trends)
# ─────────────────────────────────────────────

ECOMM_SHARE_ESTIMATES = {
    "Electronics":             0.62,
    "Clothing & Accessories":  0.38,
    "Home & Garden":           0.33,
    "Health & Beauty":         0.42,
    "Toys & Hobbies":          0.51,
    "Pet Supplies":            0.28,
    "Sporting Goods":          0.30,
    "Automotive Parts":        0.22,
    "Musical Instruments":     0.45,
    "Collectibles":            0.70,
}

def build_retail_vs_ecomm(
    spend_df: pd.DataFrame,
    trends_df: pd.DataFrame,
) -> pd.DataFrame:
    if spend_df.empty:
        return pd.DataFrame()

    rows = []
    today = date.today()

    for category_name, base_ecomm_share in ECOMM_SHARE_ESTIMATES.items():
        cat_spend = spend_df[spend_df["category_name"] == category_name]
        cat_trends = trends_df[trends_df["category_name"] == category_name] if not trends_df.empty else pd.DataFrame()

        yoy = float(cat_spend["yoy_change"].mean()) if not cat_spend.empty else 0.0

        ecomm_growth = yoy * 1.35
        retail_growth = yoy * 0.65

        ecomm_share = min(base_ecomm_share + (ecomm_growth * 0.1), 0.95)
        retail_share = round(1.0 - ecomm_share, 4)

        rows.append({
            "category_name":  category_name,
            "period_date":    today,
            "ecomm_share":    round(ecomm_share, 4),
            "retail_share":   round(retail_share, 4),
            "ecomm_growth":   round(ecomm_growth, 4),
            "retail_growth":  round(retail_growth, 4),
            "source":         "derived",
        })

    return pd.DataFrame(rows)
